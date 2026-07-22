use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};

use crate::validation::ValidationError;
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::hub_cache::{HubCacheFile, HubCacheReadError, HubCacheReader, HubSnapshotFileForm};
use super::hub_layout::HubBlobKey;
use super::key::{BlobDigest, SelectionId};
use super::local_dir_materialization::{Cancellation, LocalDirFileTarget};
use super::local_dir_reconciliation::{
    LocalDirCandidateSet, LocalDirSourceError, PreparedLocalDirSource,
};
use super::metadata::{SnapshotFileRecord, SnapshotManifestRecord};
use super::publication::{CacheError, CacheKernel, Effects, SnapshotLease};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(super) struct CompatibleSnapshotImporter {
    reader: HubCacheReader,
    sidecar: CacheKernel,
}

impl CompatibleSnapshotImporter {
    pub(super) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
    ) -> Result<Self, CompatibleCacheError> {
        let (reader, sidecar) = open_shared_cache(root.as_ref(), endpoint, spec, effects)?;
        Ok(Self { reader, sidecar })
    }

    pub(super) fn import(
        &self,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> Result<CompatibleSnapshot, CompatibleCacheError> {
        let selection = ExactSelection::new(paths)?;
        let index = self.reader.read_index(revision)?;
        let prepared = PreparedSnapshot::read(&self.reader, &index, selection)?;

        // Initialization is deliberately after the full read. Even the format
        // record would otherwise make a failed Python-cache validation look
        // like an imported snapshot to cache inspection.
        self.sidecar
            .initialize()
            .map_err(|error| CompatibleCacheError::from(error).with_may_have_published())?;
        for (key, binding) in &prepared.bindings {
            self.sidecar
                .publish_hub_blob_binding(key, binding.digest, binding.size)
                .map_err(|error| CompatibleCacheError::from(error).with_may_have_published())?;
        }

        let records = prepared
            .files
            .iter()
            .map(PreparedFile::record)
            .collect::<Vec<_>>();
        publish_manifest(
            &self.sidecar,
            &prepared.commit,
            &prepared.selection.id,
            records,
        )
        .map_err(CompatibleCacheError::with_may_have_published)?;

        let immutable_revision = Revision::parse(prepared.commit.as_str())
            .map_err(|error| CompatibleCacheError::from(error).with_may_have_published())?;
        self.offline()
            .open(&immutable_revision, prepared.selection.paths())
            .map_err(CompatibleCacheError::with_may_have_published)
    }

    fn offline(&self) -> CompatibleCacheOffline {
        CompatibleCacheOffline::from_parts(self.reader.clone(), self.sidecar.clone())
    }
}

pub(super) fn publish_manifest(
    sidecar: &CacheKernel,
    commit: &CommitId,
    selection: &SelectionId,
    files: Vec<SnapshotFileRecord>,
) -> Result<(), CompatibleCacheError> {
    let expected = SnapshotManifestRecord::new(commit, selection, files.clone())?;
    match sidecar.publish_compatible_manifest(commit, selection, files) {
        Ok(()) => Ok(()),
        Err(error) if error.may_have_published() => {
            match sidecar.read_snapshot_manifest(commit, selection) {
                Ok(actual) if actual == expected => Ok(()),
                Ok(_) | Err(_) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Debug)]
pub(super) struct CompatibleCacheOffline {
    reader: HubCacheReader,
    sidecar: CacheKernel,
}

impl CompatibleCacheOffline {
    pub(super) fn from_parts(reader: HubCacheReader, sidecar: CacheKernel) -> Self {
        Self { reader, sidecar }
    }

    pub(super) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
    ) -> Result<Self, CompatibleCacheError> {
        let (reader, sidecar) = open_shared_cache(root.as_ref(), endpoint, spec, effects)?;
        Ok(Self { reader, sidecar })
    }

    pub(super) fn open(
        &self,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> Result<CompatibleSnapshot, CompatibleCacheError> {
        let selection = ExactSelection::new(paths)?;
        let index = self.reader.read_index(revision)?;
        let lease = self
            .sidecar
            .acquire_snapshot_lease(index.commit(), &selection.id)?;
        let manifest = self
            .sidecar
            .read_snapshot_manifest(index.commit(), &selection.id)?;
        if manifest.files().len() != selection.paths.len() {
            return Err(CompatibleCacheError::corrupt());
        }

        let mut files = Vec::with_capacity(selection.paths.len());
        for (path, record) in selection.paths.iter().zip(manifest.files()) {
            if record.path() != path.as_str() {
                return Err(CompatibleCacheError::corrupt());
            }
            let key = record
                .hub_blob_key()?
                .ok_or_else(CompatibleCacheError::corrupt)?;
            let digest = record.digest()?;
            let binding = self.sidecar.read_hub_blob_binding(&key)?;
            if binding.digest()? != digest || binding.size() != record.size() {
                return Err(CompatibleCacheError::corrupt());
            }

            let current = self.reader.read_snapshot_file(&index, path)?;
            if current.hub_blob_key() != &key
                || current.digest() != digest
                || current.size() != record.size()
            {
                return Err(CompatibleCacheError::corrupt());
            }
            files.push(CompatibleSnapshotFile::new(path.clone(), &current));
        }

        Ok(CompatibleSnapshot {
            commit: index.commit().clone(),
            selection: selection.id,
            files: files.into_boxed_slice(),
            lease,
        })
    }

    pub(super) fn local_dir_candidates(
        &self,
        snapshot: &CompatibleSnapshot,
    ) -> CompatibleSnapshotCandidates {
        CompatibleSnapshotCandidates::new(self.reader.clone(), snapshot)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CompatibleSnapshotCandidates {
    reader: HubCacheReader,
    files: BTreeMap<RepoPath, CompatibleSnapshotFile>,
}

impl CompatibleSnapshotCandidates {
    fn new(reader: HubCacheReader, snapshot: &CompatibleSnapshot) -> Self {
        let files = snapshot
            .files()
            .iter()
            .cloned()
            .map(|file| (file.path().clone(), file))
            .collect();
        Self { reader, files }
    }
}

impl LocalDirCandidateSet for CompatibleSnapshotCandidates {
    fn prepare_local(
        &mut self,
        target: &LocalDirFileTarget,
        _cancellation: &dyn Cancellation,
    ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
        let Some(file) = self.files.get(target.path()) else {
            return Ok(None);
        };
        if file.size() != target.entry().size() || file.digest() != target.digest() {
            return Err(LocalDirSourceError::invalid());
        }
        let source_path = file.blob_path().unwrap_or_else(|| file.content_path());
        self.reader
            .open_validated_content(source_path, file.size())
            .map(PreparedLocalDirSource::compatible_cache)
            .map(Some)
            .map_err(|_source| LocalDirSourceError::invalid())
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExactSelection {
    paths: Box<[RepoPath]>,
    id: SelectionId,
}

impl ExactSelection {
    pub(super) fn new(paths: &[RepoPath]) -> Result<Self, ValidationError> {
        let mut paths = paths.to_vec();
        paths.sort_unstable();
        paths.dedup();
        let id = SelectionId::derive(&paths)?;
        Ok(Self {
            paths: paths.into_boxed_slice(),
            id,
        })
    }

    pub(super) fn paths(&self) -> &[RepoPath] {
        &self.paths
    }

    pub(super) const fn id(&self) -> &SelectionId {
        &self.id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Binding {
    digest: BlobDigest,
    size: u64,
}

#[derive(Debug)]
struct PreparedSnapshot {
    commit: CommitId,
    selection: ExactSelection,
    files: Box<[PreparedFile]>,
    bindings: BTreeMap<HubBlobKey, Binding>,
}

impl PreparedSnapshot {
    fn read(
        reader: &HubCacheReader,
        index: &super::hub_cache::HubCacheIndex,
        selection: ExactSelection,
    ) -> Result<Self, CompatibleCacheError> {
        let mut files = Vec::with_capacity(selection.paths.len());
        let mut bindings = BTreeMap::new();
        for path in &selection.paths {
            let file = reader.read_snapshot_file(index, path)?;
            let binding = Binding {
                digest: file.digest(),
                size: file.size(),
            };
            if bindings
                .insert(file.hub_blob_key().clone(), binding)
                .is_some_and(|existing| existing != binding)
            {
                return Err(CompatibleCacheError::corrupt());
            }
            files.push(PreparedFile {
                path: path.clone(),
                file,
            });
        }
        Ok(Self {
            commit: index.commit().clone(),
            selection,
            files: files.into_boxed_slice(),
            bindings,
        })
    }
}

#[derive(Debug)]
struct PreparedFile {
    path: RepoPath,
    file: HubCacheFile,
}

impl PreparedFile {
    fn record(&self) -> SnapshotFileRecord {
        SnapshotFileRecord::new(
            &self.path,
            self.file.digest(),
            self.file.size(),
            Some(self.file.hub_blob_key().clone()),
        )
    }
}

fn open_shared_cache(
    root: &Path,
    endpoint: &Endpoint,
    spec: &RepositorySpec,
    effects: Effects,
) -> Result<(HubCacheReader, CacheKernel), CompatibleCacheError> {
    let layout = super::hub_layout::HubCacheLayout::shared(root, endpoint, spec)?;
    let authority = effects
        .open_cache_authority(layout.sidecar().capability_root())
        .map_err(HubCacheReadError::from)?;
    let reader = HubCacheReader::from_layout(layout, authority.reader())?;
    let sidecar = CacheKernel::for_compatible_cache(reader.layout(), authority.writer(), effects)?;
    Ok((reader, sidecar))
}

#[derive(Clone, Debug)]
pub(super) struct CompatibleSnapshot {
    commit: CommitId,
    selection: SelectionId,
    files: Box<[CompatibleSnapshotFile]>,
    lease: Arc<SnapshotLease>,
}

impl PartialEq for CompatibleSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.commit == other.commit
            && self.selection == other.selection
            && self.files == other.files
    }
}

impl Eq for CompatibleSnapshot {}

impl CompatibleSnapshot {
    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) const fn selection(&self) -> &SelectionId {
        &self.selection
    }

    pub(super) fn files(&self) -> &[CompatibleSnapshotFile] {
        &self.files
    }

    pub(super) fn lease(&self) -> &Arc<SnapshotLease> {
        &self.lease
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CompatibleSnapshotFile {
    path: RepoPath,
    content_path: PathBuf,
    blob_path: Option<PathBuf>,
    hub_blob_key: HubBlobKey,
    digest: BlobDigest,
    size: u64,
    form: HubSnapshotFileForm,
}

impl CompatibleSnapshotFile {
    fn new(path: RepoPath, file: &HubCacheFile) -> Self {
        Self {
            path,
            content_path: file.path().to_path_buf(),
            blob_path: file.blob_path().map(Path::to_path_buf),
            hub_blob_key: file.hub_blob_key().clone(),
            digest: file.digest(),
            size: file.size(),
            form: file.form(),
        }
    }

    pub(super) const fn path(&self) -> &RepoPath {
        &self.path
    }

    pub(super) fn content_path(&self) -> &Path {
        &self.content_path
    }

    pub(super) fn blob_path(&self) -> Option<&Path> {
        self.blob_path.as_deref()
    }

    pub(super) const fn hub_blob_key(&self) -> &HubBlobKey {
        &self.hub_blob_key
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }

    pub(super) const fn form(&self) -> HubSnapshotFileForm {
        self.form
    }
}

#[derive(Debug)]
pub(super) struct CompatibleCacheError {
    kind: Box<CompatibleCacheErrorKind>,
    may_have_published: bool,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum CompatibleCacheErrorKind {
    Hub(HubCacheReadError),
    Sidecar(CacheError),
    Validation(ValidationError),
    Incomplete,
    Corrupt,
}

impl CompatibleCacheError {
    fn new(kind: CompatibleCacheErrorKind, may_have_published: bool) -> Self {
        Self {
            kind: Box::new(kind),
            may_have_published,
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) fn corrupt() -> Self {
        Self::new(CompatibleCacheErrorKind::Corrupt, false)
    }

    pub(super) fn incomplete() -> Self {
        Self::new(CompatibleCacheErrorKind::Incomplete, false)
    }

    pub(super) fn with_may_have_published(mut self) -> Self {
        self.may_have_published = true;
        self
    }

    pub(super) fn is_incomplete(&self) -> bool {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Incomplete => true,
            CompatibleCacheErrorKind::Hub(source) => source.is_missing() || source.is_incomplete(),
            CompatibleCacheErrorKind::Sidecar(_)
            | CompatibleCacheErrorKind::Validation(_)
            | CompatibleCacheErrorKind::Corrupt => false,
        }
    }

    pub(super) fn is_corrupt(&self) -> bool {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Corrupt => true,
            CompatibleCacheErrorKind::Hub(source) => source.is_corrupt(),
            CompatibleCacheErrorKind::Sidecar(source) => source.is_corrupt_record(),
            CompatibleCacheErrorKind::Validation(_) | CompatibleCacheErrorKind::Incomplete => false,
        }
    }

    pub(super) fn is_unsafe(&self) -> bool {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Hub(source) => source.is_unsafe(),
            CompatibleCacheErrorKind::Validation(source) => source.is_unsafe_path(),
            CompatibleCacheErrorKind::Sidecar(source) => source.is_unsafe(),
            CompatibleCacheErrorKind::Incomplete | CompatibleCacheErrorKind::Corrupt => false,
        }
    }

    pub(super) fn is_unsupported_version(&self) -> bool {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Hub(source) => source.is_unsupported_version(),
            CompatibleCacheErrorKind::Sidecar(source) => source.is_unsupported_record(),
            CompatibleCacheErrorKind::Validation(_)
            | CompatibleCacheErrorKind::Incomplete
            | CompatibleCacheErrorKind::Corrupt => false,
        }
    }

    pub(super) const fn may_have_published(&self) -> bool {
        self.may_have_published
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for CompatibleCacheError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Hub(_) => {
                formatter.write_str("compatible cache content validation failed")
            }
            CompatibleCacheErrorKind::Sidecar(_) => {
                formatter.write_str("compatible cache sidecar operation failed")
            }
            CompatibleCacheErrorKind::Validation(_) => {
                formatter.write_str("compatible cache identity validation failed")
            }
            CompatibleCacheErrorKind::Incomplete => {
                formatter.write_str("compatible cache snapshot is incomplete")
            }
            CompatibleCacheErrorKind::Corrupt => {
                formatter.write_str("compatible cache snapshot is corrupt")
            }
        }
    }
}

impl Error for CompatibleCacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            CompatibleCacheErrorKind::Hub(source) => Some(source),
            CompatibleCacheErrorKind::Sidecar(source) => Some(source),
            CompatibleCacheErrorKind::Validation(source) => Some(source),
            CompatibleCacheErrorKind::Incomplete | CompatibleCacheErrorKind::Corrupt => None,
        }
    }
}

impl From<HubCacheReadError> for CompatibleCacheError {
    fn from(source: HubCacheReadError) -> Self {
        Self::new(CompatibleCacheErrorKind::Hub(source), false)
    }
}

impl From<CacheError> for CompatibleCacheError {
    fn from(source: CacheError) -> Self {
        if source.is_not_found() {
            Self::new(CompatibleCacheErrorKind::Incomplete, false)
        } else {
            let may_have_published = source.may_have_published();
            Self::new(
                CompatibleCacheErrorKind::Sidecar(source),
                may_have_published,
            )
        }
    }
}

impl From<ValidationError> for CompatibleCacheError {
    fn from(source: ValidationError) -> Self {
        Self::new(CompatibleCacheErrorKind::Validation(source), false)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tempfile::TempDir;

    use crate::RepositoryId;

    use super::*;
    use crate::cache::hub_layout::HubCacheLayout;
    use crate::cache::hub_metadata::{HubTree, HubTreeEntry, encode_ref, encode_tree};
    use crate::cache::local_dir_layout::HubLocalDirLayout;
    use crate::cache::local_dir_materialization::{ExistingFilePolicy, LocalDirFileMaterializer};
    use crate::cache::metadata::{HubBlobBindingRecord, encode_record};
    use crate::cache::publication::{
        NoPublicationFaults, OsFileSystem, PublicationFaults, PublicationPoint, RandomOperationIds,
        SystemClock,
    };
    use crate::cache::rooted_fs::{CacheRoot, RootedFileSystem};

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_COMMIT: &str = "89abcdef0123456789abcdef0123456789abcdef";
    const CONFIG_BYTES: &[u8] = b"config\n";
    const WEIGHTS_BYTES: &[u8] = b"weights\n";

    struct Fixture {
        directory: TempDir,
        root: PathBuf,
        endpoint: Endpoint,
        spec: RepositorySpec,
        commit: CommitId,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let root = directory.path().join("hub");
            let endpoint = Endpoint::hugging_face();
            let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let commit = CommitId::parse(COMMIT)?;
            let fixture = Self {
                directory,
                root,
                endpoint,
                spec,
                commit,
            };
            fixture.write_cache()?;
            Ok(fixture)
        }

        fn effects() -> Effects {
            Self::effects_with(Arc::new(NoPublicationFaults))
        }

        fn effects_with(faults: Arc<dyn PublicationFaults>) -> Effects {
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(RandomOperationIds),
                Arc::new(SystemClock),
                faults,
            )
        }

        fn write_cache(&self) -> Result<(), Box<dyn Error>> {
            let layout = self.layout()?;
            let config = RepoPath::parse("config.json")?;
            let weights = RepoPath::parse("weights/model.bin")?;
            let config_entry = git_entry(CONFIG_BYTES)?;
            let weights_entry = git_entry(WEIGHTS_BYTES)?;
            let tree = HubTree::new([
                (config.clone(), config_entry.clone()),
                (weights.clone(), weights_entry.clone()),
            ])?;
            write(&layout.tree_path(&self.commit), &encode_tree(&tree)?)?;
            write(
                &layout.ref_path(&Revision::parse("main")?)?,
                &encode_ref(&self.commit),
            )?;
            write(
                &layout.ref_path(&Revision::parse("refs/pr/7")?)?,
                &encode_ref(&self.commit),
            )?;
            for (path, bytes, entry) in [
                (&config, CONFIG_BYTES, config_entry),
                (&weights, WEIGHTS_BYTES, weights_entry),
            ] {
                write(&layout.snapshot_file(&self.commit, path), bytes)?;
                let key = crate::cache::hub_layout::HubBlobKey::parse(entry.blob_id())?;
                write(&layout.blob_path(&key), bytes)?;
            }
            Ok(())
        }

        fn importer(&self) -> Result<CompatibleSnapshotImporter, CompatibleCacheError> {
            CompatibleSnapshotImporter::shared(
                &self.root,
                &self.endpoint,
                &self.spec,
                Self::effects(),
            )
        }

        fn offline(&self) -> Result<CompatibleCacheOffline, CompatibleCacheError> {
            CompatibleCacheOffline::shared(&self.root, &self.endpoint, &self.spec, Self::effects())
        }

        fn importer_with_faults(
            &self,
            faults: Arc<dyn PublicationFaults>,
        ) -> Result<CompatibleSnapshotImporter, CompatibleCacheError> {
            CompatibleSnapshotImporter::shared(
                &self.root,
                &self.endpoint,
                &self.spec,
                Self::effects_with(faults),
            )
        }

        fn layout(&self) -> Result<HubCacheLayout, ValidationError> {
            HubCacheLayout::shared(&self.root, &self.endpoint, &self.spec)
        }

        fn overwrite_tree(
            &self,
            entries: impl IntoIterator<Item = (RepoPath, HubTreeEntry)>,
        ) -> Result<(), Box<dyn Error>> {
            let tree = HubTree::new(entries)?;
            fs::write(self.layout()?.tree_path(&self.commit), encode_tree(&tree)?)?;
            Ok(())
        }
    }

    #[test]
    fn imports_every_file_before_publishing_a_sorted_exact_manifest() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let requested = [
            RepoPath::parse("weights/model.bin")?,
            RepoPath::parse("config.json")?,
        ];

        let snapshot = fixture
            .importer()?
            .import(&Revision::parse("refs/pr/7")?, &requested)?;

        assert_eq!(snapshot.commit(), &fixture.commit);
        assert_eq!(snapshot.selection(), &SelectionId::derive(&requested)?);
        assert_eq!(
            snapshot
                .files()
                .iter()
                .map(|file| file.path().as_str())
                .collect::<Vec<_>>(),
            ["config.json", "weights/model.bin"]
        );
        assert_eq!(
            fixture
                .offline()?
                .open(&Revision::parse(COMMIT)?, &requested)?,
            snapshot
        );
        for file in snapshot.files() {
            assert!(file.content_path().is_file());
            assert!(file.blob_path().is_some_and(Path::is_file));
            assert_eq!(file.form(), HubSnapshotFileForm::CopiedWithBlob);
            assert_eq!(file.size(), fs::metadata(file.content_path())?.len());
            assert_eq!(
                file.digest(),
                BlobDigest::for_bytes(&fs::read(file.content_path())?)
            );
            assert!(!file.hub_blob_key().as_str().is_empty());
        }
        Ok(())
    }

    #[test]
    fn compatible_snapshot_candidate_materializes_an_independent_local_file_without_transport()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let importer = fixture.importer()?;
        let snapshot = importer.import(&Revision::parse("main")?, std::slice::from_ref(&path))?;
        let offline = fixture.offline()?;
        let mut candidates = offline.local_dir_candidates(&snapshot);
        let entry = git_entry(CONFIG_BYTES)?;
        let target = LocalDirFileTarget::new(path, entry, BlobDigest::for_bytes(CONFIG_BYTES));

        let prepared = candidates
            .prepare_local(
                &target,
                &super::super::local_dir_materialization::NeverCancelled,
            )?
            .ok_or("compatible cache candidate was not found")?;
        let mut reader = prepared.into_reader();
        let local_root = fixture.directory.path().join("local-dir");
        fs::create_dir(&local_root)?;
        let layout = HubLocalDirLayout::new(&local_root, &fixture.endpoint, &fixture.spec)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(&local_root)?);
        let materializer = LocalDirFileMaterializer::from_layout(
            layout,
            root,
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(RandomOperationIds),
                Arc::new(SystemClock),
                Arc::new(NoPublicationFaults),
            ),
        );
        let copied_file =
            materializer.materialize(&target, reader.as_mut(), ExistingFilePolicy::Reject)?;
        assert_eq!(fs::read(copied_file.path())?, CONFIG_BYTES);

        fs::write(copied_file.path(), b"user edit")?;
        assert_eq!(fs::read(snapshot.files()[0].content_path())?, CONFIG_BYTES);
        if let Some(blob_path) = snapshot.files()[0].blob_path() {
            assert_eq!(fs::read(blob_path)?, CONFIG_BYTES);
        }
        Ok(())
    }

    #[test]
    fn failed_prevalidation_publishes_no_sidecar() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let requested = [
            RepoPath::parse("config.json")?,
            RepoPath::parse("missing.bin")?,
        ];

        let error = fixture
            .importer()?
            .import(&Revision::parse("main")?, &requested)
            .expect_err("an absent selected file must fail import");

        assert!(error.is_incomplete());
        let layout = HubCacheLayout::shared(&fixture.root, &fixture.endpoint, &fixture.spec)?;
        assert!(!layout.repository_directory().join(".hf-store").exists());
        Ok(())
    }

    #[test]
    fn importer_keeps_reads_and_sidecars_bound_to_one_open_root() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let opened_root = fixture.directory.path().join("opened-hub");
        fs::rename(&fixture.root, &opened_root)?;
        create_root_link(&opened_root, &fixture.root)?;
        let importer = fixture.importer()?;
        remove_root_link(&fixture.root)?;
        fs::create_dir(&fixture.root)?;
        let config = RepoPath::parse("config.json")?;
        let selection = SelectionId::derive(std::slice::from_ref(&config))?;

        let snapshot = importer.import(&Revision::parse("main")?, std::slice::from_ref(&config))?;

        assert_eq!(snapshot.commit(), &fixture.commit);
        let opened_layout = HubCacheLayout::shared(&opened_root, &fixture.endpoint, &fixture.spec)?;
        assert!(
            opened_layout
                .sidecar()
                .snapshot_manifest(&fixture.commit, &selection)
                .is_file()
        );
        assert!(fs::read_dir(&fixture.root)?.next().is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn import_rejects_a_redirected_sidecar_directory() -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        symlink(
            outside.path(),
            fixture.layout()?.repository_directory().join(".hf-store"),
        )?;
        let config = RepoPath::parse("config.json")?;

        let error = fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("compatible import followed a redirected sidecar directory");

        assert!(error.is_unsafe());
        assert!(fs::read_dir(outside.path())?.next().is_none());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn import_rejects_a_redirected_sidecar_directory() -> Result<(), Box<dyn Error>> {
        use std::os::windows::fs::symlink_dir;

        let fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        let link = fixture.layout()?.repository_directory().join(".hf-store");
        if let Err(error) = symlink_dir(outside.path(), link) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return Ok(());
            }
            return Err(error.into());
        }
        let config = RepoPath::parse("config.json")?;

        let error = fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("compatible import followed a redirected sidecar directory");

        assert!(error.is_unsafe());
        assert!(fs::read_dir(outside.path())?.next().is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn offline_rejects_a_redirected_sidecar_directory() -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        let sidecar = fixture.layout()?.repository_directory().join(".hf-store");
        let outside = TempDir::new()?;
        let redirected = outside.path().join("redirected");
        fs::rename(&sidecar, &redirected)?;
        symlink(&redirected, &sidecar)?;

        let error = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("compatible offline lookup followed a redirected sidecar directory");
        assert!(error.is_unsafe());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn offline_rejects_a_redirected_sidecar_directory() -> Result<(), Box<dyn Error>> {
        use std::os::windows::fs::symlink_dir;

        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        let sidecar = fixture.layout()?.repository_directory().join(".hf-store");
        let outside = TempDir::new()?;
        let redirected = outside.path().join("redirected");
        fs::rename(&sidecar, &redirected)?;
        if let Err(error) = symlink_dir(&redirected, &sidecar) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return Ok(());
            }
            return Err(error.into());
        }

        let error = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("compatible offline lookup followed a redirected sidecar directory");
        assert!(error.is_unsafe());
        Ok(())
    }

    #[test]
    fn empty_selection_is_exact_and_never_copies_compatible_bytes() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;

        let first = fixture.importer()?.import(&Revision::parse("main")?, &[])?;
        let second = fixture.importer()?.import(&Revision::parse(COMMIT)?, &[])?;

        assert!(first.files().is_empty());
        assert_eq!(first, second);
        let layout = fixture.layout()?;
        assert!(
            layout
                .sidecar()
                .snapshot_manifest(&fixture.commit, first.selection())
                .is_file()
        );
        assert!(!fixture.root.join("hf-store-v1").exists());
        assert!(
            layout
                .repository_directory()
                .join(".hf-store/hf-store-v1")
                .is_dir()
        );
        assert!(
            !layout
                .sidecar()
                .blob_path(&BlobDigest::for_bytes(CONFIG_BYTES))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn import_is_idempotent_and_deduplicates_exact_paths() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let requested = [config.clone(), config];
        let importer = fixture.importer()?;

        let first = importer.import(&Revision::parse("main")?, &requested)?;
        let manifest = fixture
            .layout()?
            .sidecar()
            .snapshot_manifest(&fixture.commit, first.selection());
        let first_bytes = fs::read(&manifest)?;
        let second = importer.import(&Revision::parse("main")?, &requested)?;

        assert_eq!(first, second);
        assert_eq!(first.files().len(), 1);
        assert_eq!(fs::read(manifest)?, first_bytes);
        Ok(())
    }

    #[test]
    fn offline_requires_the_exact_selection_and_current_python_state() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let before_import = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("Python files without an exact sidecar are not an offline hit");
        assert!(before_import.is_incomplete());
        assert!(
            !fixture
                .layout()?
                .repository_directory()
                .join(".hf-store")
                .exists()
        );

        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;

        let weights = RepoPath::parse("weights/model.bin")?;
        let wrong = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&weights))
            .expect_err("a sidecar for another selection must not be reused");
        assert!(wrong.is_incomplete());

        fs::remove_file(fixture.layout()?.tree_path(&fixture.commit))?;
        let sidecar_only = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("sidecar-only state must not establish completeness");
        assert!(sidecar_only.is_incomplete());
        Ok(())
    }

    #[test]
    fn offline_classifies_corrupt_and_unsupported_sidecar_metadata() -> Result<(), Box<dyn Error>> {
        let config = RepoPath::parse("config.json")?;

        let corrupt_fixture = Fixture::new()?;
        let corrupt_snapshot = corrupt_fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        let corrupt_manifest = corrupt_fixture
            .layout()?
            .sidecar()
            .snapshot_manifest(&corrupt_fixture.commit, corrupt_snapshot.selection());
        fs::write(corrupt_manifest, b"not-json\n")?;

        let corrupt = corrupt_fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("malformed sidecar metadata must fail closed");
        assert!(corrupt.is_corrupt());
        assert!(!corrupt.is_unsupported_version());

        let unsupported_fixture = Fixture::new()?;
        let unsupported_snapshot = unsupported_fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        let unsupported_manifest = unsupported_fixture.layout()?.sidecar().snapshot_manifest(
            &unsupported_fixture.commit,
            unsupported_snapshot.selection(),
        );
        fs::write(unsupported_manifest, b"{\"format_version\":2}\n")?;

        let unsupported = unsupported_fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("future sidecar metadata must not be interpreted");
        assert!(unsupported.is_unsupported_version());
        assert!(!unsupported.is_corrupt());
        Ok(())
    }

    #[test]
    fn offline_rejects_snapshot_blob_and_binding_mutation() -> Result<(), Box<dyn Error>> {
        for case in ["snapshot", "truncated", "blob", "binding"] {
            let fixture = Fixture::new()?;
            let config = RepoPath::parse("config.json")?;
            let snapshot = fixture
                .importer()?
                .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
            let file = snapshot.files().first().ok_or("snapshot file missing")?;
            match case {
                "snapshot" => fs::write(file.content_path(), b"CONFIG\n")?,
                "truncated" => fs::write(file.content_path(), b"config")?,
                "blob" => fs::write(
                    file.blob_path().ok_or("retained blob missing")?,
                    b"CONFIG\n",
                )?,
                "binding" => {
                    let path = fixture
                        .layout()?
                        .sidecar()
                        .hub_blob_binding_record(file.hub_blob_key())?;
                    let replacement = HubBlobBindingRecord::new(
                        file.hub_blob_key(),
                        BlobDigest::for_bytes(b"different"),
                        file.size(),
                    );
                    fs::write(path, encode_record(&replacement)?)?;
                }
                _ => return Err("unknown mutation case".into()),
            }

            let error = fixture
                .offline()?
                .open(&Revision::parse("main")?, std::slice::from_ref(&config))
                .expect_err("mutated compatible state must not be reused");
            assert!(error.is_corrupt());
        }
        Ok(())
    }

    #[test]
    fn offline_revalidates_ref_tree_and_hub_blob_key() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;

        fixture.overwrite_tree([(
            config.clone(),
            HubTreeEntry::new(CONFIG_BYTES.len() as u64, "changed-opaque-key")?,
        )])?;
        let tree_error = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("a substituted tree key must not be accepted");
        assert!(tree_error.is_corrupt());

        fixture.write_cache()?;
        write(
            &fixture.layout()?.ref_path(&Revision::parse("main")?)?,
            &encode_ref(&CommitId::parse(OTHER_COMMIT)?),
        )?;
        let ref_error = fixture
            .offline()?
            .open(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("a moved ref without a complete imported snapshot must fail");
        assert!(ref_error.is_incomplete());
        Ok(())
    }

    #[test]
    fn binding_faults_may_leave_bindings_but_never_publish_a_manifest() -> Result<(), Box<dyn Error>>
    {
        for point in [
            PublicationPoint::BeforeAtomicReplace,
            PublicationPoint::AfterAtomicReplace,
        ] {
            let fixture = Fixture::new()?;
            fixture.importer()?.import(&Revision::parse("main")?, &[])?;
            let config = RepoPath::parse("config.json")?;
            let selection = SelectionId::derive(std::slice::from_ref(&config))?;

            let error = fixture
                .importer_with_faults(Arc::new(FailOnce::new(point)))?
                .import(&Revision::parse("main")?, std::slice::from_ref(&config))
                .expect_err("an injected binding publication fault must surface");

            assert!(error.may_have_published());
            assert!(
                !fixture
                    .layout()?
                    .sidecar()
                    .snapshot_manifest(&fixture.commit, &selection)
                    .exists()
            );
        }
        Ok(())
    }

    #[test]
    fn later_binding_failure_reports_prior_inert_publication() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        fixture.importer()?.import(&Revision::parse("main")?, &[])?;
        let requested = [
            RepoPath::parse("config.json")?,
            RepoPath::parse("weights/model.bin")?,
        ];

        let error = fixture
            .importer_with_faults(Arc::new(FailOnNth::new(
                PublicationPoint::BeforeAtomicReplace,
                2,
            )))?
            .import(&Revision::parse("main")?, &requested)
            .expect_err("the second binding publication must fail");

        assert!(error.may_have_published());
        let layout = fixture.layout()?;
        let existing = [CONFIG_BYTES, WEIGHTS_BYTES]
            .iter()
            .map(|bytes| -> Result<bool, Box<dyn Error>> {
                let entry = git_entry(bytes)?;
                let key = HubBlobKey::parse(entry.blob_id())?;
                Ok(layout.sidecar().hub_blob_binding_record(&key)?.is_file())
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(existing.iter().filter(|exists| **exists).count(), 1);
        let selection = SelectionId::derive(&requested)?;
        assert!(
            !layout
                .sidecar()
                .snapshot_manifest(&fixture.commit, &selection)
                .exists()
        );
        Ok(())
    }

    #[test]
    fn manifest_prepublication_failure_leaves_only_inert_bindings() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let weights = RepoPath::parse("weights/model.bin")?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&weights))?;
        let requested = [config, weights];
        let selection = SelectionId::derive(&requested)?;

        let error = fixture
            .importer_with_faults(Arc::new(FailOnce::new(
                PublicationPoint::BeforeAtomicReplace,
            )))?
            .import(&Revision::parse("main")?, &requested)
            .expect_err("the exact manifest publication must fail before replacement");

        assert!(error.may_have_published());
        assert!(
            !fixture
                .layout()?
                .sidecar()
                .snapshot_manifest(&fixture.commit, &selection)
                .exists()
        );
        Ok(())
    }

    #[test]
    fn ambiguous_complete_manifest_publication_is_reconciled_as_success()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let weights = RepoPath::parse("weights/model.bin")?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;
        fixture
            .importer()?
            .import(&Revision::parse("main")?, std::slice::from_ref(&weights))?;
        let requested = [weights, config];

        let snapshot = fixture
            .importer_with_faults(Arc::new(FailOnce::new(
                PublicationPoint::AfterAtomicReplace,
            )))?
            .import(&Revision::parse("main")?, &requested)?;

        assert_eq!(snapshot.files().len(), 2);
        assert_eq!(
            fixture
                .offline()?
                .open(&Revision::parse("main")?, &requested)?,
            snapshot
        );
        Ok(())
    }

    #[test]
    fn symbolic_ref_movement_during_publication_returns_the_validated_commit()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let ref_path = fixture.layout()?.ref_path(&Revision::parse("main")?)?;
        let move_ref = Arc::new(RewriteOnce::new(
            PublicationPoint::BeforeAtomicReplace,
            ref_path,
            encode_ref(&CommitId::parse(OTHER_COMMIT)?),
        ));
        let config = RepoPath::parse("config.json")?;

        let snapshot = fixture
            .importer_with_faults(move_ref)?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))?;

        assert_eq!(snapshot.commit(), &fixture.commit);
        assert_eq!(
            fs::read(fixture.layout()?.ref_path(&Revision::parse("main")?)?)?,
            encode_ref(&CommitId::parse(OTHER_COMMIT)?)
        );
        Ok(())
    }

    #[test]
    fn final_revalidation_failure_reports_that_sidecars_were_published()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let snapshot_path = fixture.layout()?.snapshot_file(&fixture.commit, &config);
        let mutate = Arc::new(RewriteOnce::new(
            PublicationPoint::AfterAtomicReplace,
            snapshot_path,
            b"CONFIG\n".to_vec(),
        ));

        let error = fixture
            .importer_with_faults(mutate)?
            .import(&Revision::parse("main")?, std::slice::from_ref(&config))
            .expect_err("mutation after validation must fail final revalidation");

        assert!(error.is_corrupt());
        assert!(error.may_have_published());
        let selection = SelectionId::derive(std::slice::from_ref(&config))?;
        assert!(
            fixture
                .layout()?
                .sidecar()
                .snapshot_manifest(&fixture.commit, &selection)
                .is_file()
        );
        Ok(())
    }

    #[derive(Debug)]
    struct FailOnce {
        point: PublicationPoint,
        failed: AtomicBool,
    }

    impl FailOnce {
        fn new(point: PublicationPoint) -> Self {
            Self {
                point,
                failed: AtomicBool::new(false),
            }
        }
    }

    impl PublicationFaults for FailOnce {
        fn check(&self, point: PublicationPoint) -> io::Result<()> {
            if point == self.point && !self.failed.swap(true, Ordering::AcqRel) {
                Err(io::Error::other("injected compatible publication fault"))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug)]
    struct FailOnNth {
        point: PublicationPoint,
        target: usize,
        observed: AtomicUsize,
    }

    impl FailOnNth {
        fn new(point: PublicationPoint, target: usize) -> Self {
            Self {
                point,
                target,
                observed: AtomicUsize::new(0),
            }
        }
    }

    impl PublicationFaults for FailOnNth {
        fn check(&self, point: PublicationPoint) -> io::Result<()> {
            if point != self.point {
                return Ok(());
            }
            let observed = self.observed.fetch_add(1, Ordering::AcqRel) + 1;
            if observed == self.target {
                Err(io::Error::other("injected nth publication fault"))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug)]
    struct RewriteOnce {
        point: PublicationPoint,
        path: PathBuf,
        bytes: Vec<u8>,
        rewritten: AtomicBool,
    }

    impl RewriteOnce {
        fn new(point: PublicationPoint, path: PathBuf, bytes: Vec<u8>) -> Self {
            Self {
                point,
                path,
                bytes,
                rewritten: AtomicBool::new(false),
            }
        }
    }

    impl PublicationFaults for RewriteOnce {
        fn check(&self, point: PublicationPoint) -> io::Result<()> {
            if point == self.point && !self.rewritten.swap(true, Ordering::AcqRel) {
                fs::write(&self.path, &self.bytes)?;
            }
            Ok(())
        }
    }

    fn git_entry(bytes: &[u8]) -> Result<HubTreeEntry, Box<dyn Error>> {
        use sha1::{Digest, Sha1};

        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
        hasher.update(bytes);
        Ok(HubTreeEntry::new(
            bytes.len() as u64,
            format!("{:x}", hasher.finalize()),
        )?)
    }

    fn write(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(path.parent().ok_or("fixture path has no parent")?)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    #[cfg(unix)]
    fn create_root_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_root_link(target: &Path, link: &Path) -> io::Result<()> {
        let output = std::process::Command::new("cmd.exe")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "failed to create test junction: {}",
                String::from_utf8_lossy(&output.stderr)
            )))
        }
    }

    #[cfg(unix)]
    fn remove_root_link(link: &Path) -> io::Result<()> {
        fs::remove_file(link)
    }

    #[cfg(windows)]
    fn remove_root_link(link: &Path) -> io::Result<()> {
        fs::remove_dir(link)
    }
}
