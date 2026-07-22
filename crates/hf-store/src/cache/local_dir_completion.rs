//! Atomic logical-completion state for user-owned local directories.

use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::validation::ValidationError;
use crate::{CommitId, RepoPath};

use super::key::{BlobDigest, SelectionId};
use super::local_dir_layout::HubLocalDirLayout;
use super::metadata::{
    LocalDirFileRecord, LocalDirStateRecord, MetadataError, decode_record, encode_record,
};
use super::publication::{Effects, PublicationPoint};
use super::rooted_fs::{RootedFileSystem, RootedRead, RootedRegularFile};
use super::sanitized_io::SanitizedIo;

const MAX_LOCAL_DIR_STATE_BYTES: usize = 16 * 1024 * 1024;
const HASH_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LocalDirCompletionFile {
    path: RepoPath,
    size: u64,
    digest: BlobDigest,
}

impl LocalDirCompletionFile {
    pub(super) const fn new(path: RepoPath, size: u64, digest: BlobDigest) -> Self {
        Self { path, size, digest }
    }
}

#[derive(Clone)]
pub(super) struct LocalDirCompletionWriter {
    layout: HubLocalDirLayout,
    root: Arc<dyn RootedFileSystem>,
    effects: Effects,
}

impl LocalDirCompletionWriter {
    pub(super) fn new(
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    ) -> Result<Self, LocalDirCompletionError> {
        let _state = layout.coordination_state_relative()?;
        Ok(Self {
            layout,
            root,
            effects,
        })
    }

    pub(super) fn publish_in_progress(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<(), LocalDirCompletionError> {
        let sidecar = self.layout.completion_sidecar();
        let state = LocalDirStateRecord::in_progress(
            sidecar.origin_key(),
            sidecar.repository_key(),
            commit,
            selection,
        )?;
        self.publish(&state)
    }

    pub(super) fn publish_complete(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
        files: &[LocalDirCompletionFile],
    ) -> Result<(), LocalDirCompletionError> {
        let files = files
            .iter()
            .map(|file| LocalDirFileRecord::new(&file.path, file.digest, file.size))
            .collect();
        let sidecar = self.layout.completion_sidecar();
        let state = LocalDirStateRecord::complete(
            sidecar.origin_key(),
            sidecar.repository_key(),
            commit,
            selection,
            files,
        )?;
        self.publish(&state)
    }

    fn publish(&self, state: &LocalDirStateRecord) -> Result<(), LocalDirCompletionError> {
        let bytes = encode_record(state)?;
        let relative = self.layout.coordination_state_relative()?;
        let parent = relative.parent().ok_or_else(|| {
            LocalDirCompletionError::io(
                &io::Error::new(io::ErrorKind::InvalidInput, "completion path has no parent"),
                false,
            )
        })?;
        self.root.ensure_dir(parent)?;
        let staging = self.effects.next_staging_name()?;
        self.check_fault(PublicationPoint::BeforeCompletionReplace, false)?;
        self.root
            .replace(&relative, &bytes, &staging)
            .map_err(|source| LocalDirCompletionError::io(&source, true))?;
        self.check_fault(PublicationPoint::AfterCompletionReplace, true)?;
        self.root
            .sync_directory(parent)
            .map_err(|source| LocalDirCompletionError::io(&source, true))
    }

    fn check_fault(
        &self,
        point: PublicationPoint,
        may_have_published: bool,
    ) -> Result<(), LocalDirCompletionError> {
        self.effects
            .check_publication_fault(point)
            .map_err(|source| LocalDirCompletionError::io(&source, may_have_published))
    }
}

impl fmt::Debug for LocalDirCompletionWriter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirCompletionWriter")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(super) struct LocalDirOfflineReader {
    layout: HubLocalDirLayout,
    root: Arc<dyn RootedFileSystem>,
    state_relative: PathBuf,
    lock_relative: PathBuf,
}

impl LocalDirOfflineReader {
    pub(super) fn new(
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
    ) -> Result<Self, LocalDirOfflineError> {
        let state_relative = layout.coordination_state_relative()?;
        let lock_relative = layout.coordination_lock_relative()?;
        Ok(Self {
            layout,
            root,
            state_relative,
            lock_relative,
        })
    }

    pub(super) fn open(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<LocalDirOfflineSnapshot, LocalDirOfflineError> {
        let _guard = self.root.lock_exclusive(&self.lock_relative)?;
        let state = self.read_state()?;
        self.validate_identity(&state, commit, selection)?;
        let mut files = Vec::with_capacity(state.files().len());
        for record in state.files() {
            let path = record.repo_path()?;
            let expected_digest = record.digest()?;
            let destination = self.layout.file_path(&path)?;
            let relative = self.layout.capability_relative(&destination)?;
            let actual_digest = self.hash_file(relative, record.size())?;
            if actual_digest != expected_digest {
                return Err(LocalDirOfflineError::incomplete());
            }
            files.push(LocalDirOfflineFile {
                path,
                destination,
                size: record.size(),
                digest: expected_digest,
            });
        }
        if self.read_state()? != state {
            return Err(LocalDirOfflineError::stale());
        }
        Ok(LocalDirOfflineSnapshot {
            commit: commit.clone(),
            selection: *selection,
            files: files.into_boxed_slice(),
        })
    }

    fn read_state(&self) -> Result<LocalDirStateRecord, LocalDirOfflineError> {
        let bytes = match self
            .root
            .read_regular_bounded(&self.state_relative, MAX_LOCAL_DIR_STATE_BYTES)?
        {
            RootedRead::Bytes(bytes) => bytes,
            RootedRead::Missing | RootedRead::Other => {
                return Err(LocalDirOfflineError::incomplete());
            }
        };
        Ok(decode_record(&bytes)?)
    }

    fn validate_identity(
        &self,
        state: &LocalDirStateRecord,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<(), LocalDirOfflineError> {
        if !state.is_complete() {
            return Err(LocalDirOfflineError::incomplete());
        }
        let sidecar = self.layout.completion_sidecar();
        if state.origin_key() != sidecar.origin_key().to_string()
            || state.repository_key() != sidecar.repository_key().to_string()
            || state.commit() != commit.as_str()
            || state.selection_id() != selection.to_string()
        {
            return Err(LocalDirOfflineError::stale());
        }
        Ok(())
    }

    fn hash_file(
        &self,
        relative: &std::path::Path,
        expected_size: u64,
    ) -> Result<BlobDigest, LocalDirOfflineError> {
        let (mut reader, size) = match self.root.open_regular(relative)? {
            RootedRegularFile::File { reader, size, .. } => (reader, size),
            RootedRegularFile::Missing | RootedRegularFile::Other => {
                return Err(LocalDirOfflineError::incomplete());
            }
        };
        if size != expected_size {
            return Err(LocalDirOfflineError::incomplete());
        }
        let mut hasher = Sha256::new();
        let mut observed = 0_u64;
        let mut buffer = vec![0_u8; HASH_BUFFER_SIZE].into_boxed_slice();
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            observed = observed
                .checked_add(u64::try_from(read).map_err(io::Error::other)?)
                .ok_or_else(|| io::Error::other("local directory file size overflow"))?;
            if observed > expected_size {
                return Err(LocalDirOfflineError::incomplete());
            }
            hasher.update(&buffer[..read]);
        }
        if observed != expected_size {
            return Err(LocalDirOfflineError::incomplete());
        }
        Ok(BlobDigest::from_bytes(hasher.finalize().into()))
    }
}

impl fmt::Debug for LocalDirOfflineReader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirOfflineReader")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirOfflineSnapshot {
    commit: CommitId,
    selection: SelectionId,
    files: Box<[LocalDirOfflineFile]>,
}

impl LocalDirOfflineSnapshot {
    pub(super) const fn files(&self) -> &[LocalDirOfflineFile] {
        &self.files
    }
}

impl fmt::Debug for LocalDirOfflineSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirOfflineSnapshot")
            .field("file_count", &self.files.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirOfflineFile {
    path: RepoPath,
    destination: PathBuf,
    size: u64,
    digest: BlobDigest,
}

impl LocalDirOfflineFile {
    pub(super) const fn path(&self) -> &RepoPath {
        &self.path
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }
}

impl fmt::Debug for LocalDirOfflineFile {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirOfflineFile")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

pub(super) struct LocalDirOfflineError {
    kind: Box<LocalDirOfflineErrorKind>,
    backtrace: Backtrace,
}

enum LocalDirOfflineErrorKind {
    Io(SanitizedIo),
    Validation(ValidationError),
    Metadata(MetadataError),
    Incomplete,
    Stale,
}

impl LocalDirOfflineError {
    fn new(kind: LocalDirOfflineErrorKind) -> Self {
        Self {
            kind: Box::new(kind),
            backtrace: Backtrace::capture(),
        }
    }

    fn incomplete() -> Self {
        Self::new(LocalDirOfflineErrorKind::Incomplete)
    }

    fn stale() -> Self {
        Self::new(LocalDirOfflineErrorKind::Stale)
    }

    pub(super) fn is_incomplete(&self) -> bool {
        matches!(self.kind.as_ref(), LocalDirOfflineErrorKind::Incomplete)
    }

    pub(super) fn is_stale(&self) -> bool {
        matches!(self.kind.as_ref(), LocalDirOfflineErrorKind::Stale)
    }
}

impl fmt::Debug for LocalDirOfflineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind.as_ref() {
            LocalDirOfflineErrorKind::Io(_) => "Io",
            LocalDirOfflineErrorKind::Validation(_) => "Validation",
            LocalDirOfflineErrorKind::Metadata(_) => "Metadata",
            LocalDirOfflineErrorKind::Incomplete => "Incomplete",
            LocalDirOfflineErrorKind::Stale => "Stale",
        };
        formatter
            .debug_struct("LocalDirOfflineError")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

impl Display for LocalDirOfflineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("local directory is not complete for offline use")
    }
}

impl Error for LocalDirOfflineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            LocalDirOfflineErrorKind::Validation(source) => Some(source),
            LocalDirOfflineErrorKind::Io(_)
            | LocalDirOfflineErrorKind::Metadata(_)
            | LocalDirOfflineErrorKind::Incomplete
            | LocalDirOfflineErrorKind::Stale => None,
        }
    }
}

impl From<io::Error> for LocalDirOfflineError {
    fn from(source: io::Error) -> Self {
        Self::new(LocalDirOfflineErrorKind::Io(SanitizedIo::new(&source)))
    }
}

impl From<ValidationError> for LocalDirOfflineError {
    fn from(source: ValidationError) -> Self {
        Self::new(LocalDirOfflineErrorKind::Validation(source))
    }
}

impl From<MetadataError> for LocalDirOfflineError {
    fn from(source: MetadataError) -> Self {
        Self::new(LocalDirOfflineErrorKind::Metadata(source))
    }
}

pub(super) struct LocalDirCompletionError {
    kind: Box<LocalDirCompletionErrorKind>,
    may_have_published: bool,
    backtrace: Backtrace,
}

enum LocalDirCompletionErrorKind {
    Io(SanitizedIo),
    Validation(ValidationError),
    Metadata(MetadataError),
}

impl LocalDirCompletionError {
    fn new(kind: LocalDirCompletionErrorKind, may_have_published: bool) -> Self {
        Self {
            kind: Box::new(kind),
            may_have_published,
            backtrace: Backtrace::capture(),
        }
    }

    fn io(source: &io::Error, may_have_published: bool) -> Self {
        Self::new(
            LocalDirCompletionErrorKind::Io(SanitizedIo::new(source)),
            may_have_published,
        )
    }

    pub(super) const fn may_have_published(&self) -> bool {
        self.may_have_published
    }

    pub(super) const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl fmt::Debug for LocalDirCompletionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind.as_ref() {
            LocalDirCompletionErrorKind::Io(_) => "Io",
            LocalDirCompletionErrorKind::Validation(_) => "Validation",
            LocalDirCompletionErrorKind::Metadata(_) => "Metadata",
        };
        formatter
            .debug_struct("LocalDirCompletionError")
            .field("kind", &kind)
            .field("may_have_published", &self.may_have_published)
            .finish_non_exhaustive()
    }
}

impl Display for LocalDirCompletionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("local-dir completion state publication failed")
    }
}

impl Error for LocalDirCompletionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            LocalDirCompletionErrorKind::Validation(source) => Some(source),
            LocalDirCompletionErrorKind::Io(_) | LocalDirCompletionErrorKind::Metadata(_) => None,
        }
    }
}

impl From<io::Error> for LocalDirCompletionError {
    fn from(source: io::Error) -> Self {
        Self::io(&source, false)
    }
}

impl From<ValidationError> for LocalDirCompletionError {
    fn from(source: ValidationError) -> Self {
        Self::new(LocalDirCompletionErrorKind::Validation(source), false)
    }
}

impl From<MetadataError> for LocalDirCompletionError {
    fn from(source: MetadataError) -> Self {
        Self::new(LocalDirCompletionErrorKind::Metadata(source), false)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::cache::key::{BlobDigest, SelectionId};
    use crate::cache::local_dir_layout::HubLocalDirLayout;
    use crate::cache::metadata::{LocalDirStateRecord, decode_record};
    use crate::cache::publication::{
        Effects, FaultController, NoPublicationFaults, OsFileSystem, PublicationPoint,
        SequenceOperationIds, SystemClock,
    };
    use crate::cache::rooted_fs::{CacheRoot, RootedFileSystem};
    use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec};

    use super::{LocalDirCompletionFile, LocalDirCompletionWriter, LocalDirOfflineReader};

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn in_progress_replaces_prior_completion_and_complete_is_published_last()
    -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let endpoint = Endpoint::hugging_face();
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(SequenceOperationIds::new(1)),
            Arc::new(SystemClock),
            Arc::new(NoPublicationFaults),
        );
        let writer = LocalDirCompletionWriter::new(layout.clone(), root, effects)?;
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let file = LocalDirCompletionFile::new(
            path,
            u64::try_from(b"payload".len())?,
            BlobDigest::for_bytes(b"payload"),
        );

        writer.publish_complete(&commit, &selection, std::slice::from_ref(&file))?;
        assert!(read_state(&layout)?.is_complete());
        writer.publish_in_progress(&commit, &selection)?;
        let invalidated = read_state(&layout)?;
        assert!(invalidated.is_in_progress());
        assert!(invalidated.files().is_empty());
        writer.publish_complete(&commit, &selection, &[file])?;
        let complete = read_state(&layout)?;
        assert!(complete.is_complete());
        assert_eq!(complete.files()[0].path(), "weights/model.bin");
        assert_eq!(complete.files()[0].size(), 7);
        assert_eq!(
            complete.files()[0].digest()?,
            BlobDigest::for_bytes(b"payload")
        );
        Ok(())
    }

    #[test]
    fn completion_replacement_faults_expose_only_old_or_new_complete_records()
    -> Result<(), Box<dyn Error>> {
        for point in [
            PublicationPoint::BeforeCompletionReplace,
            PublicationPoint::AfterCompletionReplace,
        ] {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
            let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
            let commit = CommitId::parse(COMMIT)?;
            let path = RepoPath::parse("model.bin")?;
            let selection = SelectionId::derive(std::slice::from_ref(&path))?;
            let file = LocalDirCompletionFile::new(
                path,
                u64::try_from(b"payload".len())?,
                BlobDigest::for_bytes(b"payload"),
            );
            LocalDirCompletionWriter::new(
                layout.clone(),
                Arc::clone(&root),
                Effects::new(
                    Arc::new(OsFileSystem),
                    Arc::new(SequenceOperationIds::new(1)),
                    Arc::new(SystemClock),
                    Arc::new(NoPublicationFaults),
                ),
            )?
            .publish_complete(&commit, &selection, &[file])?;

            let faults = Arc::new(FaultController::default());
            faults.fail_once(point);
            let writer = LocalDirCompletionWriter::new(
                layout.clone(),
                root,
                Effects::new(
                    Arc::new(OsFileSystem),
                    Arc::new(SequenceOperationIds::new(100)),
                    Arc::new(SystemClock),
                    faults,
                ),
            )?;
            let error = writer
                .publish_in_progress(&commit, &selection)
                .expect_err("injected completion replacement fault was ignored");
            assert_eq!(
                error.may_have_published(),
                point == PublicationPoint::AfterCompletionReplace
            );
            let visible = read_state(&layout)?;
            if point == PublicationPoint::BeforeCompletionReplace {
                assert!(visible.is_complete());
            } else {
                assert!(visible.is_in_progress());
            }
        }
        Ok(())
    }

    #[test]
    fn offline_open_rehashes_every_file_and_rejects_mutations() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let endpoint = Endpoint::hugging_face();
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let destination = layout.file_path(&path)?;
        fs::create_dir_all(destination.parent().ok_or("file has no parent")?)?;
        fs::write(&destination, b"payload")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let file = LocalDirCompletionFile::new(path.clone(), 7, BlobDigest::for_bytes(b"payload"));
        let writer = LocalDirCompletionWriter::new(
            layout.clone(),
            Arc::clone(&root),
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(SystemClock),
                Arc::new(NoPublicationFaults),
            ),
        )?;
        writer.publish_complete(&commit, &selection, &[file])?;
        let reader = LocalDirOfflineReader::new(layout.clone(), root)?;

        let snapshot = reader.open(&commit, &selection)?;
        assert_eq!(snapshot.files().len(), 1);
        assert_eq!(snapshot.files()[0].path(), &path);
        assert_eq!(
            snapshot.files()[0].digest(),
            BlobDigest::for_bytes(b"payload")
        );

        for changed in [
            b"short".as_slice(),
            b"substit".as_slice(),
            b"payload-extra".as_slice(),
        ] {
            fs::write(&destination, changed)?;
            let error = reader
                .open(&commit, &selection)
                .expect_err("mutated local directory was accepted offline");
            assert!(error.is_incomplete());
        }
        fs::remove_file(&destination)?;
        let error = reader
            .open(&commit, &selection)
            .expect_err("missing local directory file was accepted offline");
        assert!(error.is_incomplete());
        Ok(())
    }

    #[test]
    fn offline_open_rejects_in_progress_and_stale_identity() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let endpoint = Endpoint::hugging_face();
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
        let commit = CommitId::parse(COMMIT)?;
        let selection = SelectionId::derive(&[])?;
        let reader = LocalDirOfflineReader::new(layout.clone(), Arc::clone(&root))?;
        assert!(
            reader
                .open(&commit, &selection)
                .expect_err("accepted a local directory without hf-store completion state")
                .is_incomplete()
        );
        let writer = LocalDirCompletionWriter::new(
            layout.clone(),
            Arc::clone(&root),
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(SystemClock),
                Arc::new(NoPublicationFaults),
            ),
        )?;
        writer.publish_in_progress(&commit, &selection)?;
        assert!(
            reader
                .open(&commit, &selection)
                .expect_err("accepted in-progress state")
                .is_incomplete()
        );
        let other = CommitId::parse("89abcdef0123456789abcdef0123456789abcdef")?;
        writer.publish_complete(&commit, &selection, &[])?;
        assert!(
            reader
                .open(&other, &selection)
                .expect_err("accepted stale commit")
                .is_stale()
        );
        let other_selection = SelectionId::derive(&[RepoPath::parse("other.bin")?])?;
        assert!(
            reader
                .open(&commit, &other_selection)
                .expect_err("accepted a stale path selection")
                .is_stale()
        );
        let other_repository = RepositorySpec::model(RepositoryId::parse("other/repo")?);
        let other_layout = HubLocalDirLayout::new(directory.path(), &endpoint, &other_repository)?;
        let other_reader = LocalDirOfflineReader::new(other_layout, root)?;
        assert!(
            other_reader
                .open(&commit, &selection)
                .expect_err("accepted completion state for another repository")
                .is_stale()
        );
        Ok(())
    }

    fn read_state(layout: &HubLocalDirLayout) -> Result<LocalDirStateRecord, Box<dyn Error>> {
        let bytes = fs::read(layout.coordination_state_path())?;
        Ok(decode_record(&bytes)?)
    }
}
