use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::validation::ValidationError;
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec};

use super::hub_metadata::{HubTree, LocalDownloadMetadata, decode_local_download, decode_tree};
use super::local_dir_layout::HubLocalDirLayout;
use super::publication::FileSystem;
use super::rooted_fs::{RootedFileSystem, RootedRegularFile, is_unsafe_cache_path_error};
use super::sanitized_io::SanitizedIo;

const MAX_LOCAL_DOWNLOAD_METADATA_BYTES: usize = 64 * 1024;
const MAX_LOCAL_TREE_BYTES: usize = 64 * 1024 * 1024;

/// Reads upstream local-directory bookkeeping as non-validating reuse hints.
#[derive(Clone, Debug)]
pub(super) struct LocalDirHintReader {
    layout: HubLocalDirLayout,
    root: Arc<dyn RootedFileSystem>,
}

impl LocalDirHintReader {
    pub(super) fn open(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        file_system: &dyn FileSystem,
    ) -> Result<Self, LocalDirBookkeepingError> {
        let root = root.as_ref();
        let layout = HubLocalDirLayout::new(root, endpoint, spec)?;
        let authority = file_system.open_cache_authority(root)?;
        Ok(Self::from_layout(layout, authority.writer()))
    }

    pub(super) const fn from_layout(
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
    ) -> Self {
        Self { layout, root }
    }

    /// Returns fresh upstream metadata without validating destination bytes.
    pub(super) fn file_hint(
        &self,
        path: &RepoPath,
    ) -> Result<Option<LocalDirFileHint>, LocalDirBookkeepingError> {
        let metadata_path = self.layout.download_metadata_path(path)?;
        let lock_path = self.layout.lock_path(path)?;
        let metadata_relative = self.relative(&metadata_path)?.to_path_buf();
        let lock_relative = self.relative(&lock_path)?;
        let _guard = self.root.lock_exclusive(lock_relative)?;

        let metadata = match self.read_record(&metadata_relative, MAX_LOCAL_DOWNLOAD_METADATA_BYTES)
        {
            Err(source) if source.is_unsafe() => return Err(source),
            Err(source) if source.is_io() => {
                self.remove_corrupt_metadata(&metadata_relative);
                return Ok(None);
            }
            Err(source) => return Err(source),
            Ok(record) => match record {
                RecordRead::Bytes(bytes) => match decode_local_download(&bytes) {
                    Ok(metadata) => metadata,
                    Err(_corrupt) => {
                        self.remove_corrupt_metadata(&metadata_relative);
                        return Ok(None);
                    }
                },
                RecordRead::Corrupt => {
                    self.remove_corrupt_metadata(&metadata_relative);
                    return Ok(None);
                }
                RecordRead::Missing | RecordRead::NonRegular => return Ok(None),
            },
        };

        let destination = self.layout.file_path(path)?;
        let destination_relative = self.relative(&destination)?;
        match self.root.open_regular(destination_relative)? {
            RootedRegularFile::File { modified, .. }
                if upstream_metadata_is_fresh(modified, metadata.timestamp()) =>
            {
                Ok(Some(LocalDirFileHint { metadata }))
            }
            RootedRegularFile::File { .. }
            | RootedRegularFile::Missing
            | RootedRegularFile::Other => Ok(None),
        }
    }

    /// Returns a decoded upstream tree record without validating local files.
    pub(super) fn tree_hint(
        &self,
        commit: &CommitId,
    ) -> Result<Option<LocalDirTreeHint>, LocalDirBookkeepingError> {
        let path = self.layout.tree_path(commit);
        let relative = self.relative(&path)?;
        let record = match self.read_record(relative, MAX_LOCAL_TREE_BYTES) {
            Ok(record) => record,
            Err(source) if source.is_io() => return Ok(None),
            Err(source) => return Err(source),
        };
        let RecordRead::Bytes(bytes) = record else {
            return Ok(None);
        };
        Ok(decode_tree(&bytes)
            .ok()
            .map(|tree| LocalDirTreeHint { tree }))
    }

    fn read_record(
        &self,
        path: &Path,
        limit: usize,
    ) -> Result<RecordRead, LocalDirBookkeepingError> {
        let (mut reader, size) = match self.root.open_regular(path)? {
            RootedRegularFile::File { reader, size, .. } => (reader, size),
            RootedRegularFile::Missing => return Ok(RecordRead::Missing),
            RootedRegularFile::Other => return Ok(RecordRead::NonRegular),
        };
        let limit_u64 = u64::try_from(limit).map_err(io::Error::other)?;
        if size > limit_u64 {
            return Ok(RecordRead::Corrupt);
        }
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(limit_u64.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            Ok(RecordRead::Corrupt)
        } else {
            Ok(RecordRead::Bytes(bytes))
        }
    }

    fn remove_corrupt_metadata(&self, path: &Path) {
        let _cleanup_result = self.root.remove_file(path);
    }

    fn relative<'a>(&self, path: &'a Path) -> Result<&'a Path, LocalDirBookkeepingError> {
        self.layout.capability_relative(path).map_err(Into::into)
    }
}

/// Fresh upstream per-file metadata that still requires content validation.
#[derive(Clone, PartialEq)]
pub(super) struct LocalDirFileHint {
    metadata: LocalDownloadMetadata,
}

impl fmt::Debug for LocalDirFileHint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirFileHint")
            .finish_non_exhaustive()
    }
}

/// An upstream tree record that still requires per-file content validation.
#[derive(Clone, PartialEq)]
pub(super) struct LocalDirTreeHint {
    tree: HubTree,
}

impl LocalDirTreeHint {
    pub(super) const fn tree(&self) -> &HubTree {
        &self.tree
    }
}

impl fmt::Debug for LocalDirTreeHint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirTreeHint")
            .finish_non_exhaustive()
    }
}

impl LocalDirFileHint {
    pub(super) const fn commit(&self) -> &CommitId {
        self.metadata.commit()
    }

    pub(super) fn etag(&self) -> &str {
        self.metadata.etag()
    }

    pub(super) const fn timestamp(&self) -> f64 {
        self.metadata.timestamp()
    }
}

#[derive(Debug)]
enum RecordRead {
    Missing,
    NonRegular,
    Corrupt,
    Bytes(Vec<u8>),
}

fn upstream_metadata_is_fresh(modified: SystemTime, metadata_timestamp: f64) -> bool {
    unix_seconds(modified) - 1.0 <= metadata_timestamp
}

fn unix_seconds(time: SystemTime) -> f64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64(),
        Err(before_epoch) => -before_epoch.duration().as_secs_f64(),
    }
}

#[derive(Debug)]
pub(super) struct LocalDirBookkeepingError {
    kind: LocalDirBookkeepingErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum LocalDirBookkeepingErrorKind {
    Io(SanitizedIo),
    UnsafeFileSystem(SanitizedIo),
    Validation(ValidationError),
}

impl LocalDirBookkeepingError {
    fn new(kind: LocalDirBookkeepingErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) fn is_unsafe(&self) -> bool {
        matches!(self.kind, LocalDirBookkeepingErrorKind::UnsafeFileSystem(_))
            || matches!(
                &self.kind,
                LocalDirBookkeepingErrorKind::Validation(source) if source.is_unsafe_path()
            )
    }

    fn is_io(&self) -> bool {
        matches!(self.kind, LocalDirBookkeepingErrorKind::Io(_))
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    #[cfg(test)]
    fn io_kind(&self) -> Option<io::ErrorKind> {
        match self.kind {
            LocalDirBookkeepingErrorKind::Io(source)
            | LocalDirBookkeepingErrorKind::UnsafeFileSystem(source) => Some(source.kind()),
            LocalDirBookkeepingErrorKind::Validation(_) => None,
        }
    }
}

impl Display for LocalDirBookkeepingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            LocalDirBookkeepingErrorKind::Io(_) => {
                "local-dir bookkeeping filesystem operation failed"
            }
            LocalDirBookkeepingErrorKind::UnsafeFileSystem(_) => {
                "local-dir bookkeeping filesystem path is unsafe"
            }
            LocalDirBookkeepingErrorKind::Validation(_) => {
                "local-dir bookkeeping path validation failed"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for LocalDirBookkeepingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            LocalDirBookkeepingErrorKind::Validation(source) => Some(source),
            LocalDirBookkeepingErrorKind::Io(_)
            | LocalDirBookkeepingErrorKind::UnsafeFileSystem(_) => None,
        }
    }
}

impl From<io::Error> for LocalDirBookkeepingError {
    fn from(source: io::Error) -> Self {
        let unsafe_path = is_unsafe_cache_path_error(&source);
        let source = SanitizedIo::new(&source);
        let kind = if unsafe_path {
            LocalDirBookkeepingErrorKind::UnsafeFileSystem(source)
        } else {
            LocalDirBookkeepingErrorKind::Io(source)
        };
        Self::new(kind)
    }
}

impl From<ValidationError> for LocalDirBookkeepingError {
    fn from(source: ValidationError) -> Self {
        Self::new(LocalDirBookkeepingErrorKind::Validation(source))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt::{self, Debug, Display, Formatter};
    use std::fs::{self, FileTimes, OpenOptions};
    use std::io::{self, Read};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tempfile::TempDir;

    use crate::cache::hub_metadata::{
        HubTree, HubTreeEntry, LocalDownloadMetadata, encode_local_download, encode_tree,
    };
    use crate::cache::local_dir_layout::HubLocalDirLayout;
    use crate::cache::publication::{CacheAuthority, FileSystem, OsFileSystem};
    use crate::cache::rooted_fs::{
        CacheRoot, CreateOnceOutcome, RootedEntryKind, RootedFileSystem, RootedLockGuard,
        RootedRead, RootedRegularFile, RootedWrite, StagingName,
    };
    use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec};

    use super::LocalDirHintReader;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const ETAG: &str = "9e107d9d372bb6826bd81d3542a419d6";
    const SECRET_ERROR_SENTINEL: &str = "hf_secret_local_dir_bookkeeping_sentinel";
    const UNTRUSTED_ETAG_SENTINEL: &str = "hf_untrusted_etag_sentinel";
    const UNTRUSTED_PATH_SENTINEL: &str = "hf_untrusted_path_sentinel/config.json";
    const UNTRUSTED_OBJECT_SENTINEL: &str = "hf_untrusted_object_id_sentinel";
    const PINNED_COMMIT: &str = "4444444444444444444444444444444444444444";
    const PINNED_ETAG: &str = "1d3e832db20793bc16ef45d42eace92e9b3d09ef";
    const PINNED_TIMESTAMP: f64 = 1_720_000_000.25;
    const PINNED_CONTENT: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/local-dir/config/fixture.json"
    );
    const PINNED_METADATA: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/local-dir/.cache/huggingface/download/config/fixture.json.metadata"
    );
    const PINNED_TREE: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/local-dir/.cache/huggingface/trees/4444444444444444444444444444444444444444.json"
    );

    #[test]
    fn python_freshness_boundary_is_inclusive_at_exactly_one_second() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("nested/config.json")?;
        fixture.write_content(&path, b"not validated by a hint")?;
        let modified = fs::metadata(fixture.layout.file_path(&path)?)?.modified()?;
        let modified_seconds = unix_seconds(modified);

        fixture.write_metadata(&path, modified_seconds - 1.0)?;
        let exact = fixture
            .reader
            .file_hint(&path)?
            .ok_or("exact Python freshness boundary was treated as stale")?;
        assert_eq!(exact.commit().as_str(), COMMIT);
        assert_eq!(exact.etag(), ETAG);
        assert_eq!(
            exact.timestamp().to_bits(),
            (modified_seconds - 1.0).to_bits()
        );

        fixture.write_metadata(&path, modified_seconds - 1.000_001)?;
        assert!(fixture.reader.file_hint(&path)?.is_none());
        assert!(fixture.layout.download_metadata_path(&path)?.is_file());
        assert!(fixture.layout.lock_path(&path)?.is_file());

        Ok(())
    }

    #[test]
    fn missing_metadata_or_destination_and_nonregular_destination_are_not_hints()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let no_metadata = RepoPath::parse("missing-metadata.bin")?;
        fixture.write_content(&no_metadata, b"content")?;
        assert!(fixture.reader.file_hint(&no_metadata)?.is_none());

        let no_destination = RepoPath::parse("missing-destination.bin")?;
        fixture.write_metadata(&no_destination, f64::MAX)?;
        assert!(fixture.reader.file_hint(&no_destination)?.is_none());
        assert!(
            fixture
                .layout
                .download_metadata_path(&no_destination)?
                .is_file()
        );

        let directory = RepoPath::parse("directory-destination")?;
        fs::create_dir_all(fixture.layout.file_path(&directory)?)?;
        fixture.write_metadata(&directory, f64::MAX)?;
        assert!(fixture.reader.file_hint(&directory)?.is_none());
        assert!(fixture.layout.download_metadata_path(&directory)?.is_file());

        Ok(())
    }

    #[test]
    fn corrupt_metadata_is_removed_while_the_exact_upstream_lock_is_held()
    -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let (layout, inner) = layout_and_root(directory.path())?;
        let path = RepoPath::parse("nested/model.bin")?;
        let lock_path = layout.lock_path(&path)?;
        let expected_lock = layout.capability_relative(&lock_path)?;
        let metadata_relative = layout
            .capability_relative(&layout.download_metadata_path(&path)?)?
            .to_path_buf();
        let probe = Arc::new(LockProbeRoot::new(
            inner,
            expected_lock.to_path_buf(),
            vec![metadata_relative],
        ));
        let reader = LocalDirHintReader::from_layout(
            layout.clone(),
            Arc::clone(&probe) as Arc<dyn RootedFileSystem>,
        );
        let content_path = layout.file_path(&path)?;
        create_parent(&content_path)?;
        fs::write(&content_path, b"user-owned content")?;
        let metadata_path = layout.download_metadata_path(&path)?;
        create_parent(&metadata_path)?;
        fs::write(&metadata_path, b"not valid metadata\n")?;

        assert!(reader.file_hint(&path)?.is_none());
        assert!(!metadata_path.try_exists()?);
        assert_eq!(fs::read(content_path)?, b"user-owned content");
        assert_eq!(probe.lock_path()?, Some(expected_lock.to_path_buf()));
        assert_eq!(probe.removals_while_locked(), 1);
        assert!(!probe.lock_active());

        Ok(())
    }

    #[test]
    fn corrupt_metadata_cleanup_failure_is_best_effort() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let (layout, inner) = layout_and_root(directory.path())?;
        let path = RepoPath::parse("config.json")?;
        let lock_path = layout.lock_path(&path)?;
        let lock_relative = layout.capability_relative(&lock_path)?.to_path_buf();
        let metadata_path = layout.download_metadata_path(&path)?;
        let metadata_relative = layout.capability_relative(&metadata_path)?.to_path_buf();
        let probe = Arc::new(
            LockProbeRoot::new(inner, lock_relative, vec![metadata_relative])
                .with_removal_failure(),
        );
        let reader = LocalDirHintReader::from_layout(
            layout,
            Arc::clone(&probe) as Arc<dyn RootedFileSystem>,
        );
        create_parent(&metadata_path)?;
        fs::write(&metadata_path, b"corrupt\n")?;

        assert!(reader.file_hint(&path)?.is_none());
        assert!(metadata_path.is_file());
        assert_eq!(probe.removals_while_locked(), 1);
        assert!(!probe.lock_active());

        Ok(())
    }

    #[test]
    fn a_fresh_hint_never_reads_content_or_publishes_completion() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let (layout, inner) = layout_and_root(directory.path())?;
        let path = RepoPath::parse("weights/model.safetensors")?;
        let content_path = layout.file_path(&path)?;
        create_parent(&content_path)?;
        fs::write(&content_path, b"bytes requiring later validation")?;
        let modified = fs::metadata(&content_path)?.modified()?;
        write_metadata(&layout, &path, unix_seconds(modified))?;
        let content_relative = layout.capability_relative(&content_path)?.to_path_buf();
        let metadata_relative = layout
            .capability_relative(&layout.download_metadata_path(&path)?)?
            .to_path_buf();
        let lock_relative = layout
            .capability_relative(&layout.lock_path(&path)?)?
            .to_path_buf();
        let lock_probe = Arc::new(LockProbeRoot::new(
            inner,
            lock_relative.clone(),
            vec![metadata_relative, content_relative.clone()],
        ));
        let reads = Arc::new(AtomicUsize::new(0));
        let root = Arc::new(NoContentReadRoot {
            inner: Arc::clone(&lock_probe) as Arc<dyn RootedFileSystem>,
            content_relative,
            reads: Arc::clone(&reads),
        });
        let reader = LocalDirHintReader::from_layout(layout.clone(), root);

        assert!(reader.file_hint(&path)?.is_some());
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(lock_probe.lock_path()?, Some(lock_relative));
        assert_eq!(lock_probe.opens_while_locked(), 2);
        assert!(!layout.completion_sidecar().cache_root().try_exists()?);

        Ok(())
    }

    #[test]
    fn tree_records_are_optional_nonvalidating_hints() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let commit = CommitId::parse(COMMIT)?;
        assert!(fixture.reader.tree_hint(&commit)?.is_none());

        let tree_path = fixture.layout.tree_path(&commit);
        create_parent(&tree_path)?;
        fs::write(&tree_path, br#"{"format_version":1,"files":[]}"#)?;
        assert!(fixture.reader.tree_hint(&commit)?.is_none());
        assert!(tree_path.is_file());

        fs::write(&tree_path, br#"{"format_version":2,"future":true}"#)?;
        assert!(fixture.reader.tree_hint(&commit)?.is_none());
        assert!(tree_path.is_file());

        let repo_path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(
            repo_path.clone(),
            HubTreeEntry::new(7, "8a0f4347f7db2a96cf666a4df22355cdd1ec215d")?,
        )])?;
        fs::write(&tree_path, encode_tree(&tree)?)?;
        let hint = fixture
            .reader
            .tree_hint(&commit)?
            .ok_or("valid tree was not returned as a hint")?;
        assert_eq!(
            hint.tree().files().get(&repo_path).map(HubTreeEntry::size),
            Some(7)
        );
        assert!(
            !fixture
                .layout
                .completion_sidecar()
                .cache_root()
                .try_exists()?
        );

        Ok(())
    }

    #[test]
    fn optional_bookkeeping_read_errors_are_treated_as_misses() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let (layout, inner) = layout_and_root(directory.path())?;
        let path = RepoPath::parse("config.json")?;
        let content_path = layout.file_path(&path)?;
        create_parent(&content_path)?;
        fs::write(&content_path, b"unvalidated")?;
        let modified = fs::metadata(&content_path)?.modified()?;
        write_metadata(&layout, &path, unix_seconds(modified))?;

        let commit = CommitId::parse(COMMIT)?;
        let tree = HubTree::new([(
            path.clone(),
            HubTreeEntry::new(11, "8a0f4347f7db2a96cf666a4df22355cdd1ec215d")?,
        )])?;
        let tree_path = layout.tree_path(&commit);
        create_parent(&tree_path)?;
        fs::write(&tree_path, encode_tree(&tree)?)?;

        let lock_path = layout.lock_path(&path)?;
        let lock_relative = layout.capability_relative(&lock_path)?.to_path_buf();
        let metadata_path = layout.download_metadata_path(&path)?;
        let metadata_relative = layout.capability_relative(&metadata_path)?.to_path_buf();
        let tree_relative = layout.capability_relative(&tree_path)?.to_path_buf();
        let probe = Arc::new(
            LockProbeRoot::new(inner, lock_relative, vec![metadata_relative.clone()])
                .with_open_failures(vec![metadata_relative, tree_relative]),
        );
        let reader = LocalDirHintReader::from_layout(
            layout,
            Arc::clone(&probe) as Arc<dyn RootedFileSystem>,
        );

        assert!(reader.file_hint(&path)?.is_none());
        assert!(!metadata_path.try_exists()?);
        assert!(reader.tree_hint(&commit)?.is_none());
        assert!(tree_path.is_file());
        assert!(!probe.lock_active());

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlink_ancestors_and_destinations_are_rejected_without_following()
    -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::symlink;

        let ancestor_fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        fs::create_dir_all(ancestor_fixture.directory.path().join(".cache"))?;
        symlink(
            outside.path(),
            ancestor_fixture.directory.path().join(".cache/huggingface"),
        )?;
        let path = RepoPath::parse("config.json")?;
        let ancestor_error = ancestor_fixture
            .reader
            .file_hint(&path)
            .expect_err("followed a linked bookkeeping ancestor");
        assert!(ancestor_error.is_unsafe());
        assert!(
            !outside
                .path()
                .join("download/config.json.lock")
                .try_exists()?
        );

        let destination_fixture = Fixture::new()?;
        let external_file = outside.path().join("external.bin");
        fs::write(&external_file, b"external")?;
        let destination = destination_fixture.layout.file_path(&path)?;
        symlink(&external_file, &destination)?;
        destination_fixture.write_metadata(&path, f64::MAX)?;
        let destination_error = destination_fixture
            .reader
            .file_hint(&path)
            .expect_err("followed a linked local-dir destination");
        assert!(destination_error.is_unsafe());
        assert_eq!(fs::read(external_file)?, b"external");

        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn reparse_point_ancestor_is_rejected_without_writing_through_it() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        fs::create_dir_all(fixture.directory.path().join(".cache"))?;
        create_dir_junction(
            outside.path(),
            &fixture.directory.path().join(".cache").join("huggingface"),
        )?;
        let path = RepoPath::parse("config.json")?;

        let error = fixture
            .reader
            .file_hint(&path)
            .expect_err("followed a reparse-point bookkeeping ancestor");
        assert!(error.is_unsafe());
        assert!(
            !outside
                .path()
                .join("download/config.json.lock")
                .try_exists()?
        );

        Ok(())
    }

    #[test]
    fn filesystem_errors_are_redacted_from_every_representation() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let error =
            LocalDirHintReader::open(directory.path(), &endpoint, &spec, &FailingFileSystem)
                .expect_err("accepted a failing filesystem authority");

        assert_secret_absent(&error);
        assert_eq!(error.io_kind(), Some(io::ErrorKind::PermissionDenied));

        Ok(())
    }

    #[test]
    fn successful_hint_debug_omits_untrusted_metadata_and_tree_values() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse(UNTRUSTED_PATH_SENTINEL)?;
        fixture.write_content(&path, b"unvalidated bytes")?;
        let modified = fs::metadata(fixture.layout.file_path(&path)?)?.modified()?;
        write_metadata_values(
            &fixture.layout,
            &path,
            COMMIT,
            UNTRUSTED_ETAG_SENTINEL,
            unix_seconds(modified),
        )?;
        let file_hint = fixture
            .reader
            .file_hint(&path)?
            .ok_or("fresh file metadata was not returned as a hint")?;

        let commit = CommitId::parse(COMMIT)?;
        let tree = HubTree::new([(
            path.clone(),
            HubTreeEntry::new(17, UNTRUSTED_OBJECT_SENTINEL)?,
        )])?;
        let tree_path = fixture.layout.tree_path(&commit);
        create_parent(&tree_path)?;
        fs::write(tree_path, encode_tree(&tree)?)?;
        let tree_hint = fixture
            .reader
            .tree_hint(&commit)?
            .ok_or("valid tree metadata was not returned as a hint")?;

        assert_redacted_hint_debug(
            &format!("{file_hint:?}"),
            "LocalDirFileHint",
            &[
                UNTRUSTED_ETAG_SENTINEL,
                UNTRUSTED_PATH_SENTINEL,
                UNTRUSTED_OBJECT_SENTINEL,
            ],
        );
        assert_redacted_hint_debug(
            &format!("{tree_hint:?}"),
            "LocalDirTreeHint",
            &[
                UNTRUSTED_ETAG_SENTINEL,
                UNTRUSTED_PATH_SENTINEL,
                UNTRUSTED_OBJECT_SENTINEL,
            ],
        );
        assert!(tree_hint.tree().files().contains_key(&path));

        Ok(())
    }

    #[test]
    fn reads_checked_in_pinned_python_local_dir_bookkeeping() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config/fixture.json")?;
        let destination = fixture.layout.file_path(&path)?;
        create_parent(&destination)?;
        fs::write(&destination, PINNED_CONTENT)?;
        let modified = UNIX_EPOCH + Duration::from_secs(1_720_000_001) + Duration::from_millis(250);
        OpenOptions::new()
            .write(true)
            .open(&destination)?
            .set_times(FileTimes::new().set_modified(modified))?;
        assert!(unix_seconds(fs::metadata(&destination)?.modified()?) - 1.0 <= PINNED_TIMESTAMP);

        let metadata_path = fixture.layout.download_metadata_path(&path)?;
        create_parent(&metadata_path)?;
        fs::write(metadata_path, PINNED_METADATA)?;
        let commit = CommitId::parse(PINNED_COMMIT)?;
        let tree_path = fixture.layout.tree_path(&commit);
        create_parent(&tree_path)?;
        fs::write(tree_path, PINNED_TREE)?;

        let file_hint = fixture
            .reader
            .file_hint(&path)?
            .ok_or("pinned Python file metadata was not fresh")?;
        assert_eq!(file_hint.commit(), &commit);
        assert_eq!(file_hint.etag(), PINNED_ETAG);
        assert_eq!(file_hint.timestamp().to_bits(), PINNED_TIMESTAMP.to_bits());
        let tree_hint = fixture
            .reader
            .tree_hint(&commit)?
            .ok_or("pinned Python tree metadata was not readable")?;
        assert_eq!(
            tree_hint.tree().files().get(&path).map(HubTreeEntry::size),
            Some(u64::try_from(PINNED_CONTENT.len())?)
        );

        Ok(())
    }

    struct Fixture {
        directory: TempDir,
        layout: HubLocalDirLayout,
        reader: LocalDirHintReader,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &spec)?;
            let reader =
                LocalDirHintReader::open(directory.path(), &endpoint, &spec, &OsFileSystem)?;
            Ok(Self {
                directory,
                layout,
                reader,
            })
        }

        fn write_content(&self, path: &RepoPath, bytes: &[u8]) -> io::Result<()> {
            let destination = self.layout.file_path(path).map_err(io::Error::other)?;
            create_parent(&destination)?;
            fs::write(destination, bytes)
        }

        fn write_metadata(&self, path: &RepoPath, timestamp: f64) -> Result<(), Box<dyn Error>> {
            write_metadata(&self.layout, path, timestamp)
        }
    }

    fn layout_and_root(
        root: &Path,
    ) -> Result<(HubLocalDirLayout, Arc<dyn RootedFileSystem>), Box<dyn Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(root, &endpoint, &spec)?;
        let rooted: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(root)?);
        Ok((layout, rooted))
    }

    fn write_metadata(
        layout: &HubLocalDirLayout,
        path: &RepoPath,
        timestamp: f64,
    ) -> Result<(), Box<dyn Error>> {
        write_metadata_values(layout, path, COMMIT, ETAG, timestamp)
    }

    fn write_metadata_values(
        layout: &HubLocalDirLayout,
        path: &RepoPath,
        commit: &str,
        etag: &str,
        timestamp: f64,
    ) -> Result<(), Box<dyn Error>> {
        let metadata = LocalDownloadMetadata::new(CommitId::parse(commit)?, etag, timestamp)?;
        let destination = layout.download_metadata_path(path)?;
        create_parent(&destination)?;
        fs::write(destination, encode_local_download(&metadata))?;
        Ok(())
    }

    fn assert_redacted_hint_debug(rendered: &str, type_name: &str, sentinels: &[&str]) {
        assert!(rendered.contains(type_name));
        for sentinel in sentinels {
            assert!(!rendered.contains(sentinel));
        }
    }

    fn create_parent(path: &Path) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("test path has no parent"))?;
        fs::create_dir_all(parent)
    }

    fn unix_seconds(time: SystemTime) -> f64 {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs_f64(),
            Err(before_epoch) => -before_epoch.duration().as_secs_f64(),
        }
    }

    #[derive(Debug)]
    struct LockProbeRoot {
        inner: Arc<dyn RootedFileSystem>,
        expected_lock: PathBuf,
        guarded_paths: Vec<PathBuf>,
        lock_path: Mutex<Option<PathBuf>>,
        lock_active: Arc<AtomicBool>,
        opens_while_locked: AtomicUsize,
        removals_while_locked: AtomicUsize,
        fail_removal: bool,
        open_failure_paths: Vec<PathBuf>,
    }

    impl LockProbeRoot {
        fn new(
            inner: Arc<dyn RootedFileSystem>,
            expected_lock: PathBuf,
            guarded_paths: Vec<PathBuf>,
        ) -> Self {
            Self {
                inner,
                expected_lock,
                guarded_paths,
                lock_path: Mutex::new(None),
                lock_active: Arc::new(AtomicBool::new(false)),
                opens_while_locked: AtomicUsize::new(0),
                removals_while_locked: AtomicUsize::new(0),
                fail_removal: false,
                open_failure_paths: Vec::new(),
            }
        }

        fn with_removal_failure(mut self) -> Self {
            self.fail_removal = true;
            self
        }

        fn with_open_failures(mut self, paths: Vec<PathBuf>) -> Self {
            self.open_failure_paths = paths;
            self
        }

        fn lock_path(&self) -> io::Result<Option<PathBuf>> {
            self.lock_path
                .lock()
                .map(|path| path.clone())
                .map_err(|_poisoned| io::Error::other("lock probe mutex poisoned"))
        }

        fn removals_while_locked(&self) -> usize {
            self.removals_while_locked.load(Ordering::Acquire)
        }

        fn opens_while_locked(&self) -> usize {
            self.opens_while_locked.load(Ordering::Acquire)
        }

        fn lock_active(&self) -> bool {
            self.lock_active.load(Ordering::Acquire)
        }
    }

    impl RootedFileSystem for LockProbeRoot {
        fn ensure_dir(&self, path: &Path) -> io::Result<()> {
            self.inner.ensure_dir(path)
        }

        fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
            self.inner.entry_kind(path)
        }

        fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
            if self.guarded_paths.iter().any(|guarded| guarded == path) {
                if !self.lock_active() {
                    return Err(io::Error::other(
                        "local-dir bookkeeping path was opened outside its lock",
                    ));
                }
                self.opens_while_locked.fetch_add(1, Ordering::AcqRel);
            }
            if self.open_failure_paths.iter().any(|failed| failed == path) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected optional bookkeeping read failure",
                ));
            }
            self.inner.open_regular(path)
        }

        fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead> {
            self.inner.read_regular_bounded(path, limit)
        }

        fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>> {
            self.inner.create_new(path)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            if self.lock_active() {
                self.removals_while_locked.fetch_add(1, Ordering::AcqRel);
            }
            if self.fail_removal {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected metadata cleanup failure",
                ))
            } else {
                self.inner.remove_file(path)
            }
        }

        fn install_staged_create_once(
            &self,
            staging: &Path,
            destination: &Path,
        ) -> io::Result<CreateOnceOutcome> {
            self.inner.install_staged_create_once(staging, destination)
        }

        fn replace(&self, path: &Path, bytes: &[u8], staging: &StagingName) -> io::Result<()> {
            self.inner.replace(path, bytes, staging)
        }

        fn replace_from_staging(
            &self,
            path: &Path,
            bytes: &[u8],
            staging_path: &Path,
        ) -> io::Result<()> {
            self.inner.replace_from_staging(path, bytes, staging_path)
        }

        fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn RootedLockGuard>> {
            if path != self.expected_lock {
                return Err(io::Error::other(
                    "reader used the wrong local-dir lock path",
                ));
            }
            let guard = self.inner.lock_exclusive(path)?;
            *self
                .lock_path
                .lock()
                .map_err(|_poisoned| io::Error::other("lock probe mutex poisoned"))? =
                Some(path.to_path_buf());
            self.lock_active.store(true, Ordering::Release);
            Ok(Box::new(LockProbeGuard {
                _inner: guard,
                active: Arc::clone(&self.lock_active),
            }))
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.inner.sync_directory(path)
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
    }

    #[derive(Debug)]
    struct LockProbeGuard {
        _inner: Box<dyn RootedLockGuard>,
        active: Arc<AtomicBool>,
    }

    impl RootedLockGuard for LockProbeGuard {}

    impl Drop for LockProbeGuard {
        fn drop(&mut self) {
            self.active.store(false, Ordering::Release);
        }
    }

    #[derive(Debug)]
    struct NoContentReadRoot {
        inner: Arc<dyn RootedFileSystem>,
        content_relative: PathBuf,
        reads: Arc<AtomicUsize>,
    }

    impl RootedFileSystem for NoContentReadRoot {
        fn ensure_dir(&self, path: &Path) -> io::Result<()> {
            self.inner.ensure_dir(path)
        }

        fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
            self.inner.entry_kind(path)
        }

        fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
            match self.inner.open_regular(path)? {
                RootedRegularFile::File {
                    reader,
                    size,
                    modified,
                } if path == self.content_relative => Ok(RootedRegularFile::File {
                    reader: Box::new(ReadProbe {
                        inner: reader,
                        reads: Arc::clone(&self.reads),
                    }),
                    size,
                    modified,
                }),
                other => Ok(other),
            }
        }

        fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead> {
            self.inner.read_regular_bounded(path, limit)
        }

        fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>> {
            self.inner.create_new(path)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }

        fn install_staged_create_once(
            &self,
            staging: &Path,
            destination: &Path,
        ) -> io::Result<CreateOnceOutcome> {
            self.inner.install_staged_create_once(staging, destination)
        }

        fn replace(&self, path: &Path, bytes: &[u8], staging: &StagingName) -> io::Result<()> {
            self.inner.replace(path, bytes, staging)
        }

        fn replace_from_staging(
            &self,
            path: &Path,
            bytes: &[u8],
            staging_path: &Path,
        ) -> io::Result<()> {
            self.inner.replace_from_staging(path, bytes, staging_path)
        }

        fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn RootedLockGuard>> {
            self.inner.lock_exclusive(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.inner.sync_directory(path)
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
    }

    struct ReadProbe {
        inner: Box<dyn Read + Send>,
        reads: Arc<AtomicUsize>,
    }

    impl Debug for ReadProbe {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.debug_struct("ReadProbe").finish_non_exhaustive()
        }
    }

    impl Read for ReadProbe {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reads.fetch_add(1, Ordering::AcqRel);
            self.inner.read(buffer)
        }
    }

    #[derive(Debug)]
    struct FailingFileSystem;

    impl FileSystem for FailingFileSystem {
        fn open_cache_authority(&self, _path: &Path) -> io::Result<CacheAuthority> {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, SecretError))
        }
    }

    #[derive(Debug)]
    struct SecretError;

    impl Display for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_ERROR_SENTINEL)
        }
    }

    impl Error for SecretError {}

    fn assert_secret_absent(error: &(dyn Error + 'static)) {
        let mut current = Some(error);
        while let Some(source) = current {
            assert!(!source.to_string().contains(SECRET_ERROR_SENTINEL));
            assert!(!format!("{source:?}").contains(SECRET_ERROR_SENTINEL));
            current = source.source();
        }
    }

    #[cfg(windows)]
    fn create_dir_junction(target: &Path, link: &Path) -> io::Result<()> {
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
}
