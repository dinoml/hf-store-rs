//! One-file independent-copy materialization for user-owned local directories.

use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::RepoPath;
use crate::validation::ValidationError;

use super::hub_cache::{HubCacheReadError, copy_and_validate_content};
use super::hub_metadata::HubTreeEntry;
use super::key::{BlobDigest, portable_path_key};
use super::local_dir_layout::HubLocalDirLayout;
use super::publication::{Effects, PublicationPoint};
use super::rooted_fs::{
    CreateOnceOutcome, RootedEntryKind, RootedFileSystem, RootedRegularFile,
    is_unsafe_cache_path_error, staging_path,
};
use super::sanitized_io::SanitizedIo;

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirFileTarget {
    path: RepoPath,
    entry: HubTreeEntry,
    digest: BlobDigest,
}

impl LocalDirFileTarget {
    pub(super) const fn new(path: RepoPath, entry: HubTreeEntry, digest: BlobDigest) -> Self {
        Self {
            path,
            entry,
            digest,
        }
    }

    pub(super) const fn path(&self) -> &RepoPath {
        &self.path
    }

    pub(super) const fn entry(&self) -> &HubTreeEntry {
        &self.entry
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }
}

impl Debug for LocalDirFileTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirFileTarget")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExistingFilePolicy {
    Reject,
    ReplaceRegularFile,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LocalDirFileDisposition {
    Reused,
    Copied,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirFileMaterialization {
    path: PathBuf,
    size: u64,
    digest: BlobDigest,
    disposition: LocalDirFileDisposition,
}

impl LocalDirFileMaterialization {
    fn new(
        path: PathBuf,
        target: &LocalDirFileTarget,
        disposition: LocalDirFileDisposition,
    ) -> Self {
        Self {
            path,
            size: target.entry.size(),
            digest: target.digest,
            disposition,
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }

    pub(super) const fn disposition(&self) -> LocalDirFileDisposition {
        self.disposition
    }
}

impl Debug for LocalDirFileMaterialization {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirFileMaterialization")
            .field("size", &self.size)
            .field("disposition", &self.disposition)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(super) struct LocalDirFileMaterializer {
    layout: HubLocalDirLayout,
    root: Arc<dyn RootedFileSystem>,
    effects: Effects,
}

impl LocalDirFileMaterializer {
    pub(super) const fn from_layout(
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    ) -> Self {
        Self {
            layout,
            root,
            effects,
        }
    }

    pub(super) fn materialize(
        &self,
        target: &LocalDirFileTarget,
        source: &mut dyn Read,
        policy: ExistingFilePolicy,
    ) -> Result<LocalDirFileMaterialization, LocalDirMaterializationError> {
        let destination = self.layout.file_path(target.path())?;
        let destination_relative = self.layout.capability_relative(&destination)?.to_path_buf();
        let initial = self.inspect_destination(&destination_relative, target)?;
        match initial {
            DestinationState::Exact => {
                return Ok(LocalDirFileMaterialization::new(
                    destination,
                    target,
                    LocalDirFileDisposition::Reused,
                ));
            }
            DestinationState::DifferentRegular if policy == ExistingFilePolicy::Reject => {
                return Err(LocalDirMaterializationError::conflict(false));
            }
            DestinationState::Missing | DestinationState::DifferentRegular => {}
        }

        let staging_name = self.effects.next_staging_name()?;
        let paths = MaterializationPaths {
            destination,
            staging: staging_path(&destination_relative, &staging_name)?,
            relative: destination_relative,
        };
        paths.ensure_distinct_staging(target)?;
        let mut cleanup = StagingCleanup::new(self.root.as_ref(), paths.staging.clone());
        let result = self.stage_and_install(target, source, policy, &paths, &mut cleanup);
        cleanup.finish(result)
    }

    fn stage_and_install(
        &self,
        target: &LocalDirFileTarget,
        source: &mut dyn Read,
        policy: ExistingFilePolicy,
        paths: &MaterializationPaths,
        cleanup: &mut StagingCleanup<'_>,
    ) -> Result<LocalDirFileMaterialization, LocalDirMaterializationError> {
        let mut writer = self.root.create_new(&paths.staging)?;
        cleanup.activate();
        self.check_fault(PublicationPoint::AfterStagingCreate, false)?;
        let (_size, actual_digest) =
            copy_and_validate_content(source, writer.as_mut(), target.entry())
                .map_err(LocalDirMaterializationError::content)?;
        if actual_digest != target.digest() {
            return Err(LocalDirMaterializationError::digest_mismatch(false));
        }
        writer.flush()?;
        writer.sync_all()?;
        drop(writer);
        self.check_fault(PublicationPoint::AfterStagingSync, false)?;

        match self.inspect_destination(&paths.relative, target)? {
            DestinationState::Exact => Ok(LocalDirFileMaterialization::new(
                paths.destination.clone(),
                target,
                LocalDirFileDisposition::Reused,
            )),
            DestinationState::Missing => self.install_missing(target, policy, paths, cleanup),
            DestinationState::DifferentRegular => {
                if policy == ExistingFilePolicy::Reject {
                    Err(LocalDirMaterializationError::conflict(false))
                } else {
                    self.install_replacement(target, paths, cleanup)
                }
            }
        }
    }

    fn install_missing(
        &self,
        target: &LocalDirFileTarget,
        policy: ExistingFilePolicy,
        paths: &MaterializationPaths,
        cleanup: &mut StagingCleanup<'_>,
    ) -> Result<LocalDirFileMaterialization, LocalDirMaterializationError> {
        self.check_fault(PublicationPoint::BeforeAtomicReplace, false)?;
        cleanup.mark_publication_attempted();
        let outcome = self
            .root
            .install_staged_create_once(&paths.staging, &paths.relative)
            .map_err(|source| LocalDirMaterializationError::io(&source, true))?;
        match outcome {
            CreateOnceOutcome::Created => self.finish_install(
                target,
                &paths.destination,
                &paths.relative,
                LocalDirFileDisposition::Copied,
            ),
            CreateOnceOutcome::Existing => {
                match self.inspect_destination(&paths.relative, target)? {
                    DestinationState::Exact => Ok(LocalDirFileMaterialization::new(
                        paths.destination.clone(),
                        target,
                        LocalDirFileDisposition::Reused,
                    )),
                    DestinationState::DifferentRegular
                        if policy == ExistingFilePolicy::ReplaceRegularFile =>
                    {
                        self.install_replacement(target, paths, cleanup)
                    }
                    DestinationState::Missing | DestinationState::DifferentRegular => {
                        Err(LocalDirMaterializationError::conflict(true))
                    }
                }
            }
        }
    }

    fn install_replacement(
        &self,
        target: &LocalDirFileTarget,
        paths: &MaterializationPaths,
        cleanup: &mut StagingCleanup<'_>,
    ) -> Result<LocalDirFileMaterialization, LocalDirMaterializationError> {
        self.check_fault(
            PublicationPoint::BeforeAtomicReplace,
            cleanup.publication_attempted(),
        )?;
        cleanup.mark_publication_attempted();
        self.root
            .install_staged_replace(&paths.staging, &paths.relative)
            .map_err(|source| LocalDirMaterializationError::io(&source, true))?;
        cleanup.deactivate();
        self.finish_install(
            target,
            &paths.destination,
            &paths.relative,
            LocalDirFileDisposition::Copied,
        )
    }

    fn finish_install(
        &self,
        target: &LocalDirFileTarget,
        destination: &Path,
        destination_relative: &Path,
        disposition: LocalDirFileDisposition,
    ) -> Result<LocalDirFileMaterialization, LocalDirMaterializationError> {
        self.check_fault(PublicationPoint::AfterAtomicReplace, true)?;
        match self.inspect_destination(destination_relative, target) {
            Ok(DestinationState::Exact) => {}
            Ok(DestinationState::Missing | DestinationState::DifferentRegular) => {
                return Err(LocalDirMaterializationError::final_validation());
            }
            Err(source) => return Err(source.with_may_have_published()),
        }
        self.sync_parent(destination_relative)
            .map_err(|source| LocalDirMaterializationError::io(&source, true))?;
        Ok(LocalDirFileMaterialization::new(
            destination.to_path_buf(),
            target,
            disposition,
        ))
    }

    fn inspect_destination(
        &self,
        destination: &Path,
        target: &LocalDirFileTarget,
    ) -> Result<DestinationState, LocalDirMaterializationError> {
        self.validate_parent(destination)?;
        let opened = self.root.open_regular(destination).map_err(|source| {
            if is_unsafe_cache_path_error(&source) {
                LocalDirMaterializationError::conflict(false)
            } else {
                LocalDirMaterializationError::io(&source, false)
            }
        })?;
        let (mut reader, size) = match opened {
            RootedRegularFile::File { reader, size, .. } => (reader, size),
            RootedRegularFile::Missing => return Ok(DestinationState::Missing),
            RootedRegularFile::Other => {
                return Err(LocalDirMaterializationError::conflict(false));
            }
        };
        if size != target.entry().size() {
            return Ok(DestinationState::DifferentRegular);
        }
        match copy_and_validate_content(reader.as_mut(), &mut io::sink(), target.entry()) {
            Ok((_size, digest)) if digest == target.digest() => Ok(DestinationState::Exact),
            Ok(_) => Ok(DestinationState::DifferentRegular),
            Err(source) if source.is_corrupt() => Ok(DestinationState::DifferentRegular),
            Err(source) => Err(LocalDirMaterializationError::content(source)),
        }
    }

    fn validate_parent(&self, destination: &Path) -> Result<(), LocalDirMaterializationError> {
        let parent = destination.parent().ok_or_else(|| {
            LocalDirMaterializationError::io(
                &io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "local-dir destination has no parent",
                ),
                false,
            )
        })?;
        match self.root.entry_kind(parent) {
            Ok(RootedEntryKind::Missing | RootedEntryKind::Directory) => Ok(()),
            Ok(RootedEntryKind::RegularFile | RootedEntryKind::Other) => {
                Err(LocalDirMaterializationError::conflict(false))
            }
            Err(source) if is_unsafe_cache_path_error(&source) => {
                Err(LocalDirMaterializationError::io(&source, false))
            }
            Err(source) if source.kind() == io::ErrorKind::InvalidData => {
                Err(LocalDirMaterializationError::conflict(false))
            }
            Err(source) => Err(LocalDirMaterializationError::io(&source, false)),
        }
    }

    fn sync_parent(&self, destination: &Path) -> io::Result<()> {
        let parent = destination
            .parent()
            .ok_or_else(|| io::Error::other("local-dir destination has no parent"))?;
        self.root.sync_directory(parent)
    }

    fn check_fault(
        &self,
        point: PublicationPoint,
        may_have_published: bool,
    ) -> Result<(), LocalDirMaterializationError> {
        self.effects
            .check_publication_fault(point)
            .map_err(|source| LocalDirMaterializationError::io(&source, may_have_published))
    }
}

impl Debug for LocalDirFileMaterializer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirFileMaterializer")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DestinationState {
    Missing,
    Exact,
    DifferentRegular,
}

#[derive(Debug)]
struct MaterializationPaths {
    destination: PathBuf,
    relative: PathBuf,
    staging: PathBuf,
}

impl MaterializationPaths {
    fn ensure_distinct_staging(
        &self,
        target: &LocalDirFileTarget,
    ) -> Result<(), LocalDirMaterializationError> {
        let destination_name = target
            .path()
            .as_str()
            .rsplit('/')
            .next()
            .ok_or_else(|| LocalDirMaterializationError::conflict(false))?;
        let staging_name = self
            .staging
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LocalDirMaterializationError::conflict(false))?;
        if portable_path_key(destination_name) == portable_path_key(staging_name) {
            Err(LocalDirMaterializationError::conflict(false))
        } else {
            Ok(())
        }
    }
}

struct StagingCleanup<'a> {
    root: &'a dyn RootedFileSystem,
    path: PathBuf,
    active: bool,
    publication_attempted: bool,
}

impl<'a> StagingCleanup<'a> {
    fn new(root: &'a dyn RootedFileSystem, path: PathBuf) -> Self {
        Self {
            root,
            path,
            active: false,
            publication_attempted: false,
        }
    }

    const fn activate(&mut self) {
        self.active = true;
    }

    const fn deactivate(&mut self) {
        self.active = false;
    }

    const fn mark_publication_attempted(&mut self) {
        self.publication_attempted = true;
    }

    const fn publication_attempted(&self) -> bool {
        self.publication_attempted
    }

    fn finish<T>(
        mut self,
        result: Result<T, LocalDirMaterializationError>,
    ) -> Result<T, LocalDirMaterializationError> {
        let cleanup = if self.active {
            self.root.remove_file(&self.path)
        } else {
            Ok(())
        };
        self.active = false;
        match result {
            Err(source) if self.publication_attempted => Err(source.with_may_have_published()),
            Err(source) => Err(source),
            Ok(value) => match cleanup {
                Ok(()) => Ok(value),
                Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(value),
                Err(source) => Err(LocalDirMaterializationError::io(
                    &source,
                    self.publication_attempted,
                )),
            },
        }
    }
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        if self.active {
            let _cleanup_result = self.root.remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub(super) struct LocalDirMaterializationError {
    kind: Box<LocalDirMaterializationErrorKind>,
    may_have_published: bool,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum LocalDirMaterializationErrorKind {
    Io(SanitizedIo),
    UnsafeFileSystem(SanitizedIo),
    Validation(ValidationError),
    Content(HubCacheReadError),
    DigestMismatch,
    Conflict,
    FinalValidation,
}

impl LocalDirMaterializationError {
    fn new(kind: LocalDirMaterializationErrorKind, may_have_published: bool) -> Self {
        Self {
            kind: Box::new(kind),
            may_have_published,
            backtrace: Backtrace::capture(),
        }
    }

    fn io(source: &io::Error, may_have_published: bool) -> Self {
        let unsafe_path = is_unsafe_cache_path_error(source);
        let source = SanitizedIo::new(source);
        let kind = if unsafe_path {
            LocalDirMaterializationErrorKind::UnsafeFileSystem(source)
        } else {
            LocalDirMaterializationErrorKind::Io(source)
        };
        Self::new(kind, may_have_published)
    }

    fn content(source: HubCacheReadError) -> Self {
        Self::new(LocalDirMaterializationErrorKind::Content(source), false)
    }

    fn digest_mismatch(may_have_published: bool) -> Self {
        Self::new(
            LocalDirMaterializationErrorKind::DigestMismatch,
            may_have_published,
        )
    }

    fn conflict(may_have_published: bool) -> Self {
        Self::new(
            LocalDirMaterializationErrorKind::Conflict,
            may_have_published,
        )
    }

    fn final_validation() -> Self {
        Self::new(LocalDirMaterializationErrorKind::FinalValidation, true)
    }

    fn with_may_have_published(mut self) -> Self {
        self.may_have_published = true;
        self
    }

    pub(super) fn is_conflict(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirMaterializationErrorKind::Conflict
        )
    }

    pub(super) fn is_source_mismatch(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirMaterializationErrorKind::Content(source) if source.is_corrupt()
        )
    }

    pub(super) fn is_digest_mismatch(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirMaterializationErrorKind::DigestMismatch
        )
    }

    pub(super) fn is_unsafe(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirMaterializationErrorKind::UnsafeFileSystem(_)
        ) || matches!(
            self.kind.as_ref(),
            LocalDirMaterializationErrorKind::Validation(source) if source.is_unsafe_path()
        )
    }

    pub(super) const fn may_have_published(&self) -> bool {
        self.may_have_published
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    #[cfg(test)]
    fn io_kind(&self) -> Option<io::ErrorKind> {
        match self.kind.as_ref() {
            LocalDirMaterializationErrorKind::Io(source)
            | LocalDirMaterializationErrorKind::UnsafeFileSystem(source) => Some(source.kind()),
            LocalDirMaterializationErrorKind::Validation(_)
            | LocalDirMaterializationErrorKind::Content(_)
            | LocalDirMaterializationErrorKind::DigestMismatch
            | LocalDirMaterializationErrorKind::Conflict
            | LocalDirMaterializationErrorKind::FinalValidation => None,
        }
    }
}

impl Display for LocalDirMaterializationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind.as_ref() {
            LocalDirMaterializationErrorKind::Io(_) => {
                "local-dir materialization filesystem operation failed"
            }
            LocalDirMaterializationErrorKind::UnsafeFileSystem(_) => {
                "local-dir materialization filesystem path is unsafe"
            }
            LocalDirMaterializationErrorKind::Validation(_) => {
                "local-dir materialization path validation failed"
            }
            LocalDirMaterializationErrorKind::Content(_) => {
                "local-dir source content failed validation"
            }
            LocalDirMaterializationErrorKind::DigestMismatch => {
                "local-dir source digest does not match its expected identity"
            }
            LocalDirMaterializationErrorKind::Conflict => {
                "local-dir destination conflicts with the selected file"
            }
            LocalDirMaterializationErrorKind::FinalValidation => {
                "local-dir destination failed validation after publication"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for LocalDirMaterializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            LocalDirMaterializationErrorKind::Validation(source) => Some(source),
            LocalDirMaterializationErrorKind::Content(source) => Some(source),
            LocalDirMaterializationErrorKind::Io(_)
            | LocalDirMaterializationErrorKind::UnsafeFileSystem(_)
            | LocalDirMaterializationErrorKind::DigestMismatch
            | LocalDirMaterializationErrorKind::Conflict
            | LocalDirMaterializationErrorKind::FinalValidation => None,
        }
    }
}

impl From<io::Error> for LocalDirMaterializationError {
    fn from(source: io::Error) -> Self {
        Self::io(&source, false)
    }
}

impl From<ValidationError> for LocalDirMaterializationError {
    fn from(source: ValidationError) -> Self {
        Self::new(LocalDirMaterializationErrorKind::Validation(source), false)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt::{self, Display, Formatter};
    use std::fs::{self, File, FileTimes};
    use std::io::{self, Cursor, Read};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use sha1::Sha1;
    use sha2::Digest;
    use tempfile::TempDir;

    use crate::cache::hub_metadata::HubTreeEntry;
    use crate::cache::key::BlobDigest;
    use crate::cache::local_dir_layout::HubLocalDirLayout;
    use crate::cache::publication::{
        Effects, FaultController, NoPublicationFaults, OsFileSystem, PublicationFaults,
        PublicationPoint, SequenceOperationIds, SystemClock,
    };
    use crate::cache::rooted_fs::{
        CacheRoot, CreateOnceOutcome, RootedEntryKind, RootedFileSystem, RootedLockGuard,
        RootedRead, RootedRegularFile, RootedWrite, StagingName, staging_path,
    };
    use crate::{Endpoint, RepoPath, RepositoryId, RepositorySpec};

    use super::{
        ExistingFilePolicy, LocalDirFileDisposition, LocalDirFileMaterializer, LocalDirFileTarget,
    };

    const CONTENT: &[u8] = b"validated local-dir payload\n";
    const OLD_CONTENT: &[u8] = b"previous user-owned file bytes\n";
    const SECRET_SENTINEL: &str = "hf_secret_local_dir_materialization_sentinel";

    struct Fixture {
        directory: TempDir,
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            Self::with_effects(
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(NoPublicationFaults),
            )
        }

        fn with_effects(
            ids: Arc<SequenceOperationIds>,
            faults: Arc<dyn PublicationFaults>,
        ) -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &spec)?;
            let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
            let effects = Effects::new(Arc::new(OsFileSystem), ids, Arc::new(SystemClock), faults);
            Ok(Self {
                directory,
                layout,
                root,
                effects,
            })
        }

        fn with_root(&self, root: Arc<dyn RootedFileSystem>) -> LocalDirFileMaterializer {
            LocalDirFileMaterializer::from_layout(self.layout.clone(), root, self.effects.clone())
        }

        fn materializer(&self) -> LocalDirFileMaterializer {
            self.with_root(Arc::clone(&self.root))
        }

        fn target(path: &str) -> Result<LocalDirFileTarget, Box<dyn Error>> {
            target(path, CONTENT)
        }

        fn destination(&self, path: &str) -> PathBuf {
            self.directory.path().join(path)
        }

        fn assert_no_staging(&self, path: &str) -> Result<(), Box<dyn Error>> {
            let destination = self.destination(path);
            let parent = destination.parent().ok_or("destination has no parent")?;
            if parent.try_exists()? {
                for entry in fs::read_dir(parent)? {
                    let name = entry?.file_name();
                    assert!(!name.to_string_lossy().starts_with(".hf-store-"));
                }
            }
            Ok(())
        }
    }

    #[test]
    fn exact_file_is_reused_without_reading_source_or_changing_mtime() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let target = Fixture::target("nested/config.json")?;
        let destination = fixture.destination(target.path().as_str());
        fs::create_dir_all(destination.parent().ok_or("destination has no parent")?)?;
        fs::write(&destination, CONTENT)?;
        let fixed_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        File::options()
            .write(true)
            .open(&destination)?
            .set_times(FileTimes::new().set_modified(fixed_time))?;
        let before = fs::metadata(&destination)?.modified()?;
        let mut source = ReadCounter::new(CONTENT);

        let result =
            fixture
                .materializer()
                .materialize(&target, &mut source, ExistingFilePolicy::Reject)?;

        assert_eq!(result.disposition(), LocalDirFileDisposition::Reused);
        assert_eq!(result.path(), destination);
        assert_eq!(result.size(), CONTENT.len() as u64);
        assert_eq!(result.digest(), BlobDigest::for_bytes(CONTENT));
        assert_eq!(source.reads(), 0);
        assert_eq!(fs::metadata(&destination)?.modified()?, before);
        Ok(())
    }

    #[test]
    fn conflicting_regular_file_is_rejected_without_reading_source() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("config.json")?;
        fs::write(fixture.destination("config.json"), OLD_CONTENT)?;
        let mut source = ReadCounter::new(CONTENT);

        let error = fixture
            .materializer()
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)
            .expect_err("replaced a different regular file without permission");

        assert!(error.is_conflict());
        assert!(!error.may_have_published());
        assert_eq!(source.reads(), 0);
        assert_eq!(fs::read(fixture.destination("config.json"))?, OLD_CONTENT);
        Ok(())
    }

    #[test]
    fn missing_file_is_copied_through_adjacent_staging() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("nested/config.json")?;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let root: Arc<dyn RootedFileSystem> = Arc::new(CreatePathProbe {
            inner: Arc::clone(&fixture.root),
            created: Arc::clone(&observed),
        });
        let mut source = Cursor::new(CONTENT);

        let result = fixture.with_root(root).materialize(
            &target,
            &mut source,
            ExistingFilePolicy::Reject,
        )?;

        assert_eq!(result.disposition(), LocalDirFileDisposition::Copied);
        assert_eq!(
            fs::read(fixture.destination("nested/config.json"))?,
            CONTENT
        );
        let created = observed
            .lock()
            .map_err(|_poisoned| io::Error::other("create path probe poisoned"))?;
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].parent(), Some(Path::new("nested")));
        assert_ne!(created[0], PathBuf::from("nested/config.json"));
        drop(created);
        fixture.assert_no_staging("nested/config.json")?;
        Ok(())
    }

    #[test]
    fn explicit_policy_replaces_only_a_different_regular_file() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("config.json")?;
        fs::write(fixture.destination("config.json"), OLD_CONTENT)?;
        let mut source = Cursor::new(CONTENT);

        let result = fixture.materializer().materialize(
            &target,
            &mut source,
            ExistingFilePolicy::ReplaceRegularFile,
        )?;

        assert_eq!(result.disposition(), LocalDirFileDisposition::Copied);
        assert_eq!(fs::read(fixture.destination("config.json"))?, CONTENT);
        fixture.assert_no_staging("config.json")?;
        Ok(())
    }

    #[test]
    fn valid_git_and_lfs_identities_are_checked_while_copying() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let git = git_target("git/config.json", CONTENT)?;
        let lfs = lfs_target("lfs/model.bin", CONTENT)?;

        for target in [git, lfs] {
            let mut source = Cursor::new(CONTENT);
            let result = fixture.materializer().materialize(
                &target,
                &mut source,
                ExistingFilePolicy::Reject,
            )?;
            assert_eq!(result.disposition(), LocalDirFileDisposition::Copied);
            assert_eq!(fs::read(result.path())?, CONTENT);
        }
        Ok(())
    }

    #[test]
    fn source_identity_and_digest_mismatches_preserve_destination_and_cleanup_staging()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = "nested/config.json";
        let cases = mismatch_targets(path)?;

        for (index, (target, source_mismatch)) in cases.into_iter().enumerate() {
            let destination = fixture.destination(path);
            fs::create_dir_all(destination.parent().ok_or("destination has no parent")?)?;
            fs::write(&destination, OLD_CONTENT)?;
            let mut source = Cursor::new(CONTENT);
            let error = fixture
                .materializer()
                .materialize(&target, &mut source, ExistingFilePolicy::ReplaceRegularFile)
                .expect_err("published mismatched local-dir source");
            if source_mismatch {
                assert!(error.is_source_mismatch(), "mismatch case {index}");
            } else {
                assert!(error.is_digest_mismatch(), "mismatch case {index}");
            }
            assert!(!error.may_have_published());
            assert_eq!(fs::read(&destination)?, OLD_CONTENT);
            fixture.assert_no_staging(path)?;
        }
        Ok(())
    }

    #[test]
    fn copied_destination_is_independent_from_its_source() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("weights/model.bin")?;
        let source_path = fixture.directory.path().join("source.bin");
        fs::write(&source_path, CONTENT)?;
        let mut source = File::open(&source_path)?;

        fixture
            .materializer()
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)?;
        fs::write(&source_path, b"source changed after materialization")?;

        let destination = fixture.destination("weights/model.bin");
        assert_eq!(fs::read(&destination)?, CONTENT);
        fs::write(&destination, b"user changed the local-dir file")?;
        assert_eq!(
            fs::read(&source_path)?,
            b"source changed after materialization"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_ne!(
                fs::metadata(source_path)?.ino(),
                fs::metadata(destination)?.ino()
            );
        }
        Ok(())
    }

    #[test]
    fn directories_and_final_links_remain_conflicts_even_with_replace_policy()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("config.json")?;
        let destination = fixture.destination("config.json");
        fs::create_dir(&destination)?;
        let mut source = ReadCounter::new(CONTENT);
        let error = fixture
            .materializer()
            .materialize(&target, &mut source, ExistingFilePolicy::ReplaceRegularFile)
            .expect_err("replaced a directory");
        assert!(error.is_conflict());
        assert_eq!(source.reads(), 0);
        fs::remove_dir(&destination)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let outside = fixture.directory.path().join("outside");
            fs::write(&outside, OLD_CONTENT)?;
            symlink(&outside, &destination)?;
            let mut source = ReadCounter::new(CONTENT);
            let error = fixture
                .materializer()
                .materialize(&target, &mut source, ExistingFilePolicy::ReplaceRegularFile)
                .expect_err("replaced a symbolic link");
            assert!(error.is_conflict());
            assert_eq!(source.reads(), 0);
            assert_eq!(fs::read(outside)?, OLD_CONTENT);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn special_files_are_conflicts_and_linked_ancestors_are_unsafe() -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;

        let fixture = Fixture::new()?;
        let socket_path = fixture.destination("model.socket");
        let _listener = UnixListener::bind(&socket_path)?;
        let socket_target = Fixture::target("model.socket")?;
        let mut source = ReadCounter::new(CONTENT);
        let error = fixture
            .materializer()
            .materialize(
                &socket_target,
                &mut source,
                ExistingFilePolicy::ReplaceRegularFile,
            )
            .expect_err("replaced a special file");
        assert!(error.is_conflict());
        assert_eq!(source.reads(), 0);

        let outside = TempDir::new()?;
        symlink(outside.path(), fixture.destination("redirected"))?;
        let linked_target = Fixture::target("redirected/config.json")?;
        let mut source = ReadCounter::new(CONTENT);
        let error = fixture
            .materializer()
            .materialize(
                &linked_target,
                &mut source,
                ExistingFilePolicy::ReplaceRegularFile,
            )
            .expect_err("followed a linked ancestor");
        assert!(error.is_unsafe());
        assert_eq!(source.reads(), 0);
        assert!(!outside.path().join("config.json").try_exists()?);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn junctions_are_conflicts_as_final_entries_and_unsafe_as_ancestors()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        let junction = fixture.destination("redirected");
        create_dir_junction(outside.path(), &junction)?;

        let final_target = Fixture::target("redirected")?;
        let mut final_source = ReadCounter::new(CONTENT);
        let final_error = fixture
            .materializer()
            .materialize(
                &final_target,
                &mut final_source,
                ExistingFilePolicy::ReplaceRegularFile,
            )
            .expect_err("replaced a final junction");
        assert!(final_error.is_conflict());
        assert_eq!(final_source.reads(), 0);

        let nested_target = Fixture::target("redirected/config.json")?;
        let mut nested_source = ReadCounter::new(CONTENT);
        let nested_error = fixture
            .materializer()
            .materialize(
                &nested_target,
                &mut nested_source,
                ExistingFilePolicy::ReplaceRegularFile,
            )
            .expect_err("followed a junction ancestor");
        assert!(nested_error.is_unsafe());
        assert_eq!(nested_source.reads(), 0);
        assert!(!outside.path().join("config.json").try_exists()?);
        Ok(())
    }

    #[test]
    fn reserved_local_dir_paths_are_rejected_before_source_reads() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        for path in [
            ".cache/huggingface/config.json",
            "CACHE~1/huggingface/config.json",
            "weights/MODEL~1.BIN",
        ] {
            let target = Fixture::target(path)?;
            let mut source = ReadCounter::new(CONTENT);
            let error = fixture
                .materializer()
                .materialize(&target, &mut source, ExistingFilePolicy::Reject)
                .expect_err("materialized an unsafe local-dir path");
            assert!(error.is_unsafe(), "{path}");
            assert_eq!(source.reads(), 0, "{path}");
        }
        Ok(())
    }

    #[test]
    fn destination_is_rechecked_after_staging_before_activation() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let destination = directory.path().join("config.json");
        let faults = Arc::new(MutateDestinationFault::new(
            PublicationPoint::AfterStagingSync,
            destination.clone(),
            CONTENT,
        ));
        let materializer = materializer_with_faults(directory.path(), faults)?;
        let target = Fixture::target("config.json")?;
        let mut source = Cursor::new(CONTENT);

        let result = materializer.materialize(&target, &mut source, ExistingFilePolicy::Reject)?;

        assert_eq!(result.disposition(), LocalDirFileDisposition::Reused);
        assert_eq!(fs::read(destination)?, CONTENT);
        Ok(())
    }

    #[test]
    fn destination_is_reopened_and_validated_after_activation() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let destination = directory.path().join("config.json");
        let faults = Arc::new(MutateDestinationFault::new(
            PublicationPoint::AfterAtomicReplace,
            destination.clone(),
            OLD_CONTENT,
        ));
        let materializer = materializer_with_faults(directory.path(), faults)?;
        let target = Fixture::target("config.json")?;
        let mut source = Cursor::new(CONTENT);

        let error = materializer
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)
            .expect_err("accepted content changed immediately after activation");

        assert!(error.may_have_published());
        assert_eq!(fs::read(destination)?, OLD_CONTENT);
        Ok(())
    }

    #[test]
    fn staging_name_collision_preserves_the_preexisting_entry() -> Result<(), Box<dyn Error>> {
        let ids = Arc::new(SequenceOperationIds::new(7));
        let fixture = Fixture::with_effects(ids, Arc::new(NoPublicationFaults))?;
        let target = Fixture::target("nested/config.json")?;
        let name = StagingName::new("00000000000000000000000000000007")?;
        let staging = staging_path(Path::new("nested/config.json"), &name)?;
        let absolute_staging = fixture.directory.path().join(&staging);
        fs::create_dir_all(absolute_staging.parent().ok_or("staging has no parent")?)?;
        fs::write(&absolute_staging, SECRET_SENTINEL)?;
        let mut source = Cursor::new(CONTENT);

        let error = fixture
            .materializer()
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)
            .expect_err("overwrote a colliding staging entry");

        assert_eq!(error.io_kind(), Some(io::ErrorKind::AlreadyExists));
        assert!(!error.may_have_published());
        assert_eq!(fs::read_to_string(absolute_staging)?, SECRET_SENTINEL);
        assert!(!fixture.destination("nested/config.json").try_exists()?);
        Ok(())
    }

    #[test]
    fn destination_equal_to_predictable_staging_name_is_rejected_before_writing()
    -> Result<(), Box<dyn Error>> {
        let staging_path = ".hf-store-00000000000000000000000000000001.tmp";
        for path in [
            staging_path,
            ".HF-STORE-00000000000000000000000000000001.TMP",
        ] {
            let fixture = Fixture::with_effects(
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(NoPublicationFaults),
            )?;
            let target = Fixture::target(path)?;
            let mut source = ReadCounter::new(CONTENT);
            let created = Arc::new(Mutex::new(Vec::new()));
            let root: Arc<dyn RootedFileSystem> = Arc::new(CreatePathProbe {
                inner: Arc::clone(&fixture.root),
                created: Arc::clone(&created),
            });

            let error = fixture
                .with_root(root)
                .materialize(&target, &mut source, ExistingFilePolicy::Reject)
                .expect_err("treated a user-visible destination as its staging file alias");

            assert!(error.is_conflict(), "{path}");
            assert!(!error.may_have_published(), "{path}");
            assert_eq!(source.reads(), 0, "{path}");
            assert!(
                created
                    .lock()
                    .map_err(|_poisoned| io::Error::other("create path probe poisoned"))?
                    .is_empty(),
                "{path}"
            );
            assert!(!fixture.destination(path).try_exists()?, "{path}");
            assert!(!fixture.destination(staging_path).try_exists()?, "{path}");
        }
        Ok(())
    }

    #[test]
    fn staging_is_synchronized_before_install_and_parent_after_install()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let state = Arc::new(OrderState::default());
        let root: Arc<dyn RootedFileSystem> = Arc::new(OrderingRoot {
            inner: Arc::clone(&fixture.root),
            state: Arc::clone(&state),
        });
        let target = Fixture::target("config.json")?;
        let mut source = Cursor::new(CONTENT);

        fixture
            .with_root(root)
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)?;

        assert!(state.staging_flushed.load(Ordering::Acquire));
        assert!(state.staging_synced.load(Ordering::Acquire));
        assert!(state.installed.load(Ordering::Acquire));
        assert!(state.parent_synced.load(Ordering::Acquire));
        Ok(())
    }

    #[test]
    fn publication_faults_preserve_conservative_may_have_published_state()
    -> Result<(), Box<dyn Error>> {
        for (point, may_have_published, destination_exists) in [
            (PublicationPoint::AfterStagingCreate, false, false),
            (PublicationPoint::AfterStagingSync, false, false),
            (PublicationPoint::BeforeAtomicReplace, false, false),
            (PublicationPoint::AfterAtomicReplace, true, true),
        ] {
            let faults = Arc::new(FaultController::default());
            faults.fail_once(point);
            let fixture = Fixture::with_effects(
                Arc::new(SequenceOperationIds::new(1)),
                faults as Arc<dyn PublicationFaults>,
            )?;
            let target = Fixture::target("config.json")?;
            let mut source = Cursor::new(CONTENT);
            let error = fixture
                .materializer()
                .materialize(&target, &mut source, ExistingFilePolicy::Reject)
                .expect_err("ignored an injected publication fault");
            assert_eq!(error.may_have_published(), may_have_published, "{point:?}");
            assert_eq!(
                fixture.destination("config.json").try_exists()?,
                destination_exists
            );
            fixture.assert_no_staging("config.json")?;
        }
        Ok(())
    }

    #[test]
    fn parent_sync_failure_reports_that_bytes_may_be_visible() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(ParentSyncFailureRoot {
            inner: Arc::clone(&fixture.root),
        });
        let target = Fixture::target("config.json")?;
        let mut source = Cursor::new(CONTENT);

        let error = fixture
            .with_root(root)
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)
            .expect_err("ignored parent sync failure");

        assert!(error.may_have_published());
        assert_eq!(error.io_kind(), Some(io::ErrorKind::PermissionDenied));
        assert_eq!(fs::read(fixture.destination("config.json"))?, CONTENT);
        Ok(())
    }

    #[test]
    fn diagnostics_redact_paths_remote_ids_digests_and_io_sources() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse(format!("{SECRET_SENTINEL}/config.json"))?;
        let entry = HubTreeEntry::new(CONTENT.len() as u64, SECRET_SENTINEL)?;
        let target = LocalDirFileTarget::new(path, entry, BlobDigest::for_bytes(CONTENT));
        assert!(!format!("{target:?}").contains(SECRET_SENTINEL));
        assert!(
            !format!("{:?}", fixture.materializer())
                .contains(fixture.directory.path().to_string_lossy().as_ref())
        );
        let mut source = FailingRead;
        let error = fixture
            .materializer()
            .materialize(&target, &mut source, ExistingFilePolicy::Reject)
            .expect_err("ignored source read failure");
        assert_secret_absent(&error);
        Ok(())
    }

    fn target(path: &str, bytes: &[u8]) -> Result<LocalDirFileTarget, Box<dyn Error>> {
        let entry = HubTreeEntry::new(bytes.len() as u64, "opaque-validator")?;
        Ok(LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            entry,
            BlobDigest::for_bytes(bytes),
        ))
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

    fn materializer_with_faults(
        root: &Path,
        faults: Arc<dyn PublicationFaults>,
    ) -> Result<LocalDirFileMaterializer, Box<dyn Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(root, &endpoint, &spec)?;
        let rooted: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(root)?);
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(SequenceOperationIds::new(1)),
            Arc::new(SystemClock),
            faults,
        );
        Ok(LocalDirFileMaterializer::from_layout(
            layout, rooted, effects,
        ))
    }

    fn mismatch_targets(path: &str) -> Result<Vec<(LocalDirFileTarget, bool)>, Box<dyn Error>> {
        let size = CONTENT.len() as u64;
        let short = LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            HubTreeEntry::new(size + 1, "opaque-short")?,
            BlobDigest::for_bytes(CONTENT),
        );
        let long = LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            HubTreeEntry::new(size - 1, "opaque-long")?,
            BlobDigest::for_bytes(CONTENT),
        );
        let wrong_git = LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            HubTreeEntry::new(size, "0000000000000000000000000000000000000000")?,
            BlobDigest::for_bytes(CONTENT),
        );
        let wrong_lfs = LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            HubTreeEntry::new(size, "opaque-lfs")?.with_lfs("0".repeat(64), size)?,
            BlobDigest::for_bytes(CONTENT),
        );
        let wrong_local = LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            HubTreeEntry::new(size, "opaque-local")?,
            BlobDigest::for_bytes(b"different local digest"),
        );
        Ok(vec![
            (short, true),
            (long, true),
            (wrong_git, true),
            (wrong_lfs, true),
            (wrong_local, false),
        ])
    }

    fn git_target(path: &str, bytes: &[u8]) -> Result<LocalDirFileTarget, Box<dyn Error>> {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
        hasher.update(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, format!("{:x}", hasher.finalize()))?;
        Ok(LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            entry,
            BlobDigest::for_bytes(bytes),
        ))
    }

    fn lfs_target(path: &str, bytes: &[u8]) -> Result<LocalDirFileTarget, Box<dyn Error>> {
        let size = bytes.len() as u64;
        let sha256 = format!("{:x}", sha2::Sha256::digest(bytes));
        let entry = HubTreeEntry::new(size, "opaque-lfs")?.with_lfs(sha256, size)?;
        Ok(LocalDirFileTarget::new(
            RepoPath::parse(path)?,
            entry,
            BlobDigest::for_bytes(bytes),
        ))
    }

    struct ReadCounter<'a> {
        inner: Cursor<&'a [u8]>,
        reads: usize,
    }

    impl<'a> ReadCounter<'a> {
        fn new(bytes: &'a [u8]) -> Self {
            Self {
                inner: Cursor::new(bytes),
                reads: 0,
            }
        }

        const fn reads(&self) -> usize {
            self.reads
        }
    }

    impl Read for ReadCounter<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            self.inner.read(buffer)
        }
    }

    #[derive(Debug)]
    struct CreatePathProbe {
        inner: Arc<dyn RootedFileSystem>,
        created: Arc<Mutex<Vec<PathBuf>>>,
    }

    #[derive(Debug, Default)]
    struct OrderState {
        staging_flushed: AtomicBool,
        staging_synced: AtomicBool,
        installed: AtomicBool,
        parent_synced: AtomicBool,
    }

    #[derive(Debug)]
    struct OrderingRoot {
        inner: Arc<dyn RootedFileSystem>,
        state: Arc<OrderState>,
    }

    impl RootedFileSystem for OrderingRoot {
        fn ensure_dir(&self, path: &Path) -> io::Result<()> {
            self.inner.ensure_dir(path)
        }

        fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
            self.inner.entry_kind(path)
        }

        fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
            self.inner.open_regular(path)
        }

        fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead> {
            self.inner.read_regular_bounded(path, limit)
        }

        fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>> {
            Ok(Box::new(SyncProbeWrite {
                inner: self.inner.create_new(path)?,
                synced: Arc::clone(&self.state),
            }))
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }

        fn install_staged_create_once(
            &self,
            staging: &Path,
            destination: &Path,
        ) -> io::Result<CreateOnceOutcome> {
            if !self.state.staging_synced.load(Ordering::Acquire) {
                return Err(io::Error::other("installed unsynchronized staging"));
            }
            let outcome = self
                .inner
                .install_staged_create_once(staging, destination)?;
            self.state.installed.store(true, Ordering::Release);
            Ok(outcome)
        }

        fn install_staged_replace(&self, staging: &Path, destination: &Path) -> io::Result<()> {
            if !self.state.staging_synced.load(Ordering::Acquire) {
                return Err(io::Error::other("replaced from unsynchronized staging"));
            }
            self.inner.install_staged_replace(staging, destination)?;
            self.state.installed.store(true, Ordering::Release);
            Ok(())
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
            if !self.state.installed.load(Ordering::Acquire) {
                return Err(io::Error::other("synchronized parent before install"));
            }
            self.inner.sync_directory(path)?;
            self.state.parent_synced.store(true, Ordering::Release);
            Ok(())
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
    }

    struct SyncProbeWrite {
        inner: Box<dyn RootedWrite>,
        synced: Arc<OrderState>,
    }

    impl io::Write for SyncProbeWrite {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.inner.write(buffer)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()?;
            self.synced.staging_flushed.store(true, Ordering::Release);
            Ok(())
        }
    }

    impl RootedWrite for SyncProbeWrite {
        fn sync_all(&self) -> io::Result<()> {
            if !self.synced.staging_flushed.load(Ordering::Acquire) {
                return Err(io::Error::other("synchronized staging before flushing"));
            }
            self.inner.sync_all()?;
            self.synced.staging_synced.store(true, Ordering::Release);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ParentSyncFailureRoot {
        inner: Arc<dyn RootedFileSystem>,
    }

    macro_rules! delegate_rooted_file_system {
        ($type:ty, create_new | $self_ident:ident, $path_ident:ident | $body:block) => {
            impl RootedFileSystem for $type {
                fn ensure_dir(&self, path: &Path) -> io::Result<()> {
                    self.inner.ensure_dir(path)
                }
                fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
                    self.inner.entry_kind(path)
                }
                fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
                    self.inner.open_regular(path)
                }
                fn read_regular_bounded(
                    &self,
                    path: &Path,
                    limit: usize,
                ) -> io::Result<RootedRead> {
                    self.inner.read_regular_bounded(path, limit)
                }
                fn create_new(&self, $path_ident: &Path) -> io::Result<Box<dyn RootedWrite>> {
                    let $self_ident = self;
                    $body
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
                fn install_staged_replace(
                    &self,
                    staging: &Path,
                    destination: &Path,
                ) -> io::Result<()> {
                    self.inner.install_staged_replace(staging, destination)
                }
                fn replace(
                    &self,
                    path: &Path,
                    bytes: &[u8],
                    staging: &StagingName,
                ) -> io::Result<()> {
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
        };
        ($type:ty, sync_directory | $self_ident:ident, $path_ident:ident | $body:block) => {
            impl RootedFileSystem for $type {
                fn ensure_dir(&self, path: &Path) -> io::Result<()> {
                    self.inner.ensure_dir(path)
                }
                fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
                    self.inner.entry_kind(path)
                }
                fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
                    self.inner.open_regular(path)
                }
                fn read_regular_bounded(
                    &self,
                    path: &Path,
                    limit: usize,
                ) -> io::Result<RootedRead> {
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
                fn install_staged_replace(
                    &self,
                    staging: &Path,
                    destination: &Path,
                ) -> io::Result<()> {
                    self.inner.install_staged_replace(staging, destination)
                }
                fn replace(
                    &self,
                    path: &Path,
                    bytes: &[u8],
                    staging: &StagingName,
                ) -> io::Result<()> {
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
                fn sync_directory(&self, $path_ident: &Path) -> io::Result<()> {
                    let $self_ident = self;
                    $body
                }
                fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
                    self.inner.read_dir(path)
                }
            }
        };
    }

    delegate_rooted_file_system!(
        CreatePathProbe,
        create_new | probe,
        path | {
            probe
                .created
                .lock()
                .map_err(|_poisoned| io::Error::other("create path probe poisoned"))?
                .push(path.to_path_buf());
            probe.inner.create_new(path)
        }
    );

    delegate_rooted_file_system!(
        ParentSyncFailureRoot,
        sync_directory | _probe,
        _path | {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected local-dir parent sync failure",
            ))
        }
    );

    #[derive(Debug)]
    struct MutateDestinationFault {
        point: PublicationPoint,
        destination: PathBuf,
        bytes: &'static [u8],
        fired: AtomicBool,
    }

    impl MutateDestinationFault {
        fn new(point: PublicationPoint, destination: PathBuf, bytes: &'static [u8]) -> Self {
            Self {
                point,
                destination,
                bytes,
                fired: AtomicBool::new(false),
            }
        }
    }

    impl PublicationFaults for MutateDestinationFault {
        fn check(&self, point: PublicationPoint) -> io::Result<()> {
            if point == self.point && !self.fired.swap(true, Ordering::AcqRel) {
                fs::write(&self.destination, self.bytes)?;
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailingRead;

    impl Read for FailingRead {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other(SecretError))
        }
    }

    #[derive(Debug)]
    struct SecretError;

    impl Display for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_SENTINEL)
        }
    }

    impl Error for SecretError {}

    fn assert_secret_absent(error: &(dyn Error + 'static)) {
        let mut current = Some(error);
        while let Some(source) = current {
            assert!(!source.to_string().contains(SECRET_SENTINEL));
            assert!(!format!("{source:?}").contains(SECRET_SENTINEL));
            current = source.source();
        }
    }
}
