use std::backtrace::Backtrace;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
#[cfg(test)]
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use cap_fs_ext::OpenOptionsSyncExt;
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions as CapOpenOptions};
use sha2::{Digest, Sha256};

use crate::validation::ValidationError;
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::hub_layout::{HubBlobKey, HubCacheLayout};
use super::key::{BlobDigest, OriginKey, RepositoryKey, SelectionId};
use super::layout::CacheLayout;
use super::metadata::{
    CacheRecord, FormatRecord, GcTombstoneKind, GcTombstoneRecord, HubBlobBindingRecord,
    MetadataError, OriginRecord, PartialGcTombstoneRecord, PartialTransferRecord, RefRecord,
    RemoteTreeRecord, RepositoryRecord, SnapshotFileRecord, SnapshotManifestRecord, decode_record,
    encode_record,
};
#[cfg(test)]
use super::rooted_fs::RootedRead;
use super::rooted_fs::{
    CacheRoot, CreateOnceOutcome, RootedEntryKind, RootedFileSystem, RootedLockAttempt,
    RootedLockGuard, RootedRegularFile, RootedWrite, StagingName, is_reparse_point,
    is_unsafe_cache_path_error, unsafe_cache_path,
};
use super::sanitized_io::SanitizedIo;

const COPY_BUFFER_SIZE: usize = 64 * 1024;
const MAX_SMALL_RECORD_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_RECORD_BYTES: usize = 16 * 1024 * 1024;

fn has_json_extension(name: &str) -> bool {
    Path::new(name).extension() == Some(std::ffi::OsStr::new("json"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublicationPoint {
    AfterStagingCreate,
    AfterStagingSync,
    BeforeBlobPublish,
    AfterBlobPublish,
    BeforeAtomicReplace,
    AfterAtomicReplace,
    BeforeCompletionReplace,
    AfterCompletionReplace,
    BeforeSnapshotEntryPublish,
    AfterSnapshotEntryPublish,
    BeforeSnapshotManifestPublish,
    AfterSnapshotManifestPublish,
}

pub(super) trait PublicationFaults: Debug + Send + Sync {
    fn check(&self, point: PublicationPoint) -> io::Result<()>;
}

#[derive(Debug)]
pub(super) struct NoPublicationFaults;

impl PublicationFaults for NoPublicationFaults {
    fn check(&self, _point: PublicationPoint) -> io::Result<()> {
        Ok(())
    }
}

pub(super) trait OperationIds: Debug + Send + Sync {
    fn next(&self) -> io::Result<OperationId>;
}

#[derive(Debug)]
pub(super) struct RandomOperationIds;

impl OperationIds for RandomOperationIds {
    fn next(&self) -> io::Result<OperationId> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        Ok(OperationId(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct OperationId([u8; 16]);

impl Display for OperationId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

pub(super) trait Clock: Debug + Send + Sync {
    fn now_unix_millis(&self) -> io::Result<u64>;
}

#[derive(Debug)]
pub(super) struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_millis(&self) -> io::Result<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?;
        u64::try_from(duration.as_millis()).map_err(io::Error::other)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EntryKind {
    Missing,
    RegularFile,
    Other,
}

pub(super) enum RegularFileOpen {
    File {
        reader: Box<dyn Read + Send>,
        size: u64,
    },
    Missing,
    Other,
}

pub(super) trait CacheDirectory: Debug + Send + Sync {
    fn open_dir_nofollow(&self, path: &Path) -> io::Result<Arc<dyn CacheDirectory>>;
    fn open_regular(&self, path: &Path) -> io::Result<RegularFileOpen>;
    fn entry_kind(&self, path: &Path) -> io::Result<EntryKind>;
    fn read_link(&self, path: &Path) -> io::Result<PathBuf>;
}

#[derive(Clone, Debug)]
pub(super) struct CacheAuthority {
    reader: Arc<dyn CacheDirectory>,
    writer: Arc<dyn RootedFileSystem>,
}

impl CacheAuthority {
    fn new(reader: Arc<dyn CacheDirectory>, writer: Arc<dyn RootedFileSystem>) -> Self {
        Self { reader, writer }
    }

    pub(super) fn reader(&self) -> Arc<dyn CacheDirectory> {
        Arc::clone(&self.reader)
    }

    pub(super) fn writer(&self) -> Arc<dyn RootedFileSystem> {
        Arc::clone(&self.writer)
    }
}

pub(super) trait FileSystem: Debug + Send + Sync {
    fn open_cache_authority(&self, path: &Path) -> io::Result<CacheAuthority>;
}

#[derive(Debug)]
pub(super) struct OsFileSystem;

impl FileSystem for OsFileSystem {
    fn open_cache_authority(&self, path: &Path) -> io::Result<CacheAuthority> {
        let path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        let directory = Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
        let reader = Arc::new(OsCacheDirectory {
            directory: directory.try_clone()?,
        });
        let writer = Arc::new(CacheRoot::from_dir(directory));
        Ok(CacheAuthority::new(reader, writer))
    }
}

#[derive(Debug)]
struct OsCacheDirectory {
    directory: Dir,
}

impl OsCacheDirectory {
    fn open_dir_chain(&self, path: &Path) -> io::Result<Dir> {
        let mut directory = self.directory.try_clone()?;
        for component in path.components() {
            let Component::Normal(name) = component else {
                return Err(invalid_cache_relative_path());
            };
            directory = open_cache_child_directory(&directory, name)?;
        }
        Ok(directory)
    }

    fn open_parent_and_name(&self, path: &Path) -> io::Result<(Dir, PathBuf)> {
        let Some(Component::Normal(name)) = path.components().next_back() else {
            return Err(invalid_cache_relative_path());
        };
        let Some(parent) = path.parent() else {
            return Err(invalid_cache_relative_path());
        };
        Ok((self.open_dir_chain(parent)?, PathBuf::from(name)))
    }
}

impl CacheDirectory for OsCacheDirectory {
    fn open_dir_nofollow(&self, path: &Path) -> io::Result<Arc<dyn CacheDirectory>> {
        let directory = self.open_dir_chain(path)?;
        Ok(Arc::new(Self { directory }))
    }

    fn open_regular(&self, path: &Path) -> io::Result<RegularFileOpen> {
        let (parent, name) = match self.open_parent_and_name(path) {
            Ok(location) => location,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RegularFileOpen::Missing);
            }
            Err(error) => return Err(error),
        };
        match parent.symlink_metadata(&name) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RegularFileOpen::Missing);
            }
            Err(error) => return Err(error),
            Ok(metadata) if metadata_is_redirect(&metadata) => {
                return Err(unsafe_cache_path(
                    "Hub cache file is a link or reparse point",
                ));
            }
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_metadata) => return Ok(RegularFileOpen::Other),
        }

        let mut options = CapOpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        #[cfg(unix)]
        options.nonblock(true);
        let file = match parent.open_with(&name, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RegularFileOpen::Missing);
            }
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if is_reparse_point(&metadata) {
            return Err(unsafe_cache_path(
                "opened Hub cache file is a reparse point",
            ));
        }
        if !metadata.file_type().is_file() {
            return Ok(RegularFileOpen::Other);
        }
        Ok(RegularFileOpen::File {
            reader: Box::new(file),
            size: metadata.len(),
        })
    }

    fn entry_kind(&self, path: &Path) -> io::Result<EntryKind> {
        let (parent, name) = match self.open_parent_and_name(path) {
            Ok(location) => location,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(EntryKind::Missing);
            }
            Err(error) => return Err(error),
        };
        match parent.symlink_metadata(name) {
            Ok(metadata) if metadata_is_redirect(&metadata) => Ok(EntryKind::Other),
            Ok(metadata) if metadata.file_type().is_file() => Ok(EntryKind::RegularFile),
            Ok(_metadata) => Ok(EntryKind::Other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(EntryKind::Missing),
            Err(error) => Err(error),
        }
    }

    fn read_link(&self, path: &Path) -> io::Result<PathBuf> {
        let (parent, name) = self.open_parent_and_name(path)?;
        parent.read_link_contents(name)
    }
}

fn open_cache_child_directory(parent: &Dir, name: &std::ffi::OsStr) -> io::Result<Dir> {
    let metadata = parent.symlink_metadata(name)?;
    if metadata_is_redirect(&metadata) {
        return Err(unsafe_cache_path(
            "Hub cache directory component is a link or reparse point",
        ));
    }
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Hub cache directory component is not a directory",
        ));
    }

    let directory = parent.open_dir_nofollow(name)?;
    let opened = directory.dir_metadata()?;
    if is_reparse_point(&opened) {
        return Err(unsafe_cache_path(
            "opened Hub cache directory is a reparse point",
        ));
    }
    if !opened.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "opened Hub cache entry is not a directory",
        ));
    }
    Ok(directory)
}

fn metadata_is_redirect(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink() || is_reparse_point(metadata)
}

fn invalid_cache_relative_path() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "cache capability path must contain only relative normal components",
    )
}

#[derive(Clone, Debug)]
pub(super) struct Effects {
    file_system: Arc<dyn FileSystem>,
    operation_ids: Arc<dyn OperationIds>,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn PublicationFaults>,
}

impl Effects {
    pub(super) fn production() -> Self {
        Self::new(
            Arc::new(OsFileSystem),
            Arc::new(RandomOperationIds),
            Arc::new(SystemClock),
            Arc::new(NoPublicationFaults),
        )
    }

    pub(super) fn new(
        file_system: Arc<dyn FileSystem>,
        operation_ids: Arc<dyn OperationIds>,
        clock: Arc<dyn Clock>,
        faults: Arc<dyn PublicationFaults>,
    ) -> Self {
        Self {
            file_system,
            operation_ids,
            clock,
            faults,
        }
    }

    pub(super) fn open_cache_authority(&self, path: &Path) -> io::Result<CacheAuthority> {
        self.file_system.open_cache_authority(path)
    }

    pub(super) fn next_staging_name(&self) -> io::Result<StagingName> {
        let operation_id = self.operation_ids.next()?;
        StagingName::new(&operation_id.to_string())
    }

    pub(super) fn now_unix_millis(&self) -> io::Result<u64> {
        self.clock.now_unix_millis()
    }

    pub(super) fn check_publication_fault(&self, point: PublicationPoint) -> io::Result<()> {
        self.faults.check(point)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CacheKernel {
    layout: CacheLayout,
    root: Arc<dyn RootedFileSystem>,
    origin_record: OriginRecord,
    repository_record: RepositoryRecord,
    effects: Effects,
}

pub(super) struct CachePartialSink {
    writer: Box<dyn RootedWrite>,
}

#[derive(Clone, Debug)]
pub(super) struct OwnedSnapshotFile {
    path: RepoPath,
    content_path: PathBuf,
    digest: BlobDigest,
    size: u64,
}

#[derive(Debug)]
pub(super) struct SnapshotLease {
    _guard: Box<dyn RootedLockGuard>,
}

#[derive(Clone, Debug)]
pub(super) struct CacheInventoryEntry {
    pub(super) relative_path: Box<str>,
    pub(super) namespace: Box<str>,
    pub(super) kind: RootedEntryKind,
    pub(super) metadata_state: CacheInventoryMetadataState,
    pub(super) semantic: CacheInventorySemantic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CacheInventoryMetadataState {
    Recognized,
    Corrupt,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CacheInventorySemantic {
    Ordinary,
    SidecarOnly,
    SnapshotOnly,
    CopiedWithBlob,
    RelativeSymlink,
}

#[derive(Clone, Debug)]
pub(crate) struct PartialGcCandidate {
    pub(super) key: Box<str>,
    commit: CommitId,
    path: RepoPath,
    record_digest: BlobDigest,
    received_size: u64,
    updated_unix_millis: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotGcCandidate {
    key: Box<str>,
    commit: CommitId,
    selection_id: Box<str>,
    fingerprint: BlobDigest,
    logical_bytes: u64,
    modified_unix_millis: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct BlobGcCandidate {
    key: Box<str>,
    digest: BlobDigest,
    fingerprint: BlobDigest,
    logical_bytes: u64,
    modified_unix_millis: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum GcObservation {
    Partial(PartialGcCandidate),
    Snapshot(SnapshotGcCandidate),
    Blob(BlobGcCandidate),
}

#[derive(Debug)]
struct ValidatedGcSnapshot {
    candidate: SnapshotGcCandidate,
    commit: CommitId,
    key: Box<str>,
    digests: BTreeSet<BlobDigest>,
    modified_unix_millis: u64,
}

impl GcObservation {
    pub(crate) fn snapshot_from_plan(
        key: Box<str>,
        commit: &str,
        selection_id: &str,
        fingerprint: BlobDigest,
        logical_bytes: u64,
        modified_unix_millis: u64,
    ) -> Result<Self, crate::HubError> {
        let commit = CommitId::parse(commit).map_err(crate::HubError::validation)?;
        validate_fixed_digest(selection_id)?;
        if key.as_ref() != snapshot_gc_key(&commit, selection_id).as_ref() {
            return Err(crate::HubError::protocol());
        }
        Ok(Self::Snapshot(SnapshotGcCandidate {
            key,
            commit,
            selection_id: selection_id.into(),
            fingerprint,
            logical_bytes,
            modified_unix_millis,
        }))
    }

    pub(crate) fn blob_from_plan(
        key: Box<str>,
        fingerprint: BlobDigest,
        logical_bytes: u64,
        modified_unix_millis: u64,
    ) -> Result<Self, crate::HubError> {
        let digest = BlobDigest::parse(&key).map_err(crate::HubError::validation)?;
        Ok(Self::Blob(BlobGcCandidate {
            key,
            digest,
            fingerprint,
            logical_bytes,
            modified_unix_millis,
        }))
    }

    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Partial(candidate) => candidate.key(),
            Self::Snapshot(candidate) => &candidate.key,
            Self::Blob(candidate) => &candidate.key,
        }
    }

    pub(crate) const fn size(&self) -> u64 {
        match self {
            Self::Partial(candidate) => candidate.size(),
            Self::Snapshot(candidate) => candidate.logical_bytes,
            Self::Blob(candidate) => candidate.logical_bytes,
        }
    }

    pub(crate) const fn updated_unix_millis(&self) -> u64 {
        match self {
            Self::Partial(candidate) => candidate.updated_unix_millis(),
            Self::Snapshot(candidate) => candidate.modified_unix_millis,
            Self::Blob(candidate) => candidate.modified_unix_millis,
        }
    }

    pub(crate) const fn commit(&self) -> Option<&CommitId> {
        match self {
            Self::Partial(candidate) => Some(candidate.commit()),
            Self::Snapshot(candidate) => Some(&candidate.commit),
            Self::Blob(_) => None,
        }
    }

    pub(crate) fn selection_id(&self) -> Option<&str> {
        match self {
            Self::Snapshot(candidate) => Some(&candidate.selection_id),
            Self::Partial(_) | Self::Blob(_) => None,
        }
    }

    pub(crate) const fn path(&self) -> Option<&RepoPath> {
        match self {
            Self::Partial(candidate) => Some(candidate.path()),
            Self::Snapshot(_) | Self::Blob(_) => None,
        }
    }

    pub(crate) const fn fingerprint(&self) -> BlobDigest {
        match self {
            Self::Partial(candidate) => candidate.record_digest(),
            Self::Snapshot(candidate) => candidate.fingerprint,
            Self::Blob(candidate) => candidate.fingerprint,
        }
    }
}

fn validate_fixed_digest(value: &str) -> Result<(), crate::HubError> {
    BlobDigest::parse(value)
        .map(|_digest| ())
        .map_err(crate::HubError::validation)
}

fn snapshot_gc_key(commit: &CommitId, selection_id: &str) -> Box<str> {
    let mut hasher = Sha256::new();
    hasher.update(b"hf-store-gc-snapshot\0");
    hasher.update(commit.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(selection_id.as_bytes());
    format!("{:x}", hasher.finalize()).into()
}

const fn gc_observation_rank(observation: &GcObservation) -> u8 {
    match observation {
        GcObservation::Partial(_) => 0,
        GcObservation::Snapshot(_) => 1,
        GcObservation::Blob(_) => 2,
    }
}

fn gc_observation_matches(left: &GcObservation, right: &GcObservation) -> bool {
    gc_observation_rank(left) == gc_observation_rank(right)
        && left.key() == right.key()
        && left.size() == right.size()
        && left.updated_unix_millis() == right.updated_unix_millis()
        && left.fingerprint() == right.fingerprint()
}

fn unix_millis_from_system_time(time: SystemTime) -> Result<u64, CacheError> {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_before_epoch| CacheError::conflicting_record())?
        .as_millis();
    u64::try_from(millis).map_err(|_overflow| CacheError::conflicting_record())
}

fn resolve_relative_target(link: &Path, target: &Path) -> Option<PathBuf> {
    if target.is_absolute() {
        return None;
    }
    let mut components = Vec::new();
    for component in link.parent()?.join(target).components() {
        match component {
            Component::Normal(value) => components.push(value.to_os_string()),
            Component::ParentDir => {
                components.pop()?;
            }
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    Some(components.into_iter().collect())
}

impl PartialGcCandidate {
    pub(crate) fn from_observation(
        key: Box<str>,
        commit: CommitId,
        path: RepoPath,
        record_digest: BlobDigest,
        received_size: u64,
        updated_unix_millis: u64,
    ) -> Self {
        Self {
            key,
            commit,
            path,
            record_digest,
            received_size,
            updated_unix_millis,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }
    pub(crate) const fn size(&self) -> u64 {
        self.received_size
    }
    pub(crate) const fn updated_unix_millis(&self) -> u64 {
        self.updated_unix_millis
    }
    pub(crate) const fn commit(&self) -> &CommitId {
        &self.commit
    }
    pub(crate) const fn path(&self) -> &RepoPath {
        &self.path
    }
    pub(crate) const fn record_digest(&self) -> BlobDigest {
        self.record_digest
    }
}

#[derive(Clone, Debug)]
pub(super) struct OwnedSnapshotRead {
    root: PathBuf,
    files: Vec<OwnedSnapshotFile>,
    lease: Arc<SnapshotLease>,
}

impl OwnedSnapshotRead {
    pub(super) fn into_parts(self) -> (PathBuf, Vec<OwnedSnapshotFile>, Arc<SnapshotLease>) {
        (self.root, self.files, self.lease)
    }
}

impl OwnedSnapshotFile {
    pub(super) const fn path(&self) -> &RepoPath {
        &self.path
    }

    pub(super) fn content_path(&self) -> &Path {
        &self.content_path
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }
}

impl Debug for CachePartialSink {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachePartialSink")
            .finish_non_exhaustive()
    }
}

impl crate::transfer::PartialSink for CachePartialSink {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.writer.sync_all()
    }
}

impl CacheKernel {
    pub(super) fn compatible_inventory_entries(
        &self,
        repository: &Path,
        staging_directory: &Path,
    ) -> Result<Vec<CacheInventoryEntry>, CacheError> {
        let blobs = self.compatible_blob_digests(&repository.join("blobs"))?;
        let mut entries = Vec::new();
        for namespace in ["refs", "trees", "blobs", "snapshots", ".no_exist"] {
            self.walk_compatible_inventory(
                &repository.join(namespace),
                repository,
                namespace,
                &blobs,
                &mut entries,
            )?;
        }
        self.walk_compatible_inventory(
            self.relative_path(staging_directory)?,
            repository,
            "staging",
            &blobs,
            &mut entries,
        )?;
        entries.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Ok(entries)
    }

    fn compatible_blob_digests(
        &self,
        directory: &Path,
    ) -> Result<BTreeSet<BlobDigest>, CacheError> {
        let entries = match self.root.read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
            Err(error) => return Err(error.into()),
        };
        let mut digests = BTreeSet::new();
        for path in entries {
            let name = path.file_name().and_then(std::ffi::OsStr::to_str);
            if self.root.entry_kind(&path)? == RootedEntryKind::RegularFile
                && !name.is_some_and(|value| value.ends_with(".incomplete"))
            {
                digests.insert(self.hash_regular_file(&path)?);
            }
        }
        Ok(digests)
    }

    fn walk_compatible_inventory(
        &self,
        directory: &Path,
        repository: &Path,
        namespace: &str,
        blob_digests: &BTreeSet<BlobDigest>,
        entries: &mut Vec<CacheInventoryEntry>,
    ) -> Result<(), CacheError> {
        let children = match self.root.read_dir(directory) {
            Ok(children) => children,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for child in children {
            let kind = self.root.entry_kind(&child)?;
            let (metadata_state, semantic) = self.compatible_inventory_classification(
                &child,
                repository,
                namespace,
                kind,
                blob_digests,
            )?;
            entries.push(CacheInventoryEntry {
                relative_path: child.to_string_lossy().replace('\\', "/").into(),
                namespace: if namespace == "blobs"
                    && child
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .is_some_and(|name| name.ends_with(".incomplete"))
                {
                    "partials".into()
                } else {
                    namespace.into()
                },
                kind,
                metadata_state,
                semantic,
            });
            if kind == RootedEntryKind::Directory {
                self.walk_compatible_inventory(
                    &child,
                    repository,
                    namespace,
                    blob_digests,
                    entries,
                )?;
            }
        }
        Ok(())
    }

    fn compatible_inventory_classification(
        &self,
        path: &Path,
        repository: &Path,
        namespace: &str,
        kind: RootedEntryKind,
        blob_digests: &BTreeSet<BlobDigest>,
    ) -> Result<(CacheInventoryMetadataState, CacheInventorySemantic), CacheError> {
        if kind == RootedEntryKind::Other && namespace == "snapshots" {
            let valid = self
                .root
                .read_link(path)
                .ok()
                .and_then(|target| resolve_relative_target(path, &target))
                .is_some_and(|target| {
                    target.starts_with(repository.join("blobs"))
                        && self
                            .root
                            .entry_kind(&target)
                            .is_ok_and(|kind| kind == RootedEntryKind::RegularFile)
                });
            return Ok((
                CacheInventoryMetadataState::Recognized,
                if valid {
                    CacheInventorySemantic::RelativeSymlink
                } else {
                    CacheInventorySemantic::Ordinary
                },
            ));
        }
        if kind != RootedEntryKind::RegularFile {
            return Ok((
                CacheInventoryMetadataState::Recognized,
                CacheInventorySemantic::Ordinary,
            ));
        }
        let metadata = match namespace {
            "refs" => match self.root.read_regular_bounded(path, 64)?.bytes() {
                Some(bytes) if super::hub_metadata::decode_ref(&bytes).is_ok() => {
                    CacheInventoryMetadataState::Recognized
                }
                Some(_) | None => CacheInventoryMetadataState::Corrupt,
            },
            "trees" => match self
                .root
                .read_regular_bounded(path, 64 * 1024 * 1024)?
                .bytes()
                .as_deref()
                .map(super::hub_metadata::decode_tree)
            {
                Some(Ok(_tree)) => CacheInventoryMetadataState::Recognized,
                Some(Err(error)) if error.is_unknown_version() => {
                    CacheInventoryMetadataState::Unsupported
                }
                Some(Err(_)) | None => CacheInventoryMetadataState::Corrupt,
            },
            _ => CacheInventoryMetadataState::Recognized,
        };
        let semantic = if namespace == "snapshots" {
            if blob_digests.contains(&self.hash_regular_file(path)?) {
                CacheInventorySemantic::CopiedWithBlob
            } else {
                CacheInventorySemantic::SnapshotOnly
            }
        } else {
            CacheInventorySemantic::Ordinary
        };
        Ok((metadata, semantic))
    }

    fn hash_regular_file(&self, path: &Path) -> Result<BlobDigest, CacheError> {
        let RootedRegularFile::File { mut reader, .. } = self.root.open_regular(path)? else {
            return Err(CacheError::conflicting_record());
        };
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; COPY_BUFFER_SIZE].into_boxed_slice();
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(BlobDigest::from_bytes(hasher.finalize().into()))
    }

    pub(super) fn plan_snapshot_gc(
        &self,
        now_unix_millis: u64,
        minimum_age_millis: u64,
        keep_floor: usize,
        retained_commits: &[Box<str>],
    ) -> Result<Vec<GcObservation>, CacheError> {
        let mut roots = retained_commits
            .iter()
            .map(CommitId::parse)
            .collect::<Result<BTreeSet<_>, _>>()?;
        self.scan_ref_roots(&mut roots)?;
        let mut snapshots = self.scan_owned_snapshots()?;
        snapshots.sort_unstable_by(|left, right| {
            right
                .modified_unix_millis
                .cmp(&left.modified_unix_millis)
                .then_with(|| left.key.cmp(&right.key))
        });

        let mut retained_digests = BTreeSet::new();
        let mut candidates = Vec::new();
        let mut detached_seen = 0_usize;
        for snapshot in snapshots {
            let rooted = roots.contains(&snapshot.commit);
            let kept_by_floor = if rooted {
                false
            } else {
                detached_seen = detached_seen.saturating_add(1);
                detached_seen <= keep_floor
            };
            let old_enough =
                now_unix_millis.saturating_sub(snapshot.modified_unix_millis) >= minimum_age_millis;
            if rooted || kept_by_floor || !old_enough {
                retained_digests.extend(snapshot.digests.iter().copied());
            } else {
                candidates.push(GcObservation::Snapshot(snapshot.candidate));
            }
        }

        for blob in self.scan_owned_blobs()? {
            if !retained_digests.contains(&blob.digest)
                && now_unix_millis.saturating_sub(blob.modified_unix_millis) >= minimum_age_millis
            {
                candidates.push(GcObservation::Blob(blob));
            }
        }
        candidates.sort_unstable_by(|left, right| {
            gc_observation_rank(left)
                .cmp(&gc_observation_rank(right))
                .then_with(|| left.key().cmp(right.key()))
        });
        Ok(candidates)
    }

    fn scan_ref_roots(&self, roots: &mut BTreeSet<CommitId>) -> Result<(), CacheError> {
        let directory = self
            .relative_path(&self.layout.repository_directory().join("refs"))?
            .to_path_buf();
        let entries = match self.root.read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for path in entries {
            if self.root.entry_kind(&path)? != RootedEntryKind::RegularFile {
                return Err(CacheError::conflicting_record());
            }
            let Some(bytes) = self
                .root
                .read_regular_bounded(&path, MAX_SMALL_RECORD_BYTES)?
                .bytes()
            else {
                return Err(CacheError::conflicting_record());
            };
            let record = decode_record::<RefRecord>(&bytes)?;
            roots.insert(CommitId::parse(record.commit())?);
        }
        Ok(())
    }

    fn scan_owned_snapshots(&self) -> Result<Vec<ValidatedGcSnapshot>, CacheError> {
        let directory = self
            .relative_path(&self.layout.repository_directory().join("snapshots"))?
            .to_path_buf();
        let entries = match self.root.read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        entries
            .into_iter()
            .map(|path| self.validate_gc_snapshot(&path))
            .collect()
    }

    fn validate_gc_snapshot(&self, directory: &Path) -> Result<ValidatedGcSnapshot, CacheError> {
        if self.root.entry_kind(directory)? != RootedEntryKind::Directory {
            return Err(CacheError::conflicting_record());
        }
        let Some(name) = directory.file_name().and_then(std::ffi::OsStr::to_str) else {
            return Err(CacheError::conflicting_record());
        };
        let Some((commit_text, selection_text)) = name.split_once('-') else {
            return Err(CacheError::conflicting_record());
        };
        let commit = CommitId::parse(commit_text)?;
        let selection = SelectionId::parse(selection_text)?;
        let manifest_path = directory.join("manifest.json");
        let RootedRegularFile::File {
            mut reader,
            size: manifest_size,
            modified,
        } = self.root.open_regular(&manifest_path)?
        else {
            return Err(CacheError::conflicting_record());
        };
        if manifest_size > u64::try_from(MAX_MANIFEST_RECORD_BYTES).map_err(io::Error::other)? {
            return Err(CacheError::conflicting_record());
        }
        let mut manifest_bytes = Vec::new();
        reader
            .by_ref()
            .take(manifest_size.saturating_add(1))
            .read_to_end(&mut manifest_bytes)?;
        if u64::try_from(manifest_bytes.len()).map_err(io::Error::other)? != manifest_size {
            return Err(CacheError::conflicting_record());
        }
        let manifest = decode_record::<SnapshotManifestRecord>(&manifest_bytes)?;
        if manifest.commit() != commit.as_str() || manifest.selection_id() != selection.to_string()
        {
            return Err(CacheError::conflicting_record());
        }
        let mut paths = BTreeSet::from([manifest_path.clone()]);
        let mut digests = BTreeSet::new();
        let mut logical_bytes = manifest_size;
        let mut newest = unix_millis_from_system_time(modified)?;
        let mut fingerprint = Sha256::new();
        fingerprint.update(&manifest_bytes);
        for file in manifest.files() {
            let path = RepoPath::parse(file.path())?;
            let digest = file.digest()?;
            if file.hub_blob_key()?.is_some() {
                return Err(CacheError::conflicting_record());
            }
            let snapshot_path = self
                .relative_path(&self.layout.snapshot_file(&commit, &selection, &path))?
                .to_path_buf();
            let RootedRegularFile::File { modified, .. } =
                self.root.open_regular(&snapshot_path)?
            else {
                return Err(CacheError::conflicting_record());
            };
            validate_existing_blob(self.root.as_ref(), &snapshot_path, file.size(), digest)?;
            let absolute_blob_path = self.layout.blob_path(&digest);
            let blob_path = self.relative_path(&absolute_blob_path)?;
            validate_existing_blob(self.root.as_ref(), blob_path, file.size(), digest)?;
            newest = newest.max(unix_millis_from_system_time(modified)?);
            logical_bytes = logical_bytes.saturating_add(file.size());
            fingerprint.update(path.as_str().as_bytes());
            fingerprint.update(digest.to_string().as_bytes());
            fingerprint.update(file.size().to_be_bytes());
            paths.insert(snapshot_path);
            digests.insert(digest);
        }
        let actual_paths = self.collect_snapshot_files(directory)?;
        if actual_paths != paths {
            return Err(CacheError::conflicting_record());
        }
        for path in &actual_paths {
            fingerprint.update(path.to_string_lossy().as_bytes());
        }
        let fingerprint = BlobDigest::from_bytes(fingerprint.finalize().into());
        Ok(ValidatedGcSnapshot {
            candidate: SnapshotGcCandidate {
                key: snapshot_gc_key(&commit, selection_text),
                commit: commit.clone(),
                selection_id: selection_text.into(),
                fingerprint,
                logical_bytes,
                modified_unix_millis: newest,
            },
            commit,
            key: snapshot_gc_key(&CommitId::parse(commit_text)?, selection_text),
            digests,
            modified_unix_millis: newest,
        })
    }

    fn collect_snapshot_files(&self, directory: &Path) -> Result<BTreeSet<PathBuf>, CacheError> {
        let mut pending = vec![directory.to_path_buf()];
        let mut files = BTreeSet::new();
        while let Some(current) = pending.pop() {
            for child in self.root.read_dir(&current)? {
                match self.root.entry_kind(&child)? {
                    RootedEntryKind::Directory => pending.push(child),
                    RootedEntryKind::RegularFile => {
                        files.insert(child);
                    }
                    RootedEntryKind::Missing | RootedEntryKind::Other => {
                        return Err(CacheError::conflicting_record());
                    }
                }
            }
        }
        Ok(files)
    }

    fn scan_owned_blobs(&self) -> Result<Vec<BlobGcCandidate>, CacheError> {
        let root = self
            .relative_path(&self.layout.repository_directory().join("blobs/sha256"))?
            .to_path_buf();
        let prefixes = match self.root.read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut blobs = Vec::new();
        for prefix_path in prefixes {
            if self.root.entry_kind(&prefix_path)? != RootedEntryKind::Directory {
                return Err(CacheError::conflicting_record());
            }
            let Some(prefix) = prefix_path.file_name().and_then(std::ffi::OsStr::to_str) else {
                return Err(CacheError::conflicting_record());
            };
            for path in self.root.read_dir(&prefix_path)? {
                let Some(suffix) = path.file_name().and_then(std::ffi::OsStr::to_str) else {
                    return Err(CacheError::conflicting_record());
                };
                let digest = BlobDigest::parse(&format!("{prefix}{suffix}"))?;
                let RootedRegularFile::File { size, modified, .. } =
                    self.root.open_regular(&path)?
                else {
                    return Err(CacheError::conflicting_record());
                };
                validate_existing_blob(self.root.as_ref(), &path, size, digest)?;
                let modified = unix_millis_from_system_time(modified)?;
                let mut hasher = Sha256::new();
                hasher.update(digest.to_string().as_bytes());
                hasher.update(size.to_be_bytes());
                hasher.update(modified.to_be_bytes());
                blobs.push(BlobGcCandidate {
                    key: digest.to_string().into(),
                    digest,
                    fingerprint: BlobDigest::from_bytes(hasher.finalize().into()),
                    logical_bytes: size,
                    modified_unix_millis: modified,
                });
            }
        }
        Ok(blobs)
    }

    pub(super) fn execute_snapshot_gc(
        &self,
        candidate: &SnapshotGcCandidate,
        now_unix_millis: u64,
        minimum_age_millis: u64,
        keep_floor: usize,
        retained_commits: &[Box<str>],
    ) -> Result<bool, CacheError> {
        let maintenance = self.layout.maintenance_lock();
        self.ensure_parent(&maintenance)?;
        let _maintenance_guard = self
            .root
            .lock_exclusive(self.relative_path(&maintenance)?)?;
        let selection = SelectionId::parse(&candidate.selection_id)?;
        let snapshot_lock = self.layout.snapshot_lock(&candidate.commit, &selection);
        self.ensure_parent(&snapshot_lock)?;
        let _snapshot_guard = match self
            .root
            .try_lock_exclusive(self.relative_path(&snapshot_lock)?)?
        {
            RootedLockAttempt::Acquired(guard) => guard,
            RootedLockAttempt::Contended => return Ok(false),
        };
        let _lease_guard =
            match self.try_snapshot_maintenance_lease(&candidate.commit, &selection)? {
                RootedLockAttempt::Acquired(guard) => guard,
                RootedLockAttempt::Contended => return Ok(false),
            };
        let fresh = self.plan_snapshot_gc(
            now_unix_millis,
            minimum_age_millis,
            keep_floor,
            retained_commits,
        )?;
        if !fresh.iter().any(|observation| {
            matches!(observation, GcObservation::Snapshot(_))
                && gc_observation_matches(observation, &GcObservation::Snapshot(candidate.clone()))
        }) {
            return Ok(false);
        }
        let source = self
            .relative_path(
                &self
                    .layout
                    .snapshot_directory(&candidate.commit, &selection),
            )?
            .to_path_buf();
        self.quarantine_directory(
            &source,
            GcTombstoneKind::Snapshot,
            &candidate.key,
            candidate.fingerprint,
            candidate.logical_bytes,
            candidate.modified_unix_millis,
        )?;
        Ok(true)
    }

    pub(super) fn execute_blob_gc(
        &self,
        candidate: &BlobGcCandidate,
        now_unix_millis: u64,
        minimum_age_millis: u64,
        keep_floor: usize,
        retained_commits: &[Box<str>],
    ) -> Result<bool, CacheError> {
        let maintenance = self.layout.maintenance_lock();
        self.ensure_parent(&maintenance)?;
        let _maintenance_guard = self
            .root
            .lock_exclusive(self.relative_path(&maintenance)?)?;
        let blob_lock = self.layout.blob_lock(&candidate.digest);
        self.ensure_parent(&blob_lock)?;
        let _blob_guard = match self
            .root
            .try_lock_exclusive(self.relative_path(&blob_lock)?)?
        {
            RootedLockAttempt::Acquired(guard) => guard,
            RootedLockAttempt::Contended => return Ok(false),
        };
        let fresh = self.plan_snapshot_gc(
            now_unix_millis,
            minimum_age_millis,
            keep_floor,
            retained_commits,
        )?;
        if !fresh.iter().any(|observation| {
            matches!(observation, GcObservation::Blob(_))
                && gc_observation_matches(observation, &GcObservation::Blob(candidate.clone()))
        }) {
            return Ok(false);
        }
        let source = self
            .relative_path(&self.layout.blob_path(&candidate.digest))?
            .to_path_buf();
        self.quarantine_file(
            &source,
            GcTombstoneKind::Blob,
            &candidate.key,
            candidate.fingerprint,
            candidate.logical_bytes,
            candidate.modified_unix_millis,
        )?;
        Ok(true)
    }

    fn quarantine_directory(
        &self,
        source: &Path,
        kind: GcTombstoneKind,
        key: &str,
        fingerprint: BlobDigest,
        logical_bytes: u64,
        observed_unix_millis: u64,
    ) -> Result<(), CacheError> {
        let (tombstone, payload) = self.prepare_gc_quarantine(
            kind,
            key,
            fingerprint,
            logical_bytes,
            observed_unix_millis,
            "directory",
        )?;
        self.root.rename_entry(source, &payload)?;
        if self.remove_tree_nofollow(&payload).is_ok() {
            let _cleanup_result = self.root.remove_file(&tombstone);
        }
        Ok(())
    }

    fn quarantine_file(
        &self,
        source: &Path,
        kind: GcTombstoneKind,
        key: &str,
        fingerprint: BlobDigest,
        logical_bytes: u64,
        observed_unix_millis: u64,
    ) -> Result<(), CacheError> {
        let (tombstone, payload) = self.prepare_gc_quarantine(
            kind,
            key,
            fingerprint,
            logical_bytes,
            observed_unix_millis,
            "file",
        )?;
        self.root.rename_entry(source, &payload)?;
        if self.root.remove_file(&payload).is_ok() {
            let _cleanup_result = self.root.remove_file(&tombstone);
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the quarantine identity and observation are intentionally explicit"
    )]
    fn prepare_gc_quarantine(
        &self,
        kind: GcTombstoneKind,
        key: &str,
        fingerprint: BlobDigest,
        logical_bytes: u64,
        observed_unix_millis: u64,
        payload_suffix: &str,
    ) -> Result<(PathBuf, PathBuf), CacheError> {
        let trash = self.layout.trash_directory();
        self.root.ensure_dir(self.relative_path(&trash)?)?;
        let operation = self.effects.operation_ids.next()?;
        let tombstone =
            GcTombstoneRecord::new(kind, key, fingerprint, logical_bytes, observed_unix_millis)?;
        let tombstone_path = self
            .relative_path(&trash.join(format!("gc-{operation}.tombstone.json")))?
            .to_path_buf();
        let payload = self
            .relative_path(&trash.join(format!("gc-{operation}.{payload_suffix}")))?
            .to_path_buf();
        let mut writer = self.root.create_new(&tombstone_path)?;
        writer.write_all(&encode_record(&tombstone)?)?;
        writer.sync_all()?;
        Ok((tombstone_path, payload))
    }

    fn remove_tree_nofollow(&self, directory: &Path) -> Result<(), CacheError> {
        for child in self.root.read_dir(directory)? {
            match self.root.entry_kind(&child)? {
                RootedEntryKind::Directory => self.remove_tree_nofollow(&child)?,
                RootedEntryKind::RegularFile => self.root.remove_file(&child)?,
                RootedEntryKind::Missing => {}
                RootedEntryKind::Other => return Err(CacheError::conflicting_record()),
            }
        }
        self.root.remove_dir(directory)?;
        Ok(())
    }

    pub(super) fn plan_partial_gc(
        &self,
        now_unix_millis: u64,
        minimum_age_millis: u64,
    ) -> Result<Vec<PartialGcCandidate>, CacheError> {
        let partial_directory_path = self.layout.repository_directory().join("partials");
        let partial_directory = self.relative_path(&partial_directory_path)?;
        let entries = match self.root.read_dir(partial_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut candidates = Vec::new();
        for record_path in entries {
            let Some(name) = record_path.file_name().and_then(std::ffi::OsStr::to_str) else {
                continue;
            };
            if !has_json_extension(name) {
                continue;
            }
            let Some(bytes) = self
                .root
                .read_regular_bounded(&record_path, MAX_SMALL_RECORD_BYTES)?
                .bytes()
            else {
                continue;
            };
            let Ok(record) = decode_record::<PartialTransferRecord>(&bytes) else {
                continue;
            };
            let commit = record.commit()?;
            let path = record.path()?;
            let expected_record_path = self.layout.partial_record(&commit, &path)?;
            let expected_record = self.relative_path(&expected_record_path)?;
            if expected_record != record_path {
                continue;
            }
            let data_path = self
                .relative_path(&self.layout.partial_data(&commit, &path)?)?
                .to_path_buf();
            let RootedRegularFile::File { size, .. } = self.root.open_regular(&data_path)? else {
                continue;
            };
            if size != record.received_size()
                || now_unix_millis.saturating_sub(record.updated_unix_millis()) < minimum_age_millis
            {
                continue;
            }
            candidates.push(PartialGcCandidate::from_observation(
                name.trim_end_matches(".json").into(),
                commit,
                path,
                BlobDigest::for_bytes(&bytes),
                size,
                record.updated_unix_millis(),
            ));
        }
        candidates.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        Ok(candidates)
    }

    pub(super) fn execute_partial_gc(
        &self,
        candidate: &PartialGcCandidate,
        now_unix_millis: u64,
        minimum_age_millis: u64,
    ) -> Result<bool, CacheError> {
        let maintenance = self.layout.maintenance_lock();
        self.ensure_parent(&maintenance)?;
        let _maintenance_guard = self
            .root
            .lock_exclusive(self.relative_path(&maintenance)?)?;
        let partial_lock = self
            .layout
            .partial_lock(&candidate.commit, &candidate.path)?;
        self.ensure_parent(&partial_lock)?;
        let _partial_guard = self
            .root
            .lock_exclusive(self.relative_path(&partial_lock)?)?;
        let record_path = self
            .relative_path(
                &self
                    .layout
                    .partial_record(&candidate.commit, &candidate.path)?,
            )?
            .to_path_buf();
        let data_path = self
            .relative_path(
                &self
                    .layout
                    .partial_data(&candidate.commit, &candidate.path)?,
            )?
            .to_path_buf();
        let Some(bytes) = self
            .root
            .read_regular_bounded(&record_path, MAX_SMALL_RECORD_BYTES)?
            .bytes()
        else {
            return Ok(false);
        };
        let Ok(record) = decode_record::<PartialTransferRecord>(&bytes) else {
            return Ok(false);
        };
        let RootedRegularFile::File { size, .. } = self.root.open_regular(&data_path)? else {
            return Ok(false);
        };
        if BlobDigest::for_bytes(&bytes) != candidate.record_digest
            || size != candidate.received_size
            || record.updated_unix_millis() != candidate.updated_unix_millis
            || now_unix_millis.saturating_sub(record.updated_unix_millis()) < minimum_age_millis
        {
            return Ok(false);
        }
        let trash = self.layout.trash_directory();
        self.root.ensure_dir(self.relative_path(&trash)?)?;
        let operation = self.effects.operation_ids.next()?;
        let tombstone = PartialGcTombstoneRecord::new(
            candidate.key(),
            &candidate.commit,
            &candidate.path,
            candidate.record_digest,
            candidate.received_size,
            candidate.updated_unix_millis,
        )?;
        let tombstone_path = trash.join(format!("partial-{operation}.tombstone.json"));
        let tombstone_path = self.relative_path(&tombstone_path)?;
        let mut tombstone_writer = self.root.create_new(tombstone_path)?;
        tombstone_writer.write_all(&encode_record(&tombstone)?)?;
        tombstone_writer.sync_all()?;
        let record_trash = trash.join(format!(
            "partial-{operation}-{}.record.json",
            candidate.record_digest
        ));
        let data_trash = trash.join(format!(
            "partial-{operation}-{}.data",
            candidate.record_digest
        ));
        let record_trash = self.relative_path(&record_trash)?;
        let data_trash = self.relative_path(&data_trash)?;
        self.root.rename_entry(&record_path, record_trash)?;
        if let Err(error) = self.root.rename_entry(&data_path, data_trash) {
            return Err(error.into());
        }
        self.root.remove_file(record_trash)?;
        self.root.remove_file(data_trash)?;
        self.root.remove_file(tombstone_path)?;
        Ok(true)
    }
    pub(super) fn inventory_entries(&self) -> Result<Vec<CacheInventoryEntry>, CacheError> {
        let repository_path = self.layout.repository_directory();
        let repository = self.relative_path(&repository_path)?;
        let mut entries = Vec::new();
        for namespace in [
            "refs",
            "trees",
            "blobs",
            "snapshots",
            "partials",
            "staging",
            "trash",
        ] {
            self.walk_inventory(&repository.join(namespace), namespace, &mut entries)?;
        }
        entries.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Ok(entries)
    }

    fn walk_inventory(
        &self,
        directory: &Path,
        namespace: &str,
        entries: &mut Vec<CacheInventoryEntry>,
    ) -> Result<(), CacheError> {
        let children = match self.root.read_dir(directory) {
            Ok(children) => children,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for child in children {
            let kind = self.root.entry_kind(&child)?;
            entries.push(CacheInventoryEntry {
                relative_path: child.to_string_lossy().replace('\\', "/").into(),
                namespace: namespace.into(),
                kind,
                metadata_state: self.inventory_metadata_state(&child, namespace, kind)?,
                semantic: CacheInventorySemantic::Ordinary,
            });
            if kind == RootedEntryKind::Directory {
                self.walk_inventory(&child, namespace, entries)?;
            }
        }
        Ok(())
    }

    fn inventory_metadata_state(
        &self,
        path: &Path,
        namespace: &str,
        kind: RootedEntryKind,
    ) -> Result<CacheInventoryMetadataState, CacheError> {
        if kind != RootedEntryKind::RegularFile {
            return Ok(CacheInventoryMetadataState::Recognized);
        }
        let name = path.file_name().and_then(std::ffi::OsStr::to_str);
        let state = match (namespace, name) {
            ("refs", Some(name)) if has_json_extension(name) => {
                Self::decode_inventory::<RefRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_SMALL_RECORD_BYTES)?,
                )
            }
            ("trees", Some(name)) if has_json_extension(name) => {
                Self::decode_inventory::<super::metadata::RemoteTreeRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_MANIFEST_RECORD_BYTES)?,
                )
            }
            ("snapshots", Some("manifest.json")) => {
                Self::decode_inventory::<SnapshotManifestRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_MANIFEST_RECORD_BYTES)?,
                )
            }
            ("partials", Some(name)) if has_json_extension(name) => {
                Self::decode_inventory::<PartialTransferRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_SMALL_RECORD_BYTES)?,
                )
            }
            ("trash", Some(name))
                if name.starts_with("partial-") && name.ends_with(".tombstone.json") =>
            {
                Self::decode_inventory::<PartialGcTombstoneRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_SMALL_RECORD_BYTES)?,
                )
            }
            ("trash", Some(name))
                if name.starts_with("gc-") && name.ends_with(".tombstone.json") =>
            {
                Self::decode_inventory::<GcTombstoneRecord>(
                    self.root
                        .read_regular_bounded(path, MAX_SMALL_RECORD_BYTES)?,
                )
            }
            _ => return Ok(CacheInventoryMetadataState::Recognized),
        };
        Ok(state)
    }

    fn decode_inventory<T: CacheRecord>(
        read: super::rooted_fs::RootedRead,
    ) -> CacheInventoryMetadataState {
        match read.bytes().as_deref().map(decode_record::<T>) {
            Some(Ok(_record)) => CacheInventoryMetadataState::Recognized,
            Some(Err(error)) if error.is_unknown_version() => {
                CacheInventoryMetadataState::Unsupported
            }
            Some(Err(_)) | None => CacheInventoryMetadataState::Corrupt,
        }
    }
    pub(super) fn new(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
    ) -> Result<Self, CacheError> {
        let layout = CacheLayout::new(root, endpoint, spec)?;
        let authority = effects.open_cache_authority(layout.capability_root())?;
        Self::with_layout(layout, endpoint, spec, authority.writer(), effects)
    }

    pub(super) fn for_compatible_cache(
        layout: &HubCacheLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    ) -> Result<Self, CacheError> {
        Self::with_layout(
            layout.sidecar().clone(),
            layout.endpoint(),
            layout.repository(),
            root,
            effects,
        )
    }

    fn with_layout(
        layout: CacheLayout,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    ) -> Result<Self, CacheError> {
        let origin_key = OriginKey::derive(endpoint)?;
        let repository_key = RepositoryKey::derive(&origin_key, spec)?;
        Ok(Self {
            layout,
            root,
            origin_record: OriginRecord::new(endpoint),
            repository_record: RepositoryRecord::new(&origin_key, &repository_key, spec),
            effects,
        })
    }

    pub(super) fn initialize(&self) -> Result<(), CacheError> {
        self.root.ensure_dir(self.layout.cache_root_relative())?;
        let format_lock = self.layout.format_lock();
        self.ensure_parent(&format_lock)?;
        let format_lock_relative = self.relative_path(&format_lock)?;
        let _guard = self.root.lock_exclusive(format_lock_relative)?;
        self.ensure_record(&self.layout.format_record(), &FormatRecord::new())?;

        let repository_directory = self.layout.repository_directory();
        let staging_directory = self.layout.staging_directory();
        self.root
            .ensure_dir(self.relative_path(&repository_directory)?)?;
        self.root
            .ensure_dir(self.relative_path(&staging_directory)?)?;
        self.ensure_record(&self.layout.origin_record(), &self.origin_record)?;
        self.ensure_record(&self.layout.repository_record(), &self.repository_record)?;
        Ok(())
    }

    pub(super) fn publish_blob(
        &self,
        mut reader: impl Read,
        expected_size: u64,
        expected_digest: BlobDigest,
    ) -> Result<BlobPublication, CacheError> {
        let operation_id = self.effects.operation_ids.next()?;
        let staging_path = self.layout.staged_blob(&operation_id.to_string());
        self.ensure_parent(&staging_path)?;
        let staging_relative = self.relative_path(&staging_path)?.to_path_buf();
        let mut cleanup = StagingCleanup::inactive(self.root.as_ref(), staging_relative.clone());
        let mut staging_file = self.root.create_new(&staging_relative)?;
        cleanup.activate();
        self.check_fault(PublicationPoint::AfterStagingCreate, false)?;
        let (actual_size, actual_digest) =
            copy_and_hash(&mut reader, staging_file.as_mut(), expected_size)?;
        staging_file.sync_all()?;
        drop(staging_file);
        self.check_fault(PublicationPoint::AfterStagingSync, false)?;

        if actual_size != expected_size {
            return Err(CacheError::size_mismatch(expected_size, actual_size));
        }
        if actual_digest != expected_digest {
            return Err(CacheError::digest_mismatch());
        }

        let destination = self.layout.blob_path(&expected_digest);
        let lock_path = self.layout.blob_lock(&expected_digest);
        self.ensure_parent(&destination)?;
        self.ensure_parent(&lock_path)?;
        let destination_relative = self.relative_path(&destination)?;
        let lock_relative = self.relative_path(&lock_path)?;
        let _guard = self.root.lock_exclusive(lock_relative)?;

        match self.root.entry_kind(destination_relative)? {
            RootedEntryKind::RegularFile => {
                validate_existing_blob(
                    self.root.as_ref(),
                    destination_relative,
                    expected_size,
                    expected_digest,
                )?;
                self.remove_staging_if_present(&staging_relative, false)?;
                cleanup.deactivate();
                return Ok(BlobPublication::new(
                    destination,
                    BlobPublicationOutcome::Reused,
                ));
            }
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                return Err(CacheError::corrupt_existing_blob());
            }
            RootedEntryKind::Missing => {}
        }

        self.check_fault(PublicationPoint::BeforeBlobPublish, false)?;
        let outcome = self
            .root
            .install_staged_create_once(&staging_relative, destination_relative)?;
        match outcome {
            CreateOnceOutcome::Created => {
                validate_existing_blob(
                    self.root.as_ref(),
                    destination_relative,
                    expected_size,
                    expected_digest,
                )
                .map_err(CacheError::with_may_have_published)?;
                self.check_fault(PublicationPoint::AfterBlobPublish, true)?;
                self.remove_staging_if_present(&staging_relative, true)?;
                cleanup.deactivate();
                self.sync_parent(&destination)
                    .map_err(|source| CacheError::io(&source, true))?;
                Ok(BlobPublication::new(
                    destination,
                    BlobPublicationOutcome::Published,
                ))
            }
            CreateOnceOutcome::Existing => {
                validate_existing_blob(
                    self.root.as_ref(),
                    destination_relative,
                    expected_size,
                    expected_digest,
                )?;
                self.remove_staging_if_present(&staging_relative, false)?;
                cleanup.deactivate();
                Ok(BlobPublication::new(
                    destination,
                    BlobPublicationOutcome::Reused,
                ))
            }
        }
    }

    fn remove_staging_if_present(
        &self,
        path: &Path,
        may_have_published: bool,
    ) -> Result<(), CacheError> {
        match self.root.remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CacheError::io(&error, may_have_published)),
        }
    }

    pub(super) fn create_fresh_partial_sink(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<CachePartialSink, CacheError> {
        let destination = self.layout.partial_data(commit, path)?;
        self.ensure_parent(&destination)?;
        let relative = self.relative_path(&destination)?;
        match self.root.entry_kind(relative)? {
            RootedEntryKind::Missing => {}
            RootedEntryKind::RegularFile => self.root.remove_file(relative)?,
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                return Err(CacheError::conflicting_record());
            }
        }
        Ok(CachePartialSink {
            writer: self.root.create_new(relative)?,
        })
    }

    pub(super) fn create_resume_partial_sink(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_offset: u64,
    ) -> Result<CachePartialSink, CacheError> {
        let destination = self.layout.partial_data(commit, path)?;
        let relative = self.relative_path(&destination)?;
        Ok(CachePartialSink {
            writer: self.root.open_append_regular(relative, expected_offset)?,
        })
    }

    pub(super) fn partial_data_path(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<PathBuf, CacheError> {
        Ok(self.layout.partial_data(commit, path)?)
    }

    pub(super) fn lock_partial(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<Box<dyn RootedLockGuard>, CacheError> {
        let lock = self.layout.partial_lock(commit, path)?;
        self.ensure_parent(&lock)?;
        let relative = self.relative_path(&lock)?;
        Ok(self.root.lock_exclusive(relative)?)
    }

    pub(super) fn partial_data_size(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<Option<u64>, CacheError> {
        let destination = self.layout.partial_data(commit, path)?;
        let relative = self.relative_path(&destination)?;
        match self.root.open_regular(relative)? {
            RootedRegularFile::File { size, .. } => Ok(Some(size)),
            RootedRegularFile::Missing => Ok(None),
            RootedRegularFile::Other => Err(CacheError::conflicting_record()),
        }
    }

    pub(super) fn open_partial_reader(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
    ) -> Result<Box<dyn Read + Send>, CacheError> {
        let destination = self.layout.partial_data(commit, path)?;
        let relative = self.relative_path(&destination)?;
        match self.root.open_regular(relative)? {
            RootedRegularFile::File { reader, size, .. } if size == expected_size => Ok(reader),
            RootedRegularFile::Missing
            | RootedRegularFile::Other
            | RootedRegularFile::File { .. } => Err(CacheError::conflicting_record()),
        }
    }

    pub(super) fn discard_partial(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<(), CacheError> {
        for destination in [
            self.layout.partial_data(commit, path)?,
            self.layout.partial_record(commit, path)?,
        ] {
            let relative = self.relative_path(&destination)?;
            match self.root.entry_kind(relative)? {
                RootedEntryKind::Missing => {}
                RootedEntryKind::RegularFile => self.root.remove_file(relative)?,
                RootedEntryKind::Directory | RootedEntryKind::Other => {
                    return Err(CacheError::conflicting_record());
                }
            }
        }
        Ok(())
    }

    pub(super) fn publish_validated_partial(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        expected_digest: BlobDigest,
    ) -> Result<BlobPublication, CacheError> {
        let destination = self.layout.partial_data(commit, path)?;
        let relative = self.relative_path(&destination)?;
        let (reader, actual_size) = match self.root.open_regular(relative)? {
            RootedRegularFile::File { reader, size, .. } => (reader, size),
            RootedRegularFile::Missing | RootedRegularFile::Other => {
                return Err(CacheError::conflicting_record());
            }
        };
        if actual_size != expected_size {
            return Err(CacheError::size_mismatch(expected_size, actual_size));
        }
        self.publish_blob(reader, expected_size, expected_digest)
    }

    pub(super) fn blob_path(&self, digest: &BlobDigest) -> PathBuf {
        self.layout.blob_path(digest)
    }

    pub(super) fn open_blob(
        &self,
        digest: &BlobDigest,
        expected_size: u64,
    ) -> Result<Option<Box<dyn Read + Send>>, CacheError> {
        let path = self.layout.blob_path(digest);
        match self.root.open_regular(self.relative_path(&path)?)? {
            RootedRegularFile::Missing => Ok(None),
            RootedRegularFile::File { reader, size, .. } if size == expected_size => {
                Ok(Some(reader))
            }
            RootedRegularFile::Other | RootedRegularFile::File { .. } => {
                Err(CacheError::corrupt_existing_blob())
            }
        }
    }

    pub(super) fn staging_entries(&self) -> Result<Vec<PathBuf>, CacheError> {
        let directory = self.layout.staging_directory();
        let relative = self.relative_path(&directory)?;
        Ok(self
            .root
            .read_dir(relative)?
            .into_iter()
            .map(|path| self.layout.capability_root().join(path))
            .collect())
    }

    pub(super) fn write_ref(
        &self,
        revision: &Revision,
        commit: &CommitId,
    ) -> Result<(), CacheError> {
        let destination = self.layout.ref_record(revision)?;
        let lock_path = self.layout.ref_lock(revision)?;
        self.ensure_parent(&lock_path)?;
        let _guard = self.root.lock_exclusive(self.relative_path(&lock_path)?)?;
        if matches!(
            self.root.entry_kind(self.relative_path(&destination)?)?,
            RootedEntryKind::Directory | RootedEntryKind::Other
        ) {
            return Err(CacheError::conflicting_record());
        }
        self.replace_record(&destination, &RefRecord::new(revision, commit))
    }

    pub(super) fn publish_remote_tree(
        &self,
        commit: &CommitId,
        tree: &crate::cache::HubTree,
    ) -> Result<(), CacheError> {
        let record = RemoteTreeRecord::from_tree(commit, tree)?;
        self.publish_immutable_record(
            &self.layout.tree_record(commit),
            &self.layout.tree_lock(commit),
            &record,
            MAX_MANIFEST_RECORD_BYTES,
        )
    }

    pub(super) fn read_remote_tree(
        &self,
        commit: &CommitId,
    ) -> Result<crate::cache::HubTree, CacheError> {
        let record: RemoteTreeRecord =
            self.read_record(&self.layout.tree_record(commit), MAX_MANIFEST_RECORD_BYTES)?;
        if record.commit() != commit.as_str() {
            return Err(CacheError::conflicting_record());
        }
        record.tree().map_err(Into::into)
    }

    pub(super) fn read_ref(&self, revision: &Revision) -> Result<CommitId, CacheError> {
        let path = self.layout.ref_record(revision)?;
        let record: RefRecord = self.read_record(&path, MAX_SMALL_RECORD_BYTES)?;
        if record.revision() != revision.as_str() {
            return Err(CacheError::conflicting_record());
        }
        Ok(CommitId::parse(record.commit())?)
    }

    pub(super) fn publish_hub_blob_binding(
        &self,
        hub_blob_key: &HubBlobKey,
        digest: BlobDigest,
        size: u64,
    ) -> Result<(), CacheError> {
        let destination = self.layout.hub_blob_binding_record(hub_blob_key)?;
        let lock_path = self.layout.hub_blob_binding_lock(hub_blob_key)?;
        let record = HubBlobBindingRecord::new(hub_blob_key, digest, size);
        self.publish_immutable_record(&destination, &lock_path, &record, MAX_SMALL_RECORD_BYTES)
    }

    pub(super) fn read_hub_blob_binding(
        &self,
        hub_blob_key: &HubBlobKey,
    ) -> Result<HubBlobBindingRecord, CacheError> {
        let destination = self.layout.hub_blob_binding_record(hub_blob_key)?;
        let record: HubBlobBindingRecord =
            self.read_record(&destination, MAX_SMALL_RECORD_BYTES)?;
        if record.hub_blob_key() != hub_blob_key.as_str() {
            return Err(CacheError::conflicting_record());
        }
        Ok(record)
    }

    pub(super) fn publish_compatible_manifest(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
        files: Vec<SnapshotFileRecord>,
    ) -> Result<(), CacheError> {
        let record = SnapshotManifestRecord::new(commit, selection, files)?;
        let encoded = encode_record_bounded(&record, MAX_MANIFEST_RECORD_BYTES)?;
        self.verify_compatible_manifest_bindings(&record)?;

        let destination = self.layout.snapshot_manifest(commit, selection);
        let lock_path = self.layout.snapshot_lock(commit, selection);
        self.publish_encoded_immutable_record(
            &destination,
            &lock_path,
            &record,
            &encoded,
            MAX_MANIFEST_RECORD_BYTES,
        )
    }

    pub(super) fn read_snapshot_manifest(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<SnapshotManifestRecord, CacheError> {
        let destination = self.layout.snapshot_manifest(commit, selection);
        let record: SnapshotManifestRecord =
            self.read_record(&destination, MAX_MANIFEST_RECORD_BYTES)?;
        if record.commit() != commit.as_str() || record.selection_id() != selection.to_string() {
            return Err(CacheError::conflicting_record());
        }
        Ok(record)
    }

    pub(super) fn publish_owned_snapshot(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
        files: &[(RepoPath, BlobDigest, u64)],
    ) -> Result<OwnedSnapshotRead, CacheError> {
        let lock_path = self.layout.snapshot_lock(commit, selection);
        self.ensure_parent(&lock_path)?;
        let _guard = self.root.lock_exclusive(self.relative_path(&lock_path)?)?;
        let staging_directory = self.layout.staging_directory();
        self.root
            .ensure_dir(self.relative_path(&staging_directory)?)?;
        let mut records = Vec::with_capacity(files.len());
        let mut staged_files = Vec::with_capacity(files.len());
        for (index, (path, digest, size)) in files.iter().enumerate() {
            let source_path = self.layout.blob_path(digest);
            let source = self.relative_path(&source_path)?;
            let operation = self.next_staging_name()?;
            let staging_path =
                staging_directory.join(format!("{operation}-snapshot-{index}.entry"));
            let staging = self.relative_path(&staging_path)?;
            if self.root.stage_regular_hard_link(source, staging).is_err() {
                let _cleanup_result = self.root.remove_file(staging);
                if let Err(error) = self.root.stage_regular_copy(source, staging) {
                    self.cleanup_staged_snapshot(&staged_files);
                    return Err(error.into());
                }
            }
            if let Err(error) = validate_existing_blob(self.root.as_ref(), staging, *size, *digest)
            {
                let _cleanup_result = self.root.remove_file(staging);
                self.cleanup_staged_snapshot(&staged_files);
                return Err(error);
            }
            staged_files.push((path, *digest, *size, staging_path));
            records.push(SnapshotFileRecord::new(path, *digest, *size, None));
        }
        let manifest = SnapshotManifestRecord::new(commit, selection, records)?;
        let encoded = encode_record_bounded(&manifest, MAX_MANIFEST_RECORD_BYTES)?;
        let operation = self.next_staging_name()?;
        let manifest_staging_path = staging_directory.join(format!("{operation}-manifest.entry"));
        let manifest_staging = self.relative_path(&manifest_staging_path)?;
        if let Err(error) = self.root.stage_bytes(manifest_staging, &encoded) {
            self.cleanup_staged_snapshot(&staged_files);
            return Err(error.into());
        }
        for (path, digest, size, staging_path) in &staged_files {
            let destination_path = self.layout.snapshot_file(commit, selection, path);
            self.ensure_parent(&destination_path)?;
            let destination = self.relative_path(&destination_path)?;
            let staging = self.relative_path(staging_path)?;
            self.check_fault(PublicationPoint::BeforeSnapshotEntryPublish, false)?;
            let outcome = self.root.install_staged_create_once(staging, destination)?;
            let _cleanup_result = self.root.remove_file(staging);
            validate_existing_blob(self.root.as_ref(), destination, *size, *digest)?;
            if outcome == CreateOnceOutcome::Created {
                self.sync_parent(&destination_path)?;
            }
            self.check_fault(PublicationPoint::AfterSnapshotEntryPublish, true)?;
        }
        let destination = self.layout.snapshot_manifest(commit, selection);
        match self.root.entry_kind(self.relative_path(&destination)?)? {
            RootedEntryKind::Missing => {
                self.check_fault(PublicationPoint::BeforeSnapshotManifestPublish, false)?;
                let outcome = self.root.install_staged_create_once(
                    manifest_staging,
                    self.relative_path(&destination)?,
                )?;
                if outcome == CreateOnceOutcome::Created {
                    self.sync_parent(&destination)?;
                }
                self.check_fault(PublicationPoint::AfterSnapshotManifestPublish, true)?;
            }
            RootedEntryKind::RegularFile => {
                let _cleanup_result = self.root.remove_file(manifest_staging);
            }
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                let _cleanup_result = self.root.remove_file(manifest_staging);
                return Err(CacheError::conflicting_record());
            }
        }
        let _cleanup_result = self.root.remove_file(manifest_staging);
        if self.read_snapshot_manifest(commit, selection)? != manifest {
            return Err(CacheError::conflicting_record());
        }
        self.open_owned_snapshot(
            &Revision::parse(commit.as_str())?,
            files
                .iter()
                .map(|file| file.0.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        )
    }

    fn cleanup_staged_snapshot(&self, files: &[(&RepoPath, BlobDigest, u64, PathBuf)]) {
        for (_, _, _, path) in files {
            if let Ok(relative) = self.relative_path(path) {
                let _cleanup_result = self.root.remove_file(relative);
            }
        }
    }

    pub(super) fn open_owned_snapshot(
        &self,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> Result<OwnedSnapshotRead, CacheError> {
        let commit = match CommitId::parse(revision.as_str()) {
            Ok(commit) => commit,
            Err(_symbolic) => self.read_ref(revision)?,
        };
        let mut paths = paths.to_vec();
        paths.sort_unstable();
        paths.dedup();
        let selection = SelectionId::derive(&paths)?;
        let lease_path = self.layout.snapshot_lease(&commit, &selection);
        self.ensure_parent(&lease_path)?;
        let lease = Arc::new(SnapshotLease {
            _guard: self.root.lock_shared(self.relative_path(&lease_path)?)?,
        });
        let manifest = self.read_snapshot_manifest(&commit, &selection)?;
        if manifest.files().len() != paths.len() {
            return Err(CacheError::conflicting_record());
        }
        let mut files = Vec::with_capacity(paths.len());
        for (path, record) in paths.iter().zip(manifest.files()) {
            if record.path() != path.as_str() || record.hub_blob_key()?.is_some() {
                return Err(CacheError::conflicting_record());
            }
            let digest = record.digest()?;
            let size = record.size();
            let content_path = self.layout.snapshot_file(&commit, &selection, path);
            let content = self.relative_path(&content_path)?;
            validate_existing_blob(self.root.as_ref(), content, size, digest)?;
            let blob_path = self.layout.blob_path(&digest);
            let blob = self.relative_path(&blob_path)?;
            validate_existing_blob(self.root.as_ref(), blob, size, digest)?;
            files.push(OwnedSnapshotFile {
                path: path.clone(),
                content_path,
                digest,
                size,
            });
        }
        Ok(OwnedSnapshotRead {
            root: self.layout.snapshot_directory(&commit, &selection),
            files,
            lease,
        })
    }

    pub(super) fn acquire_snapshot_lease(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<Arc<SnapshotLease>, CacheError> {
        let lease_path = self.layout.snapshot_lease(commit, selection);
        self.ensure_parent(&lease_path)?;
        Ok(Arc::new(SnapshotLease {
            _guard: self.root.lock_shared(self.relative_path(&lease_path)?)?,
        }))
    }

    pub(super) fn try_snapshot_maintenance_lease(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<super::rooted_fs::RootedLockAttempt, CacheError> {
        let lease_path = self.layout.snapshot_lease(commit, selection);
        self.ensure_parent(&lease_path)?;
        Ok(self
            .root
            .try_lock_exclusive(self.relative_path(&lease_path)?)?)
    }

    pub(super) fn new_partial_record(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        received_size: u64,
        validator: Option<String>,
        target_digest: Option<BlobDigest>,
    ) -> Result<PartialTransferRecord, CacheError> {
        let updated = self.effects.clock.now_unix_millis()?;
        Ok(PartialTransferRecord::new(
            self.layout.origin_key(),
            self.layout.repository_key(),
            commit,
            path,
            expected_size,
            received_size,
            validator,
            target_digest,
            updated,
        )?)
    }

    pub(super) fn persist_partial_record(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        received_size: u64,
        validator: Option<String>,
        target_digest: Option<BlobDigest>,
    ) -> Result<(), CacheError> {
        let record = self.new_partial_record(
            commit,
            path,
            expected_size,
            received_size,
            validator,
            target_digest,
        )?;
        let destination = self.layout.partial_record(commit, path)?;
        self.replace_record(&destination, &record)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "resume lookup compares every immutable transfer identity field"
    )]
    pub(super) fn partial_resume_offset(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        actual_size: u64,
        validator: Option<&str>,
        target_digest: Option<&BlobDigest>,
    ) -> Result<Option<u64>, CacheError> {
        let destination = self.layout.partial_record(commit, path)?;
        let record: PartialTransferRecord =
            self.read_record(&destination, MAX_SMALL_RECORD_BYTES)?;
        if !record.matches_cache_identity(
            self.layout.origin_key(),
            self.layout.repository_key(),
            commit,
            path,
        ) {
            return Ok(None);
        }
        Ok(record.resume_offset_if_eligible(
            self.layout.origin_key(),
            self.layout.repository_key(),
            commit,
            path,
            expected_size,
            actual_size,
            validator,
            target_digest,
        ))
    }

    pub(super) fn partial_resume_candidate(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        target_digest: Option<&BlobDigest>,
    ) -> Result<Option<(u64, Option<String>)>, CacheError> {
        let Some(actual_size) = self.partial_data_size(commit, path)? else {
            return Ok(None);
        };
        let destination = self.layout.partial_record(commit, path)?;
        let record: PartialTransferRecord =
            match self.read_record(&destination, MAX_SMALL_RECORD_BYTES) {
                Ok(record) => record,
                Err(error) if error.is_not_found() => return Ok(None),
                Err(error) => return Err(error),
            };
        let validator = record.validator().map(str::to_owned);
        let eligible = record.resume_offset_if_eligible(
            self.layout.origin_key(),
            self.layout.repository_key(),
            commit,
            path,
            expected_size,
            actual_size,
            validator.as_deref(),
            target_digest,
        );
        Ok(eligible.map(|offset| (offset, validator)))
    }

    fn replace_record<T: CacheRecord>(
        &self,
        destination: &Path,
        record: &T,
    ) -> Result<(), CacheError> {
        let encoded = encode_record_bounded(record, MAX_SMALL_RECORD_BYTES)?;
        self.replace_encoded_record(destination, &encoded)
    }

    fn replace_encoded_record(&self, destination: &Path, encoded: &[u8]) -> Result<(), CacheError> {
        self.ensure_parent(destination)?;
        let relative = self.relative_path(destination)?;
        let staging = self.next_staging_name()?;
        self.check_fault(PublicationPoint::BeforeAtomicReplace, false)?;
        self.root
            .replace(relative, encoded, &staging)
            .map_err(|source| CacheError::io(&source, true))?;
        self.check_fault(PublicationPoint::AfterAtomicReplace, true)?;
        self.sync_parent(destination)
            .map_err(|source| CacheError::io(&source, true))
    }

    fn publish_immutable_record<T>(
        &self,
        destination: &Path,
        lock_path: &Path,
        expected: &T,
        max_bytes: usize,
    ) -> Result<(), CacheError>
    where
        T: CacheRecord + Eq,
    {
        let encoded = encode_record_bounded(expected, max_bytes)?;
        self.publish_encoded_immutable_record(destination, lock_path, expected, &encoded, max_bytes)
    }

    fn publish_encoded_immutable_record<T>(
        &self,
        destination: &Path,
        lock_path: &Path,
        expected: &T,
        encoded: &[u8],
        max_bytes: usize,
    ) -> Result<(), CacheError>
    where
        T: CacheRecord + Eq,
    {
        self.ensure_parent(lock_path)?;
        let lock_relative = self.relative_path(lock_path)?;
        let _guard = self.root.lock_exclusive(lock_relative)?;
        match self.root.entry_kind(self.relative_path(destination)?)? {
            RootedEntryKind::RegularFile => {
                let existing = self.read_record::<T>(destination, max_bytes)?;
                if &existing == expected {
                    Ok(())
                } else {
                    Err(CacheError::conflicting_record())
                }
            }
            RootedEntryKind::Missing => {
                self.publish_encoded_create_once(destination, expected, encoded, max_bytes)
            }
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                Err(CacheError::conflicting_record())
            }
        }
    }

    fn read_record<T: CacheRecord>(
        &self,
        destination: &Path,
        max_bytes: usize,
    ) -> Result<T, CacheError> {
        let relative = self.relative_path(destination)?;
        let bytes = match self.root.open_regular(relative)? {
            RootedRegularFile::File {
                mut reader, size, ..
            } => {
                let max_size = u64::try_from(max_bytes).map_err(io::Error::other)?;
                if size > max_size {
                    return Err(CacheError::record_too_large());
                }
                read_bounded(reader.as_mut(), max_bytes)?
            }
            RootedRegularFile::Missing => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "cache metadata record is missing",
                )
                .into());
            }
            RootedRegularFile::Other => return Err(CacheError::conflicting_record()),
        };
        Ok(decode_record::<T>(&bytes)?)
    }

    fn verify_compatible_manifest_bindings(
        &self,
        manifest: &SnapshotManifestRecord,
    ) -> Result<(), CacheError> {
        for file in manifest.files() {
            let Some(hub_blob_key) = file.hub_blob_key()? else {
                return Err(CacheError::conflicting_record());
            };
            let binding = self.read_hub_blob_binding(&hub_blob_key)?;
            if binding.digest()? != file.digest()? || binding.size() != file.size() {
                return Err(CacheError::conflicting_record());
            }
        }
        Ok(())
    }

    fn ensure_record<T>(&self, destination: &Path, expected: &T) -> Result<(), CacheError>
    where
        T: CacheRecord + Eq,
    {
        match self.root.entry_kind(self.relative_path(destination)?)? {
            RootedEntryKind::RegularFile => {
                let existing = self.read_record::<T>(destination, MAX_SMALL_RECORD_BYTES)?;
                if &existing != expected {
                    return Err(CacheError::conflicting_record());
                }
                Ok(())
            }
            RootedEntryKind::Missing => {
                let encoded = encode_record_bounded(expected, MAX_SMALL_RECORD_BYTES)?;
                self.publish_encoded_create_once(
                    destination,
                    expected,
                    &encoded,
                    MAX_SMALL_RECORD_BYTES,
                )
            }
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                Err(CacheError::conflicting_record())
            }
        }
    }

    fn publish_encoded_create_once<T>(
        &self,
        destination: &Path,
        expected: &T,
        encoded: &[u8],
        max_bytes: usize,
    ) -> Result<(), CacheError>
    where
        T: CacheRecord + Eq,
    {
        self.ensure_parent(destination)?;
        let relative = self.relative_path(destination)?;
        let staging = self.next_staging_name()?;
        self.check_fault(PublicationPoint::BeforeAtomicReplace, false)?;
        let outcome = self
            .root
            .create_once(relative, encoded, &staging)
            .map_err(|source| CacheError::io(&source, true))?;
        match outcome {
            CreateOnceOutcome::Created => {
                self.check_fault(PublicationPoint::AfterAtomicReplace, true)?;
                self.sync_parent(destination)
                    .map_err(|source| CacheError::io(&source, true))
            }
            CreateOnceOutcome::Existing => {
                let actual = self.read_record::<T>(destination, max_bytes)?;
                if &actual == expected {
                    Ok(())
                } else {
                    Err(CacheError::conflicting_record())
                }
            }
        }
    }

    fn relative_path<'a>(&self, path: &'a Path) -> Result<&'a Path, CacheError> {
        path.strip_prefix(self.layout.capability_root())
            .map_err(|_outside| {
                io::Error::other("cache path is outside its retained capability root").into()
            })
    }

    fn ensure_parent(&self, path: &Path) -> Result<(), CacheError> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("cache path has no parent directory"))?;
        self.root.ensure_dir(self.relative_path(parent)?)?;
        Ok(())
    }

    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("cache path has no parent directory"))?;
        let relative = parent
            .strip_prefix(self.layout.capability_root())
            .map_err(|_outside| {
                io::Error::other("cache path is outside its retained capability root")
            })?;
        self.root.sync_directory(relative)
    }

    fn next_staging_name(&self) -> Result<StagingName, CacheError> {
        Ok(self.effects.next_staging_name()?)
    }

    fn check_fault(
        &self,
        point: PublicationPoint,
        may_have_published: bool,
    ) -> Result<(), CacheError> {
        self.effects
            .faults
            .check(point)
            .map_err(|source| CacheError::io(&source, may_have_published))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BlobPublicationOutcome {
    Published,
    Reused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BlobPublication {
    path: PathBuf,
    outcome: BlobPublicationOutcome,
}

impl BlobPublication {
    fn new(path: PathBuf, outcome: BlobPublicationOutcome) -> Self {
        Self { path, outcome }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) const fn outcome(&self) -> BlobPublicationOutcome {
        self.outcome
    }
}

#[derive(Debug)]
pub(super) struct CacheError {
    kind: Box<CacheErrorKind>,
    may_have_published: bool,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum CacheErrorKind {
    Io(SanitizedIo),
    UnsafePath(SanitizedIo),
    Validation(ValidationError),
    Metadata(MetadataError),
    SizeMismatch { expected: u64, actual: u64 },
    DigestMismatch,
    CorruptExistingBlob,
    ConflictingRecord,
    RecordTooLarge,
}

impl CacheError {
    fn new(kind: CacheErrorKind, may_have_published: bool) -> Self {
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
            CacheErrorKind::UnsafePath(source)
        } else {
            CacheErrorKind::Io(source)
        };
        Self::new(kind, may_have_published)
    }

    fn size_mismatch(expected: u64, actual: u64) -> Self {
        Self::new(CacheErrorKind::SizeMismatch { expected, actual }, false)
    }

    fn digest_mismatch() -> Self {
        Self::new(CacheErrorKind::DigestMismatch, false)
    }

    fn corrupt_existing_blob() -> Self {
        Self::new(CacheErrorKind::CorruptExistingBlob, false)
    }

    fn conflicting_record() -> Self {
        Self::new(CacheErrorKind::ConflictingRecord, false)
    }

    fn record_too_large() -> Self {
        Self::new(CacheErrorKind::RecordTooLarge, false)
    }

    fn with_may_have_published(mut self) -> Self {
        self.may_have_published = true;
        self
    }

    pub(super) fn is_size_mismatch(&self) -> bool {
        matches!(self.kind.as_ref(), CacheErrorKind::SizeMismatch { .. })
    }

    pub(super) fn is_digest_mismatch(&self) -> bool {
        matches!(self.kind.as_ref(), CacheErrorKind::DigestMismatch)
    }

    pub(super) fn is_corrupt_existing_blob(&self) -> bool {
        matches!(self.kind.as_ref(), CacheErrorKind::CorruptExistingBlob)
    }

    pub(super) fn is_record_too_large(&self) -> bool {
        matches!(self.kind.as_ref(), CacheErrorKind::RecordTooLarge)
    }

    pub(super) fn is_unsafe(&self) -> bool {
        matches!(self.kind.as_ref(), CacheErrorKind::UnsafePath(_))
            || matches!(
                self.kind.as_ref(),
                CacheErrorKind::Validation(source) if source.is_unsafe_path()
            )
    }

    pub(super) fn is_not_found(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            CacheErrorKind::Io(source) if source.kind() == io::ErrorKind::NotFound
        )
    }

    #[cfg(test)]
    fn io_kind(&self) -> Option<io::ErrorKind> {
        match self.kind.as_ref() {
            CacheErrorKind::Io(source) | CacheErrorKind::UnsafePath(source) => Some(source.kind()),
            CacheErrorKind::Validation(_)
            | CacheErrorKind::Metadata(_)
            | CacheErrorKind::SizeMismatch { .. }
            | CacheErrorKind::DigestMismatch
            | CacheErrorKind::CorruptExistingBlob
            | CacheErrorKind::ConflictingRecord
            | CacheErrorKind::RecordTooLarge => None,
        }
    }

    pub(super) fn is_corrupt_record(&self) -> bool {
        match self.kind.as_ref() {
            CacheErrorKind::Metadata(source) => source.is_corrupt(),
            CacheErrorKind::ConflictingRecord | CacheErrorKind::RecordTooLarge => true,
            CacheErrorKind::Io(_)
            | CacheErrorKind::UnsafePath(_)
            | CacheErrorKind::Validation(_)
            | CacheErrorKind::SizeMismatch { .. }
            | CacheErrorKind::DigestMismatch
            | CacheErrorKind::CorruptExistingBlob => false,
        }
    }

    pub(super) fn is_unsupported_record(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            CacheErrorKind::Metadata(source) if source.is_unknown_version()
        )
    }

    pub(super) const fn may_have_published(&self) -> bool {
        self.may_have_published
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for CacheError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.kind.as_ref() {
            CacheErrorKind::Io(_) => formatter.write_str("cache filesystem operation failed"),
            CacheErrorKind::UnsafePath(_) => formatter.write_str("cache filesystem path is unsafe"),
            CacheErrorKind::Validation(_) => {
                formatter.write_str("cache identity validation failed")
            }
            CacheErrorKind::Metadata(_) => formatter.write_str("cache metadata operation failed"),
            CacheErrorKind::SizeMismatch { expected, actual } => write!(
                formatter,
                "blob size mismatch: expected {expected} bytes but received {actual}"
            ),
            CacheErrorKind::DigestMismatch => formatter.write_str("blob digest mismatch"),
            CacheErrorKind::CorruptExistingBlob => {
                formatter.write_str("existing blob failed validation")
            }
            CacheErrorKind::ConflictingRecord => {
                formatter.write_str("cache metadata conflicts with its keyed location")
            }
            CacheErrorKind::RecordTooLarge => {
                formatter.write_str("cache metadata record exceeds its size limit")
            }
        }
    }
}

impl Error for CacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            CacheErrorKind::Validation(source) => Some(source),
            CacheErrorKind::Metadata(source) => Some(source),
            CacheErrorKind::Io(_)
            | CacheErrorKind::UnsafePath(_)
            | CacheErrorKind::SizeMismatch { .. }
            | CacheErrorKind::DigestMismatch
            | CacheErrorKind::CorruptExistingBlob
            | CacheErrorKind::ConflictingRecord
            | CacheErrorKind::RecordTooLarge => None,
        }
    }
}

impl From<io::Error> for CacheError {
    fn from(source: io::Error) -> Self {
        Self::io(&source, false)
    }
}

impl From<ValidationError> for CacheError {
    fn from(source: ValidationError) -> Self {
        Self::new(CacheErrorKind::Validation(source), false)
    }
}

impl From<MetadataError> for CacheError {
    fn from(source: MetadataError) -> Self {
        Self::new(CacheErrorKind::Metadata(source), false)
    }
}

fn copy_and_hash<W: Write + ?Sized>(
    reader: &mut dyn Read,
    writer: &mut W,
    expected_size: u64,
) -> Result<(u64, BlobDigest), CacheError> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_SIZE].into_boxed_slice();
    loop {
        let read_capacity = bounded_read_capacity(expected_size, size, buffer.len());
        let read = reader.read(&mut buffer[..read_capacity])?;
        if read == 0 {
            break;
        }
        let read = u64::try_from(read).map_err(io::Error::other)?;
        let next_size = size
            .checked_add(read)
            .ok_or_else(|| io::Error::other("blob size overflow"))?;
        if next_size > expected_size {
            return Err(CacheError::size_mismatch(expected_size, next_size));
        }
        let read = usize::try_from(read).map_err(io::Error::other)?;
        writer.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        size = next_size;
    }
    Ok((size, BlobDigest::from_bytes(hasher.finalize().into())))
}

fn hash_reader(reader: &mut dyn Read, expected_size: u64) -> Result<(u64, BlobDigest), CacheError> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_SIZE].into_boxed_slice();
    loop {
        let read_capacity = bounded_read_capacity(expected_size, size, buffer.len());
        let read = reader.read(&mut buffer[..read_capacity])?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).map_err(io::Error::other)?;
        let next_size = size
            .checked_add(read_u64)
            .ok_or_else(|| io::Error::other("blob size overflow"))?;
        if next_size > expected_size {
            return Err(CacheError::corrupt_existing_blob());
        }
        hasher.update(&buffer[..read]);
        size = next_size;
    }
    Ok((size, BlobDigest::from_bytes(hasher.finalize().into())))
}

fn bounded_read_capacity(expected_size: u64, current_size: u64, buffer_size: usize) -> usize {
    let remaining = expected_size.saturating_sub(current_size);
    let probe_size = remaining.saturating_add(1);
    usize::try_from(probe_size).map_or(buffer_size, |capacity| capacity.min(buffer_size))
}

fn validate_existing_blob(
    root: &dyn RootedFileSystem,
    path: &Path,
    expected_size: u64,
    expected_digest: BlobDigest,
) -> Result<(), CacheError> {
    let mut reader = match root.open_regular(path)? {
        RootedRegularFile::File { reader, .. } => reader,
        RootedRegularFile::Missing | RootedRegularFile::Other => {
            return Err(CacheError::corrupt_existing_blob());
        }
    };
    let (actual_size, actual_digest) = hash_reader(reader.as_mut(), expected_size)?;
    if actual_size == expected_size && actual_digest == expected_digest {
        Ok(())
    } else {
        Err(CacheError::corrupt_existing_blob())
    }
}

fn encode_record_bounded<T: CacheRecord>(
    record: &T,
    max_bytes: usize,
) -> Result<Vec<u8>, CacheError> {
    let encoded = encode_record(record)?;
    if encoded.len() > max_bytes {
        Err(CacheError::record_too_large())
    } else {
        Ok(encoded)
    }
}

fn read_bounded(reader: &mut dyn Read, max_bytes: usize) -> Result<Vec<u8>, CacheError> {
    let mut bytes = Vec::new();
    let limit = u64::try_from(max_bytes)
        .map_err(io::Error::other)?
        .saturating_add(1);
    reader.take(limit).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        Err(CacheError::record_too_large())
    } else {
        Ok(bytes)
    }
}

struct StagingCleanup<'a> {
    root: &'a dyn RootedFileSystem,
    path: PathBuf,
    active: bool,
}

impl<'a> StagingCleanup<'a> {
    fn inactive(root: &'a dyn RootedFileSystem, path: PathBuf) -> Self {
        Self {
            root,
            path,
            active: false,
        }
    }

    fn activate(&mut self) {
        self.active = true;
    }

    fn deactivate(&mut self) {
        self.active = false;
    }
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        if self.active {
            let _result = self.root.remove_file(&self.path);
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct FaultController {
    point: std::sync::Mutex<Option<PublicationPoint>>,
}

#[cfg(test)]
impl FaultController {
    pub(super) fn fail_once(&self, point: PublicationPoint) {
        if let Ok(mut configured) = self.point.lock() {
            *configured = Some(point);
        }
    }
}

#[cfg(test)]
impl PublicationFaults for FaultController {
    fn check(&self, point: PublicationPoint) -> io::Result<()> {
        let mut configured = self
            .point
            .lock()
            .map_err(|_poisoned| io::Error::other("fault controller lock poisoned"))?;
        if configured.as_ref() == Some(&point) {
            *configured = None;
            Err(io::Error::other("injected publication failure"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct SequenceOperationIds {
    next: std::sync::atomic::AtomicU64,
    first: u64,
}

#[cfg(test)]
impl SequenceOperationIds {
    pub(super) const fn new(first: u64) -> Self {
        Self {
            next: std::sync::atomic::AtomicU64::new(first),
            first,
        }
    }

    pub(super) fn issued(&self) -> u64 {
        self.next.load(std::sync::atomic::Ordering::Relaxed) - self.first
    }
}

#[cfg(test)]
impl OperationIds for SequenceOperationIds {
    fn next(&self) -> io::Result<OperationId> {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&value.to_be_bytes());
        Ok(OperationId(bytes))
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct FixedClock(u64);

#[cfg(test)]
impl FixedClock {
    pub(super) const fn new(unix_millis: u64) -> Self {
        Self(unix_millis)
    }
}

#[cfg(test)]
impl Clock for FixedClock {
    fn now_unix_millis(&self) -> io::Result<u64> {
        Ok(self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::process::{Child, Command, Output, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use crate::RepositoryId;

    use super::*;

    const FIRST_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const SECOND_COMMIT: &str = "89abcdef0123456789abcdef0123456789abcdef";
    const CROSS_PROCESS_CHILD_TEST: &str =
        "cache::publication::tests::cross_process_blob_publisher_child";
    const CROSS_PROCESS_CHILD_ENV: &str = "HF_STORE_CROSS_PROCESS_BLOB_PUBLISHER";
    const CROSS_PROCESS_ROOT_ENV: &str = "HF_STORE_CROSS_PROCESS_CACHE_ROOT";
    const CROSS_PROCESS_GATE_ENV: &str = "HF_STORE_CROSS_PROCESS_GATE";
    const CROSS_PROCESS_READY_ENV: &str = "HF_STORE_CROSS_PROCESS_READY";
    const CROSS_PROCESS_RESULT_ENV: &str = "HF_STORE_CROSS_PROCESS_RESULT";
    const CROSS_PROCESS_OPERATION_ID_ENV: &str = "HF_STORE_CROSS_PROCESS_OPERATION_ID";
    const CROSS_PROCESS_TIMEOUT: Duration = Duration::from_secs(15);
    const CROSS_PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(5);
    const CROSS_PROCESS_PAYLOAD: &[u8] = b"cross-process concurrent payload";

    #[test]
    fn blob_publication_validates_size_and_digest_before_visibility()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"complete payload";
        let digest = BlobDigest::for_bytes(payload);

        let size_error = fixture
            .kernel
            .publish_blob(
                Cursor::new(payload),
                u64::try_from(payload.len())? + 1,
                digest,
            )
            .expect_err("size mismatch must fail");
        assert!(size_error.is_size_mismatch());
        assert!(!fixture.kernel.blob_path(&digest).try_exists()?);

        let wrong_digest = BlobDigest::for_bytes(b"different payload");
        let digest_error = fixture
            .kernel
            .publish_blob(
                Cursor::new(payload),
                u64::try_from(payload.len())?,
                wrong_digest,
            )
            .expect_err("digest mismatch must fail");
        assert!(digest_error.is_digest_mismatch());
        assert!(!fixture.kernel.blob_path(&wrong_digest).try_exists()?);

        let bytes_read = Arc::new(AtomicUsize::new(0));
        let oversized = vec![b'x'; 1_024];
        let size_error = fixture
            .kernel
            .publish_blob(
                CountingReader::new(oversized, Arc::clone(&bytes_read)),
                3,
                BlobDigest::for_bytes(b"xxx"),
            )
            .expect_err("an oversized reader must be rejected at its first excess byte");
        assert!(size_error.is_size_mismatch());
        assert_eq!(bytes_read.load(Ordering::Acquire), 4);

        Ok(())
    }

    #[test]
    fn arbitrary_reader_errors_are_redacted_without_losing_the_io_kind()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let error = fixture
            .kernel
            .publish_blob(SecretReader, 1, BlobDigest::for_bytes(b"x"))
            .expect_err("accepted a reader failure");

        assert_eq!(error.io_kind(), Some(io::ErrorKind::PermissionDenied));
        assert_secret_absent_from_error_chain(&error);
        Ok(())
    }

    #[test]
    fn partial_records_persist_and_resume_only_for_the_exact_file_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let digest = BlobDigest::for_bytes(b"complete target");
        fixture.kernel.persist_partial_record(
            &commit,
            &path,
            10,
            4,
            Some("etag".to_owned()),
            Some(digest),
        )?;
        assert_eq!(
            fixture.kernel.partial_resume_offset(
                &commit,
                &path,
                10,
                4,
                Some("etag"),
                Some(&digest),
            )?,
            Some(4)
        );
        assert_eq!(
            fixture.kernel.partial_resume_offset(
                &commit,
                &path,
                10,
                3,
                Some("etag"),
                Some(&digest),
            )?,
            None
        );
        assert_eq!(
            fixture.kernel.partial_resume_offset(
                &commit,
                &path,
                10,
                4,
                Some("changed"),
                Some(&digest),
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn staged_blob_failure_is_not_visible_through_normal_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"complete payload";
        let digest = BlobDigest::for_bytes(payload);
        fixture.faults.fail_once(PublicationPoint::AfterStagingSync);

        fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)
            .expect_err("injected staging failure must surface");

        assert!(!fixture.kernel.blob_path(&digest).try_exists()?);
        assert!(fixture.kernel.staging_entries()?.is_empty());

        Ok(())
    }

    #[test]
    fn staging_identifier_collisions_preserve_the_existing_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let next_id = fixture.ids.issued() + 1;
        let staging_path = fixture
            .kernel
            .layout
            .staged_blob(&format!("{next_id:032x}"));
        let existing = b"another publisher's staged bytes";
        std::fs::write(&staging_path, existing)?;
        let payload = b"new payload";

        fixture
            .kernel
            .publish_blob(
                Cursor::new(payload),
                u64::try_from(payload.len())?,
                BlobDigest::for_bytes(payload),
            )
            .expect_err("a colliding operation identifier must fail without deleting data");
        assert_eq!(std::fs::read(staging_path)?, existing);

        Ok(())
    }

    #[test]
    fn successful_blob_publication_exposes_only_validated_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"complete payload";
        let digest = BlobDigest::for_bytes(payload);

        let publication = fixture.kernel.publish_blob(
            Cursor::new(payload),
            u64::try_from(payload.len())?,
            digest,
        )?;

        assert_eq!(publication.outcome(), BlobPublicationOutcome::Published);
        assert_eq!(std::fs::read(publication.path())?, payload);
        assert!(fixture.kernel.staging_entries()?.is_empty());

        Ok(())
    }

    #[test]
    fn snapshot_reader_lease_blocks_exclusive_maintenance_until_last_handle_drops()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"leased snapshot";
        let digest = BlobDigest::for_bytes(payload);
        fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let snapshot = fixture.kernel.publish_owned_snapshot(
            &commit,
            &selection,
            &[(path, digest, payload.len() as u64)],
        )?;
        assert!(matches!(
            fixture
                .kernel
                .try_snapshot_maintenance_lease(&commit, &selection)?,
            RootedLockAttempt::Contended
        ));
        let cloned = snapshot.clone();
        drop(snapshot);
        assert!(matches!(
            fixture
                .kernel
                .try_snapshot_maintenance_lease(&commit, &selection)?,
            RootedLockAttempt::Contended
        ));
        drop(cloned);
        assert!(matches!(
            fixture
                .kernel
                .try_snapshot_maintenance_lease(&commit, &selection)?,
            RootedLockAttempt::Acquired(_)
        ));
        Ok(())
    }

    #[test]
    fn owned_snapshot_stages_every_entry_before_publishing_any_final_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"first complete blob";
        let digest = BlobDigest::for_bytes(payload);
        fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let first = RepoPath::parse("config.json")?;
        let missing = RepoPath::parse("weights/model.bin")?;
        let selection = SelectionId::derive(&[first.clone(), missing.clone()])?;
        let missing_digest = BlobDigest::for_bytes(b"absent");

        fixture
            .kernel
            .publish_owned_snapshot(
                &commit,
                &selection,
                &[
                    (first.clone(), digest, u64::try_from(payload.len())?),
                    (missing, missing_digest, 6),
                ],
            )
            .expect_err("a missing late source unexpectedly published a snapshot");

        assert!(
            !fixture
                .kernel
                .layout
                .snapshot_file(&commit, &selection, &first)
                .exists()
        );
        assert!(
            !fixture
                .kernel
                .layout
                .snapshot_manifest(&commit, &selection)
                .exists()
        );
        assert!(fixture.kernel.staging_entries()?.is_empty());
        Ok(())
    }

    #[test]
    fn owned_snapshot_fault_boundaries_expose_only_incomplete_or_complete_state()
    -> Result<(), Box<dyn std::error::Error>> {
        for point in [
            PublicationPoint::BeforeSnapshotEntryPublish,
            PublicationPoint::AfterSnapshotEntryPublish,
            PublicationPoint::BeforeSnapshotManifestPublish,
            PublicationPoint::AfterSnapshotManifestPublish,
        ] {
            let fixture = Fixture::new()?;
            let payload = b"snapshot boundary";
            let digest = BlobDigest::for_bytes(payload);
            fixture.kernel.publish_blob(
                Cursor::new(payload),
                u64::try_from(payload.len())?,
                digest,
            )?;
            let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
            let path = RepoPath::parse("model.bin")?;
            let selection = SelectionId::derive(std::slice::from_ref(&path))?;
            fixture.faults.fail_once(point);

            fixture
                .kernel
                .publish_owned_snapshot(
                    &commit,
                    &selection,
                    &[(path.clone(), digest, u64::try_from(payload.len())?)],
                )
                .expect_err("injected snapshot boundary unexpectedly returned success");

            let opened = fixture.kernel.open_owned_snapshot(
                &Revision::parse(commit.as_str())?,
                std::slice::from_ref(&path),
            );
            if point == PublicationPoint::AfterSnapshotManifestPublish {
                let complete = opened?;
                assert_eq!(complete.files.len(), 1);
            } else {
                opened.expect_err("an incomplete snapshot became readable");
            }
        }
        Ok(())
    }

    #[test]
    fn competing_blob_publishers_converge_on_one_validated_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let kernel = Arc::new(fixture.kernel);
        let barrier = Arc::new(Barrier::new(3));
        let payload = b"concurrent payload".to_vec();
        let digest = BlobDigest::for_bytes(&payload);
        let mut workers = Vec::new();

        for _ in 0..2 {
            let kernel = Arc::clone(&kernel);
            let barrier = Arc::clone(&barrier);
            let payload = payload.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                let size = u64::try_from(payload.len())
                    .map_err(|overflow| CacheError::from(io::Error::other(overflow)))?;
                kernel.publish_blob(Cursor::new(&payload), size, digest)
            }));
        }
        barrier.wait();

        let mut outcomes = Vec::new();
        for worker in workers {
            let result = worker
                .join()
                .map_err(|_panic| io::Error::other("publisher panicked"))?;
            outcomes.push(result?);
        }
        let published = outcomes
            .iter()
            .filter(|result| result.outcome() == BlobPublicationOutcome::Published)
            .count();
        let reused = outcomes
            .iter()
            .filter(|result| result.outcome() == BlobPublicationOutcome::Reused)
            .count();

        assert_eq!(published, 1);
        assert_eq!(reused, 1);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, payload);

        Ok(())
    }

    #[test]
    fn competing_processes_converge_on_one_validated_blob() -> Result<(), Box<dyn std::error::Error>>
    {
        let fixture = Fixture::new()?;
        let digest = BlobDigest::for_bytes(CROSS_PROCESS_PAYLOAD);
        // Isolate lock and create-once behavior from concurrent directory setup.
        let blob_path = fixture.kernel.blob_path(&digest);
        fixture.kernel.ensure_parent(&blob_path)?;
        let lock_path = fixture.kernel.layout.blob_lock(&digest);
        fixture.kernel.ensure_parent(&lock_path)?;
        let lock_relative = fixture.kernel.relative_path(&lock_path)?;
        drop(fixture.kernel.root.lock_exclusive(lock_relative)?);

        let gate_path = fixture.directory.path().join("publisher-gate.lock");
        let gate_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&gate_path)?;
        fs4::FileExt::lock(&gate_file)?;
        let mut gate = TestGate::new(gate_file);
        let executable = std::env::current_exe()?;
        let mut children = Vec::new();
        let mut ready_paths = Vec::new();
        let mut result_paths = Vec::new();

        for publisher in 1_u64..=2 {
            let ready_path = fixture
                .directory
                .path()
                .join(format!("publisher-{publisher}.ready"));
            let result_path = fixture
                .directory
                .path()
                .join(format!("publisher-{publisher}.result"));
            let child = Command::new(&executable)
                .arg("--exact")
                .arg(CROSS_PROCESS_CHILD_TEST)
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CROSS_PROCESS_CHILD_ENV, "1")
                .env(CROSS_PROCESS_ROOT_ENV, fixture.directory.path())
                .env(CROSS_PROCESS_GATE_ENV, &gate_path)
                .env(CROSS_PROCESS_READY_ENV, &ready_path)
                .env(CROSS_PROCESS_RESULT_ENV, &result_path)
                .env(CROSS_PROCESS_OPERATION_ID_ENV, publisher.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            children.push(ManagedChild::new(child));
            ready_paths.push(ready_path);
            result_paths.push(result_path);
        }

        let readiness_deadline = Instant::now() + CROSS_PROCESS_TIMEOUT;
        wait_for_children_ready(&mut children, &ready_paths, readiness_deadline)?;
        gate.release()?;
        let exit_deadline = Instant::now() + CROSS_PROCESS_TIMEOUT;
        wait_for_children_success(&mut children, exit_deadline)?;

        let mut outcomes = result_paths
            .iter()
            .map(std::fs::read)
            .collect::<io::Result<Vec<_>>>()?;
        outcomes.sort_unstable();
        assert_eq!(outcomes, [b"published".to_vec(), b"reused".to_vec()]);

        assert_eq!(std::fs::read(blob_path)?, CROSS_PROCESS_PAYLOAD);
        assert!(fixture.kernel.staging_entries()?.is_empty());

        Ok(())
    }

    #[test]
    fn cross_process_blob_publisher_child() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os(CROSS_PROCESS_CHILD_ENV).is_none() {
            return Ok(());
        }

        let root = required_child_path(CROSS_PROCESS_ROOT_ENV)?;
        let gate_path = required_child_path(CROSS_PROCESS_GATE_ENV)?;
        let ready_path = required_child_path(CROSS_PROCESS_READY_ENV)?;
        let result_path = required_child_path(CROSS_PROCESS_RESULT_ENV)?;
        let operation_id = std::env::var(CROSS_PROCESS_OPERATION_ID_ENV)?.parse::<u64>()?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(SequenceOperationIds::new(operation_id)),
            Arc::new(FixedClock::new(1_721_596_800_000)),
            Arc::new(NoPublicationFaults),
        );
        let kernel = CacheKernel::new(root, &endpoint, &spec, effects)?;
        let gate_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(gate_path)?;

        std::fs::write(ready_path, b"ready")?;
        lock_shared_until(&gate_file, Instant::now() + CROSS_PROCESS_TIMEOUT)?;

        let digest = BlobDigest::for_bytes(CROSS_PROCESS_PAYLOAD);
        let publication = kernel.publish_blob(
            Cursor::new(CROSS_PROCESS_PAYLOAD),
            u64::try_from(CROSS_PROCESS_PAYLOAD.len())?,
            digest,
        )?;
        let result = match publication.outcome() {
            BlobPublicationOutcome::Published => b"published".as_slice(),
            BlobPublicationOutcome::Reused => b"reused".as_slice(),
        };
        std::fs::write(result_path, result)?;

        Ok(())
    }

    #[test]
    fn ref_replacement_keeps_old_or_new_complete_record_at_failure_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let revision = Revision::parse("main")?;
        let first = CommitId::parse(FIRST_COMMIT)?;
        let second = CommitId::parse(SECOND_COMMIT)?;
        fixture.kernel.write_ref(&revision, &first)?;

        fixture
            .faults
            .fail_once(PublicationPoint::BeforeAtomicReplace);
        fixture
            .kernel
            .write_ref(&revision, &second)
            .expect_err("pre-replace failure must surface");
        assert_eq!(fixture.kernel.read_ref(&revision)?, first);

        fixture
            .faults
            .fail_once(PublicationPoint::AfterAtomicReplace);
        let error = fixture
            .kernel
            .write_ref(&revision, &second)
            .expect_err("post-replace failure must surface");
        assert!(error.may_have_published());
        assert_eq!(fixture.kernel.read_ref(&revision)?, second);

        Ok(())
    }

    #[test]
    fn ref_lookup_rejects_a_record_stored_under_the_wrong_revision_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let requested = Revision::parse("main")?;
        let displaced = Revision::parse("stable")?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        fixture.kernel.write_ref(&requested, &commit)?;
        let path = fixture.kernel.layout.ref_record(&requested)?;
        let bytes = encode_record(&RefRecord::new(&displaced, &commit))?;
        std::fs::write(path, bytes)?;

        fixture
            .kernel
            .read_ref(&requested)
            .expect_err("a ref record must be bound to its requested revision");

        Ok(())
    }

    #[test]
    fn hub_blob_bindings_are_immutable_idempotent_and_reject_non_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let key = HubBlobKey::parse("0123456789abcdef")?;
        let digest = BlobDigest::for_bytes(b"compatible blob");
        let destination = fixture.kernel.layout.hub_blob_binding_record(&key)?;

        fixture.kernel.publish_hub_blob_binding(&key, digest, 15)?;
        let first = std::fs::read(&destination)?;
        fixture.kernel.publish_hub_blob_binding(&key, digest, 15)?;
        assert_eq!(std::fs::read(&destination)?, first);

        fixture
            .kernel
            .publish_hub_blob_binding(&key, BlobDigest::for_bytes(b"different"), 9)
            .expect_err("a conflicting immutable binding must be rejected");
        assert_eq!(std::fs::read(&destination)?, first);

        let non_file_key = HubBlobKey::parse("fedcba9876543210")?;
        let non_file_destination = fixture
            .kernel
            .layout
            .hub_blob_binding_record(&non_file_key)?;
        std::fs::create_dir_all(&non_file_destination)?;
        fixture
            .kernel
            .publish_hub_blob_binding(&non_file_key, digest, 15)
            .expect_err("a non-file binding destination must be rejected");
        assert!(non_file_destination.is_dir());

        Ok(())
    }

    #[test]
    fn hub_blob_binding_fault_boundaries_expose_only_missing_or_complete_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let digest = BlobDigest::for_bytes(b"compatible blob");
        let before_key = HubBlobKey::parse("before-replace")?;
        let before_destination = fixture.kernel.layout.hub_blob_binding_record(&before_key)?;
        fixture
            .faults
            .fail_once(PublicationPoint::BeforeAtomicReplace);
        let before = fixture
            .kernel
            .publish_hub_blob_binding(&before_key, digest, 15)
            .expect_err("the pre-replace fault must surface");
        assert!(!before.may_have_published());
        assert!(!before_destination.try_exists()?);

        let after_key = HubBlobKey::parse("after-replace")?;
        fixture
            .faults
            .fail_once(PublicationPoint::AfterAtomicReplace);
        let after = fixture
            .kernel
            .publish_hub_blob_binding(&after_key, digest, 15)
            .expect_err("the post-replace fault must surface");
        assert!(after.may_have_published());
        let record = fixture.kernel.read_hub_blob_binding(&after_key)?;
        assert_eq!(record.hub_blob_key(), after_key.as_str());
        assert_eq!(record.digest()?, digest);
        assert_eq!(record.size(), 15);

        Ok(())
    }

    #[test]
    fn compatible_manifest_requires_matching_bindings_and_is_published_sorted()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let first_path = RepoPath::parse("config.json")?;
        let second_path = RepoPath::parse("weights/model.bin")?;
        let selection = SelectionId::derive(&[second_path.clone(), first_path.clone()])?;
        let key = HubBlobKey::parse("compatible-key")?;
        let digest = BlobDigest::for_bytes(b"shared bytes");
        let files = vec![
            SnapshotFileRecord::new(&second_path, digest, 12, Some(key.clone())),
            SnapshotFileRecord::new(&first_path, digest, 12, Some(key.clone())),
        ];
        let destination = fixture.kernel.layout.snapshot_manifest(&commit, &selection);

        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, files.clone())
            .expect_err("a compatible manifest must not precede its bindings");
        assert!(!destination.try_exists()?);

        fixture.kernel.publish_hub_blob_binding(&key, digest, 12)?;
        let missing_key_file = SnapshotFileRecord::new(&first_path, digest, 12, None);
        fixture
            .kernel
            .publish_compatible_manifest(
                &commit,
                &SelectionId::derive(std::slice::from_ref(&first_path))?,
                vec![missing_key_file],
            )
            .expect_err("every compatible manifest file must name a Hub blob key");

        let mismatched = SnapshotFileRecord::new(
            &first_path,
            BlobDigest::for_bytes(b"other bytes"),
            11,
            Some(key.clone()),
        );
        fixture
            .kernel
            .publish_compatible_manifest(
                &commit,
                &SelectionId::derive(&[first_path])?,
                vec![mismatched],
            )
            .expect_err("the binding digest and size must match the manifest entry");

        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, files)?;
        let record = fixture.kernel.read_snapshot_manifest(&commit, &selection)?;
        let paths = record
            .files()
            .iter()
            .map(SnapshotFileRecord::path)
            .collect::<Vec<_>>();
        assert_eq!(paths, ["config.json", "weights/model.bin"]);
        assert_eq!(record.commit(), commit.as_str());
        assert_eq!(record.selection_id(), selection.to_string());

        Ok(())
    }

    #[test]
    fn compatible_manifests_are_immutable_idempotent_and_reject_non_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let path = RepoPath::parse("model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let first_key = HubBlobKey::parse("first-key")?;
        let first_digest = BlobDigest::for_bytes(b"first bytes");
        fixture
            .kernel
            .publish_hub_blob_binding(&first_key, first_digest, 11)?;
        let first_files = vec![SnapshotFileRecord::new(
            &path,
            first_digest,
            11,
            Some(first_key),
        )];
        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, first_files.clone())?;
        let destination = fixture.kernel.layout.snapshot_manifest(&commit, &selection);
        let first = std::fs::read(&destination)?;
        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, first_files)?;
        assert_eq!(std::fs::read(&destination)?, first);

        let second_key = HubBlobKey::parse("second-key")?;
        let second_digest = BlobDigest::for_bytes(b"second bytes");
        fixture
            .kernel
            .publish_hub_blob_binding(&second_key, second_digest, 12)?;
        let conflicting_files = vec![SnapshotFileRecord::new(
            &path,
            second_digest,
            12,
            Some(second_key),
        )];
        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, conflicting_files)
            .expect_err("a conflicting immutable manifest must be rejected");
        assert_eq!(std::fs::read(&destination)?, first);

        let other_path = RepoPath::parse("other.bin")?;
        let other_selection = SelectionId::derive(std::slice::from_ref(&other_path))?;
        let other_destination = fixture
            .kernel
            .layout
            .snapshot_manifest(&commit, &other_selection);
        std::fs::create_dir_all(&other_destination)?;
        fixture
            .kernel
            .publish_compatible_manifest(
                &commit,
                &other_selection,
                vec![SnapshotFileRecord::new(
                    &other_path,
                    second_digest,
                    12,
                    Some(HubBlobKey::parse("second-key")?),
                )],
            )
            .expect_err("a non-file manifest destination must be rejected");
        assert!(other_destination.is_dir());

        Ok(())
    }

    #[test]
    fn compatible_manifest_fault_boundaries_expose_only_missing_or_complete_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let key = HubBlobKey::parse("compatible-key")?;
        let digest = BlobDigest::for_bytes(b"shared bytes");
        fixture.kernel.publish_hub_blob_binding(&key, digest, 12)?;
        let files = vec![SnapshotFileRecord::new(&path, digest, 12, Some(key))];

        let before_commit = CommitId::parse(FIRST_COMMIT)?;
        let before_destination = fixture
            .kernel
            .layout
            .snapshot_manifest(&before_commit, &selection);
        fixture
            .faults
            .fail_once(PublicationPoint::BeforeAtomicReplace);
        let before = fixture
            .kernel
            .publish_compatible_manifest(&before_commit, &selection, files.clone())
            .expect_err("the pre-replace fault must surface");
        assert!(!before.may_have_published());
        assert!(!before_destination.try_exists()?);

        let after_commit = CommitId::parse(SECOND_COMMIT)?;
        fixture
            .faults
            .fail_once(PublicationPoint::AfterAtomicReplace);
        let after = fixture
            .kernel
            .publish_compatible_manifest(&after_commit, &selection, files)
            .expect_err("the post-replace fault must surface");
        assert!(after.may_have_published());
        let record = fixture
            .kernel
            .read_snapshot_manifest(&after_commit, &selection)?;
        assert_eq!(record.commit(), after_commit.as_str());
        assert_eq!(record.selection_id(), selection.to_string());

        Ok(())
    }

    #[test]
    fn compatible_manifests_use_a_larger_explicit_bounded_record_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let key = HubBlobKey::parse("shared-key")?;
        let digest = BlobDigest::for_bytes(b"x");
        fixture.kernel.publish_hub_blob_binding(&key, digest, 1)?;
        let paths = (0..700)
            .map(|index| RepoPath::parse(format!("nested/file-{index:04}.bin")))
            .collect::<Result<Vec<_>, _>>()?;
        let selection = SelectionId::derive(&paths)?;
        let files = paths
            .iter()
            .map(|path| SnapshotFileRecord::new(path, digest, 1, Some(key.clone())))
            .collect::<Vec<_>>();

        fixture
            .kernel
            .publish_compatible_manifest(&commit, &selection, files)?;
        let destination = fixture.kernel.layout.snapshot_manifest(&commit, &selection);
        let encoded = std::fs::read(&destination)?;
        assert!(encoded.len() > MAX_SMALL_RECORD_BYTES);
        assert!(encoded.len() <= MAX_MANIFEST_RECORD_BYTES);
        assert_eq!(
            fixture
                .kernel
                .read_snapshot_manifest(&commit, &selection)?
                .files()
                .len(),
            700
        );

        std::fs::write(&destination, vec![b'x'; MAX_MANIFEST_RECORD_BYTES + 1])?;
        fixture
            .kernel
            .read_snapshot_manifest(&commit, &selection)
            .expect_err("an oversized manifest must be rejected by its bounded reader");

        Ok(())
    }

    #[test]
    fn initialization_preserves_unknown_and_conflicting_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let unknown_fixture = Fixture::new()?;
        let format_path = unknown_fixture.kernel.layout.format_record();
        let unknown =
            br#"{"format_version":2,"record_kind":"format","payload":{"future_layout":true}}\n"#;
        std::fs::write(&format_path, unknown)?;
        let endpoint = Endpoint::hugging_face();

        unknown_fixture
            .kernel
            .initialize()
            .expect_err("unknown cache formats must not be overwritten");
        assert_eq!(std::fs::read(&format_path)?, unknown);

        let conflicting_fixture = Fixture::new()?;
        let repository_path = conflicting_fixture.kernel.layout.repository_record();
        let other_spec = RepositorySpec::dataset(RepositoryId::parse("other/repo")?);
        let origin = OriginKey::derive(&endpoint)?;
        let repository = RepositoryKey::derive(&origin, &other_spec)?;
        let conflict = encode_record(&RepositoryRecord::new(&origin, &repository, &other_spec))?;
        std::fs::write(&repository_path, &conflict)?;

        conflicting_fixture
            .kernel
            .initialize()
            .expect_err("conflicting repository metadata must not be overwritten");
        assert_eq!(std::fs::read(&repository_path)?, conflict);

        Ok(())
    }

    #[test]
    fn directory_sync_failures_report_that_publication_may_have_completed()
    -> Result<(), Box<dyn std::error::Error>> {
        let file_system = Arc::new(SyncFaultFileSystem::default());
        let fixture = Fixture::with_file_system(Arc::clone(&file_system) as Arc<dyn FileSystem>)?;
        let payload = b"durability boundary";
        let digest = BlobDigest::for_bytes(payload);
        file_system.fail_next_sync();

        let blob_error = fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)
            .expect_err("directory synchronization failure must surface");
        assert!(blob_error.may_have_published());
        assert_eq!(std::fs::read(fixture.kernel.blob_path(&digest))?, payload);

        let revision = Revision::parse("main")?;
        let first = CommitId::parse(FIRST_COMMIT)?;
        let second = CommitId::parse(SECOND_COMMIT)?;
        fixture.kernel.write_ref(&revision, &first)?;
        file_system.fail_next_sync();
        let ref_error = fixture
            .kernel
            .write_ref(&revision, &second)
            .expect_err("record directory synchronization failure must surface");
        assert!(ref_error.may_have_published());
        assert_eq!(fixture.kernel.read_ref(&revision)?, second);

        Ok(())
    }

    #[test]
    fn operation_ids_and_clock_are_substitutable() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let path = crate::RepoPath::parse("weights/model.bin")?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let partial = fixture.kernel.new_partial_record(
            &commit,
            &path,
            10,
            4,
            Some("etag".to_owned()),
            None,
        )?;

        assert_eq!(partial.updated_unix_millis(), 1_721_596_800_000);
        assert_eq!(fixture.ids.issued(), 3);

        Ok(())
    }

    #[test]
    fn partial_gc_revalidates_and_removes_only_the_planned_expired_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("weights/model.bin")?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let bytes = b"prefix";
        let mut sink = fixture.kernel.create_fresh_partial_sink(&commit, &path)?;
        crate::transfer::PartialSink::write_all(&mut sink, bytes)?;
        crate::transfer::PartialSink::sync_all(&sink)?;
        fixture.kernel.persist_partial_record(
            &commit,
            &path,
            20,
            u64::try_from(bytes.len())?,
            Some("etag".to_owned()),
            None,
        )?;
        let now = 1_721_596_800_100;
        let planned = fixture.kernel.plan_partial_gc(now, 50)?;
        assert_eq!(planned.len(), 1);
        assert!(fixture.kernel.execute_partial_gc(&planned[0], now, 50)?);
        assert!(!fixture.kernel.partial_data_path(&commit, &path)?.exists());
        assert!(fixture.kernel.plan_partial_gc(now, 50)?.is_empty());
        Ok(())
    }

    #[test]
    fn snapshot_gc_retains_ref_roots_and_schedules_detached_snapshot_before_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("weights/model.bin")?;
        let first_commit = CommitId::parse(FIRST_COMMIT)?;
        let second_commit = CommitId::parse(SECOND_COMMIT)?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let first_bytes = b"retained snapshot bytes";
        let second_bytes = b"detached snapshot bytes";
        let first_digest = BlobDigest::for_bytes(first_bytes);
        let second_digest = BlobDigest::for_bytes(second_bytes);
        fixture.kernel.publish_blob(
            Cursor::new(first_bytes),
            u64::try_from(first_bytes.len())?,
            first_digest,
        )?;
        fixture.kernel.publish_blob(
            Cursor::new(second_bytes),
            u64::try_from(second_bytes.len())?,
            second_digest,
        )?;
        drop(fixture.kernel.publish_owned_snapshot(
            &first_commit,
            &selection,
            &[(
                path.clone(),
                first_digest,
                u64::try_from(first_bytes.len())?,
            )],
        )?);
        drop(fixture.kernel.publish_owned_snapshot(
            &second_commit,
            &selection,
            &[(path, second_digest, u64::try_from(second_bytes.len())?)],
        )?);
        fixture
            .kernel
            .write_ref(&Revision::parse("main")?, &first_commit)?;

        let candidates = fixture.kernel.plan_snapshot_gc(u64::MAX, 0, 0, &[])?;
        assert_eq!(candidates.len(), 2);
        assert!(matches!(candidates[0], GcObservation::Snapshot(_)));
        assert!(matches!(candidates[1], GcObservation::Blob(_)));
        assert_eq!(candidates[1].key(), second_digest.to_string());

        let leased = fixture.kernel.open_owned_snapshot(
            &Revision::parse(second_commit.as_str())?,
            &[RepoPath::parse("weights/model.bin")?],
        )?;
        let GcObservation::Snapshot(snapshot_candidate) = &candidates[0] else {
            return Err("first GC candidate was not a snapshot".into());
        };
        assert!(
            !fixture
                .kernel
                .execute_snapshot_gc(snapshot_candidate, u64::MAX, 0, 0, &[],)?
        );
        drop(leased);
        assert!(
            fixture
                .kernel
                .execute_snapshot_gc(snapshot_candidate, u64::MAX, 0, 0, &[],)?
        );
        let GcObservation::Blob(blob_candidate) = &candidates[1] else {
            return Err("second GC candidate was not a blob".into());
        };
        assert!(
            fixture
                .kernel
                .execute_blob_gc(blob_candidate, u64::MAX, 0, 0, &[])?
        );
        assert!(fixture.kernel.layout.blob_path(&first_digest).is_file());
        assert!(!fixture.kernel.layout.blob_path(&second_digest).exists());
        Ok(())
    }

    #[test]
    fn small_metadata_writes_enforce_their_read_limit() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let revision = Revision::parse("x".repeat(MAX_SMALL_RECORD_BYTES))?;
        let commit = CommitId::parse(FIRST_COMMIT)?;
        let destination = fixture.kernel.layout.ref_record(&revision)?;

        let error = fixture
            .kernel
            .write_ref(&revision, &commit)
            .expect_err("an unreadable oversized ref record must not be published");
        assert!(error.is_record_too_large());
        assert!(!destination.try_exists()?);

        Ok(())
    }

    #[test]
    fn existing_blob_validation_stops_at_the_first_excess_byte() {
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let mut reader = CountingReader::new(vec![b'x'; 1_024], Arc::clone(&bytes_read));

        let error = hash_reader(&mut reader, 3)
            .expect_err("an oversized existing blob must be classified as corrupt");
        assert!(error.is_corrupt_existing_blob());
        assert_eq!(bytes_read.load(Ordering::Acquire), 4);
    }

    #[test]
    fn blob_publication_rejects_non_file_destinations() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let payload = b"expected payload";
        let digest = BlobDigest::for_bytes(payload);
        let destination = fixture.kernel.blob_path(&digest);
        std::fs::create_dir_all(&destination)?;

        let error = fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)
            .expect_err("a directory at an immutable blob address must be rejected");
        assert!(error.is_corrupt_existing_blob());
        assert!(destination.is_dir());

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn blob_publication_does_not_follow_existing_symlinks() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        let payload = b"expected payload";
        let digest = BlobDigest::for_bytes(payload);
        let destination = fixture.kernel.blob_path(&digest);
        let external = fixture.directory.path().join("external-blob");
        std::fs::write(&external, payload)?;
        let parent = destination
            .parent()
            .ok_or_else(|| io::Error::other("blob path has no parent"))?;
        std::fs::create_dir_all(parent)?;
        symlink(&external, &destination)?;

        let error = fixture
            .kernel
            .publish_blob(Cursor::new(payload), u64::try_from(payload.len())?, digest)
            .expect_err("an immutable blob destination symlink must be rejected");
        assert!(error.is_unsafe());
        assert!(
            std::fs::symlink_metadata(&destination)?
                .file_type()
                .is_symlink()
        );

        Ok(())
    }

    fn required_child_path(name: &str) -> io::Result<PathBuf> {
        std::env::var_os(name)
            .map(PathBuf::from)
            .ok_or_else(|| io::Error::other(format!("child process is missing {name}")))
    }

    fn lock_shared_until(file: &File, deadline: Instant) -> io::Result<()> {
        loop {
            match fs4::FileExt::try_lock_shared(file) {
                Ok(()) => return Ok(()),
                Err(fs4::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
                }
                Err(fs4::TryLockError::WouldBlock) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for the cross-process publisher gate",
                    ));
                }
                Err(fs4::TryLockError::Error(source)) => return Err(source),
            }
        }
    }

    fn wait_for_children_ready(
        children: &mut [ManagedChild],
        ready_paths: &[PathBuf],
        deadline: Instant,
    ) -> io::Result<()> {
        loop {
            if ready_paths
                .iter()
                .map(PathBuf::as_path)
                .map(Path::try_exists)
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .all(|ready| ready)
            {
                return Ok(());
            }

            for child in &mut *children {
                if let Some(status) = child.try_wait()? {
                    let output = child.finish()?;
                    return Err(child_failure(
                        "publisher exited before announcing readiness",
                        status,
                        &output,
                    ));
                }
            }

            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for cross-process publishers to become ready",
                ));
            }
            std::thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
        }
    }

    fn wait_for_children_success(
        children: &mut [ManagedChild],
        deadline: Instant,
    ) -> io::Result<()> {
        for child in children {
            let output = child.wait_until(deadline)?;
            if !output.status.success() {
                return Err(child_failure(
                    "cross-process publisher failed",
                    output.status,
                    &output,
                ));
            }
        }
        Ok(())
    }

    fn child_failure(
        context: &str,
        status: std::process::ExitStatus,
        output: &Output,
    ) -> io::Error {
        io::Error::other(format!(
            "{context} with {status}; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }

    struct TestGate {
        file: File,
        locked: bool,
    }

    impl TestGate {
        fn new(file: File) -> Self {
            Self { file, locked: true }
        }

        fn release(&mut self) -> io::Result<()> {
            fs4::FileExt::unlock(&self.file)?;
            self.locked = false;
            Ok(())
        }
    }

    impl Drop for TestGate {
        fn drop(&mut self) {
            if self.locked {
                let _result = fs4::FileExt::unlock(&self.file);
            }
        }
    }

    struct ManagedChild {
        child: Option<Child>,
    }

    impl ManagedChild {
        const fn new(child: Child) -> Self {
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
            self.child
                .as_mut()
                .ok_or_else(|| io::Error::other("publisher process was already reaped"))?
                .try_wait()
        }

        fn finish(&mut self) -> io::Result<Output> {
            self.child
                .take()
                .ok_or_else(|| io::Error::other("publisher process was already reaped"))?
                .wait_with_output()
        }

        fn wait_until(&mut self, deadline: Instant) -> io::Result<Output> {
            loop {
                if self.try_wait()?.is_some() {
                    return self.finish();
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for a cross-process publisher to exit",
                    ));
                }
                std::thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
            }
        }
    }

    impl Drop for ManagedChild {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _kill_result = child.kill();
                let _wait_result = child.wait();
            }
        }
    }

    struct Fixture {
        directory: TempDir,
        kernel: CacheKernel,
        faults: Arc<FaultController>,
        ids: Arc<SequenceOperationIds>,
    }

    const SECRET_ERROR_SENTINEL: &str = "hf_secret_signed_url_sentinel";

    struct SecretError;

    impl Debug for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_ERROR_SENTINEL)
        }
    }

    impl Display for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_ERROR_SENTINEL)
        }
    }

    impl Error for SecretError {}

    struct SecretReader;

    impl Read for SecretReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, SecretError))
        }
    }

    fn assert_secret_absent_from_error_chain(error: &(dyn Error + 'static)) {
        let mut current = Some(error);
        while let Some(source) = current {
            assert!(!source.to_string().contains(SECRET_ERROR_SENTINEL));
            assert!(!format!("{source:?}").contains(SECRET_ERROR_SENTINEL));
            current = source.source();
        }
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            Self::with_file_system(Arc::new(OsFileSystem))
        }

        fn with_file_system(
            file_system: Arc<dyn FileSystem>,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let faults = Arc::new(FaultController::default());
            let ids = Arc::new(SequenceOperationIds::new(1));
            let effects = Effects::new(
                file_system,
                Arc::clone(&ids) as Arc<dyn OperationIds>,
                Arc::new(FixedClock::new(1_721_596_800_000)),
                Arc::clone(&faults) as Arc<dyn PublicationFaults>,
            );
            let kernel = CacheKernel::new(directory.path(), &endpoint, &spec, effects)?;
            kernel.initialize()?;
            Ok(Self {
                directory,
                kernel,
                faults,
                ids,
            })
        }
    }

    #[derive(Debug, Default)]
    struct SyncFaultFileSystem {
        fail_next_sync: Arc<AtomicBool>,
        serialized: Arc<Mutex<()>>,
    }

    impl SyncFaultFileSystem {
        fn fail_next_sync(&self) {
            self.fail_next_sync.store(true, Ordering::Release);
        }
    }

    impl FileSystem for SyncFaultFileSystem {
        fn open_cache_authority(&self, path: &Path) -> io::Result<CacheAuthority> {
            let authority = OsFileSystem.open_cache_authority(path)?;
            Ok(CacheAuthority::new(
                authority.reader(),
                Arc::new(SyncFaultRoot {
                    inner: authority.writer(),
                    fail_next_sync: Arc::clone(&self.fail_next_sync),
                    serialized: Arc::clone(&self.serialized),
                }),
            ))
        }
    }

    #[derive(Debug)]
    struct SyncFaultRoot {
        inner: Arc<dyn RootedFileSystem>,
        fail_next_sync: Arc<AtomicBool>,
        serialized: Arc<Mutex<()>>,
    }

    impl RootedFileSystem for SyncFaultRoot {
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

        fn install_staged_replace(&self, staging: &Path, destination: &Path) -> io::Result<()> {
            self.inner.install_staged_replace(staging, destination)
        }

        fn create_once(
            &self,
            path: &Path,
            bytes: &[u8],
            staging: &StagingName,
        ) -> io::Result<CreateOnceOutcome> {
            self.inner.create_once(path, bytes, staging)
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

        fn try_lock_exclusive(&self, path: &Path) -> io::Result<RootedLockAttempt> {
            self.inner.try_lock_exclusive(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            let _serialized = self
                .serialized
                .lock()
                .map_err(|_poisoned| io::Error::other("sync fault lock poisoned"))?;
            if self.fail_next_sync.swap(false, Ordering::AcqRel) {
                Err(io::Error::other("injected directory sync failure"))
            } else {
                self.inner.sync_directory(path)
            }
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
    }

    #[derive(Debug)]
    struct CountingReader {
        inner: Cursor<Vec<u8>>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl CountingReader {
        fn new(bytes: Vec<u8>, bytes_read: Arc<AtomicUsize>) -> Self {
            Self {
                inner: Cursor::new(bytes),
                bytes_read,
            }
        }
    }

    impl Read for CountingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let read = self.inner.read(buffer)?;
            self.bytes_read.fetch_add(read, Ordering::AcqRel);
            Ok(read)
        }
    }
}
