use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use atomic_write_file::AtomicWriteFile;
use sha2::{Digest, Sha256};

use crate::validation::ValidationError;
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::key::{BlobDigest, OriginKey, RepositoryKey};
use super::layout::CacheLayout;
use super::metadata::{
    CacheRecord, FormatRecord, MetadataError, OriginRecord, PartialTransferRecord, RefRecord,
    RepositoryRecord, decode_record, encode_record,
};

const COPY_BUFFER_SIZE: usize = 64 * 1024;
const MAX_SMALL_RECORD_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublicationPoint {
    AfterStagingCreate,
    AfterStagingSync,
    BeforeBlobPublish,
    AfterBlobPublish,
    BeforeAtomicReplace,
    AfterAtomicReplace,
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

pub(super) trait DurableWrite: Write + Send {
    fn sync_all(&self) -> io::Result<()>;
}

impl DurableWrite for File {
    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }
}

pub(super) trait LockGuard: Send {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EntryKind {
    Missing,
    RegularFile,
    Other,
}

pub(super) trait FileSystem: Debug + Send + Sync {
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn create_new(&self, path: &Path) -> io::Result<Box<dyn DurableWrite>>;
    fn open_read(&self, path: &Path) -> io::Result<Box<dyn Read + Send>>;
    fn entry_kind(&self, path: &Path) -> io::Result<EntryKind>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()>;
    fn atomic_replace(&self, destination: &Path, bytes: &[u8]) -> io::Result<()>;
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
}

#[derive(Debug)]
pub(super) struct OsFileSystem;

impl FileSystem for OsFileSystem {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<Box<dyn DurableWrite>> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Box::new(file))
    }

    fn open_read(&self, path: &Path) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(File::open(path)?))
    }

    fn entry_kind(&self, path: &Path) -> io::Result<EntryKind> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(EntryKind::RegularFile),
            Ok(_metadata) => Ok(EntryKind::Other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(EntryKind::Missing),
            Err(error) => Err(error),
        }
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        std::fs::rename(source, destination)
    }

    fn atomic_replace(&self, destination: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = AtomicWriteFile::open(destination)?;
        file.write_all(bytes)?;
        file.commit()
    }

    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        fs4::FileExt::lock(&file)?;
        Ok(Box::new(OsLockGuard { _file: file }))
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    }
}

#[derive(Debug)]
struct OsLockGuard {
    _file: File,
}

impl LockGuard for OsLockGuard {}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(path: &Path) -> io::Result<()> {
    // This preserves an explicit filesystem boundary without claiming that
    // Windows directory metadata reached durable storage. ADR 0003 reserves
    // that stronger guarantee for a future capability-aware durable mode.
    std::fs::metadata(path).map(|_metadata| ())
}

#[derive(Clone, Debug)]
pub(super) struct Effects {
    file_system: Arc<dyn FileSystem>,
    operation_ids: Arc<dyn OperationIds>,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn PublicationFaults>,
}

impl Effects {
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
}

#[derive(Clone, Debug)]
pub(super) struct CacheKernel {
    layout: CacheLayout,
    origin_record: OriginRecord,
    repository_record: RepositoryRecord,
    effects: Effects,
}

impl CacheKernel {
    pub(super) fn new(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
    ) -> Result<Self, CacheError> {
        let origin_key = OriginKey::derive(endpoint)?;
        let repository_key = RepositoryKey::derive(&origin_key, spec)?;
        Ok(Self {
            layout: CacheLayout::new(root, endpoint, spec)?,
            origin_record: OriginRecord::new(endpoint),
            repository_record: RepositoryRecord::new(&origin_key, &repository_key, spec),
            effects,
        })
    }

    pub(super) fn initialize(&self) -> Result<(), CacheError> {
        self.effects
            .file_system
            .create_dir_all(self.layout.cache_root())?;
        let format_lock = self.layout.format_lock();
        create_parent_directories(self.effects.file_system.as_ref(), &format_lock)?;
        let _guard = self.effects.file_system.lock_exclusive(&format_lock)?;
        self.ensure_record(&self.layout.format_record(), &FormatRecord::new())?;

        self.effects
            .file_system
            .create_dir_all(&self.layout.repository_directory())?;
        self.effects
            .file_system
            .create_dir_all(&self.layout.staging_directory())?;
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
        create_parent_directories(self.effects.file_system.as_ref(), &staging_path)?;
        let mut cleanup =
            StagingCleanup::inactive(self.effects.file_system.as_ref(), staging_path.clone());
        let mut staging_file = self.effects.file_system.create_new(&staging_path)?;
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
        create_parent_directories(self.effects.file_system.as_ref(), &destination)?;
        create_parent_directories(self.effects.file_system.as_ref(), &lock_path)?;
        let _guard = self.effects.file_system.lock_exclusive(&lock_path)?;

        match self.effects.file_system.entry_kind(&destination)? {
            EntryKind::RegularFile => {
                validate_existing_blob(
                    self.effects.file_system.as_ref(),
                    &destination,
                    expected_size,
                    expected_digest,
                )?;
                self.effects.file_system.remove_file(&staging_path)?;
                cleanup.deactivate();
                return Ok(BlobPublication::new(
                    destination,
                    BlobPublicationOutcome::Reused,
                ));
            }
            EntryKind::Other => return Err(CacheError::corrupt_existing_blob()),
            EntryKind::Missing => {}
        }

        self.check_fault(PublicationPoint::BeforeBlobPublish, false)?;
        self.effects
            .file_system
            .rename(&staging_path, &destination)?;
        cleanup.deactivate();
        self.check_fault(PublicationPoint::AfterBlobPublish, true)?;
        sync_parent_directory(self.effects.file_system.as_ref(), &destination)
            .map_err(|source| CacheError::io(source, true))?;
        Ok(BlobPublication::new(
            destination,
            BlobPublicationOutcome::Published,
        ))
    }

    pub(super) fn blob_path(&self, digest: &BlobDigest) -> PathBuf {
        self.layout.blob_path(digest)
    }

    pub(super) fn staging_entries(&self) -> Result<Vec<PathBuf>, CacheError> {
        Ok(self
            .effects
            .file_system
            .read_dir(&self.layout.staging_directory())?)
    }

    pub(super) fn write_ref(
        &self,
        revision: &Revision,
        commit: &CommitId,
    ) -> Result<(), CacheError> {
        let destination = self.layout.ref_record(revision)?;
        let lock_path = self.layout.ref_lock(revision)?;
        create_parent_directories(self.effects.file_system.as_ref(), &lock_path)?;
        let _guard = self.effects.file_system.lock_exclusive(&lock_path)?;
        if self.effects.file_system.entry_kind(&destination)? == EntryKind::Other {
            return Err(CacheError::conflicting_record());
        }
        self.replace_record(&destination, &RefRecord::new(revision, commit))
    }

    pub(super) fn read_ref(&self, revision: &Revision) -> Result<CommitId, CacheError> {
        let path = self.layout.ref_record(revision)?;
        if self.effects.file_system.entry_kind(&path)? == EntryKind::Other {
            return Err(CacheError::conflicting_record());
        }
        let bytes = read_all(self.effects.file_system.as_ref(), &path)?;
        let record = decode_record::<RefRecord>(&bytes)?;
        if record.revision() != revision.as_str() {
            return Err(CacheError::conflicting_record());
        }
        Ok(CommitId::parse(record.commit())?)
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
            commit,
            path,
            expected_size,
            received_size,
            validator,
            target_digest,
            updated,
        )?)
    }

    fn replace_record<T: CacheRecord>(
        &self,
        destination: &Path,
        record: &T,
    ) -> Result<(), CacheError> {
        let encoded = encode_record(record)?;
        if encoded.len() > MAX_SMALL_RECORD_BYTES {
            return Err(CacheError::record_too_large());
        }
        create_parent_directories(self.effects.file_system.as_ref(), destination)?;
        self.check_fault(PublicationPoint::BeforeAtomicReplace, false)?;
        self.effects
            .file_system
            .atomic_replace(destination, &encoded)
            .map_err(|source| CacheError::io(source, true))?;
        self.check_fault(PublicationPoint::AfterAtomicReplace, true)?;
        sync_parent_directory(self.effects.file_system.as_ref(), destination)
            .map_err(|source| CacheError::io(source, true))
    }

    fn ensure_record<T>(&self, destination: &Path, expected: &T) -> Result<(), CacheError>
    where
        T: CacheRecord + Eq,
    {
        match self.effects.file_system.entry_kind(destination)? {
            EntryKind::RegularFile => {
                let bytes = read_all(self.effects.file_system.as_ref(), destination)?;
                let existing = decode_record::<T>(&bytes)?;
                if &existing != expected {
                    return Err(CacheError::conflicting_record());
                }
                Ok(())
            }
            EntryKind::Missing => self.replace_record(destination, expected),
            EntryKind::Other => Err(CacheError::conflicting_record()),
        }
    }

    fn check_fault(
        &self,
        point: PublicationPoint,
        may_have_published: bool,
    ) -> Result<(), CacheError> {
        self.effects
            .faults
            .check(point)
            .map_err(|source| CacheError::io(source, may_have_published))
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
    Io(io::Error),
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

    fn io(source: io::Error, may_have_published: bool) -> Self {
        Self::new(CacheErrorKind::Io(source), may_have_published)
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
            CacheErrorKind::Io(source) => Some(source),
            CacheErrorKind::Validation(source) => Some(source),
            CacheErrorKind::Metadata(source) => Some(source),
            CacheErrorKind::SizeMismatch { .. }
            | CacheErrorKind::DigestMismatch
            | CacheErrorKind::CorruptExistingBlob
            | CacheErrorKind::ConflictingRecord
            | CacheErrorKind::RecordTooLarge => None,
        }
    }
}

impl From<io::Error> for CacheError {
    fn from(source: io::Error) -> Self {
        Self::io(source, false)
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

fn copy_and_hash(
    reader: &mut dyn Read,
    writer: &mut dyn DurableWrite,
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
    file_system: &dyn FileSystem,
    path: &Path,
    expected_size: u64,
    expected_digest: BlobDigest,
) -> Result<(), CacheError> {
    let mut reader = file_system.open_read(path)?;
    let (actual_size, actual_digest) = hash_reader(reader.as_mut(), expected_size)?;
    if actual_size == expected_size && actual_digest == expected_digest {
        Ok(())
    } else {
        Err(CacheError::corrupt_existing_blob())
    }
}

fn create_parent_directories(file_system: &dyn FileSystem, path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("cache path has no parent directory"))?;
    file_system.create_dir_all(parent)
}

fn sync_parent_directory(file_system: &dyn FileSystem, path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("cache path has no parent directory"))?;
    file_system.sync_directory(parent)
}

fn read_all(file_system: &dyn FileSystem, path: &Path) -> io::Result<Vec<u8>> {
    let reader = file_system.open_read(path)?;
    let mut bytes = Vec::new();
    let limit = u64::try_from(MAX_SMALL_RECORD_BYTES)
        .map_err(io::Error::other)?
        .saturating_add(1);
    reader.take(limit).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SMALL_RECORD_BYTES {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache metadata record exceeds its size limit",
        ))
    } else {
        Ok(bytes)
    }
}

struct StagingCleanup<'a> {
    file_system: &'a dyn FileSystem,
    path: PathBuf,
    active: bool,
}

impl<'a> StagingCleanup<'a> {
    fn inactive(file_system: &'a dyn FileSystem, path: PathBuf) -> Self {
        Self {
            file_system,
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
            let _result = self.file_system.remove_file(&self.path);
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
        let staging_path = fixture
            .kernel
            .layout
            .staged_blob("00000000000000000000000000000001");
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

        let digest = BlobDigest::for_bytes(CROSS_PROCESS_PAYLOAD);
        assert_eq!(
            std::fs::read(fixture.kernel.blob_path(&digest))?,
            CROSS_PROCESS_PAYLOAD
        );
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
        assert_eq!(fixture.ids.issued(), 0);

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
        assert!(error.is_corrupt_existing_blob());
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
        fail_next_sync: AtomicBool,
        serialized: Mutex<()>,
    }

    impl SyncFaultFileSystem {
        fn fail_next_sync(&self) {
            self.fail_next_sync.store(true, Ordering::Release);
        }
    }

    impl FileSystem for SyncFaultFileSystem {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            OsFileSystem.create_dir_all(path)
        }

        fn create_new(&self, path: &Path) -> io::Result<Box<dyn DurableWrite>> {
            OsFileSystem.create_new(path)
        }

        fn open_read(&self, path: &Path) -> io::Result<Box<dyn Read + Send>> {
            OsFileSystem.open_read(path)
        }

        fn entry_kind(&self, path: &Path) -> io::Result<EntryKind> {
            OsFileSystem.entry_kind(path)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            OsFileSystem.remove_file(path)
        }

        fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
            OsFileSystem.rename(source, destination)
        }

        fn atomic_replace(&self, destination: &Path, bytes: &[u8]) -> io::Result<()> {
            OsFileSystem.atomic_replace(destination, bytes)
        }

        fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
            OsFileSystem.lock_exclusive(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            let _serialized = self
                .serialized
                .lock()
                .map_err(|_poisoned| io::Error::other("sync fault lock poisoned"))?;
            if self.fail_next_sync.swap(false, Ordering::AcqRel) {
                Err(io::Error::other("injected directory sync failure"))
            } else {
                OsFileSystem.sync_directory(path)
            }
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            OsFileSystem.read_dir(path)
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
