use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::hub_layout::{HubBlobKey, HubCacheLayout};
use super::hub_metadata::{HubMetadataError, HubTree, HubTreeEntry, decode_ref, decode_tree};
use super::key::BlobDigest;
use super::publication::{CacheDirectory, EntryKind, FileSystem, RegularFileOpen};
use super::rooted_fs::is_unsafe_cache_path_error;
use super::sanitized_io::SanitizedIo;

// A standard-cache ref contains one 40-byte commit. Leave limited headroom for
// detecting malformed records without permitting an unbounded allocation.
const MAX_REF_BYTES: usize = 64;
// Pinned tree-cache records are eagerly decoded today. Bound hostile or corrupt
// cache input until tree decoding becomes streaming or caller-configurable.
const MAX_TREE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct HubCacheReader {
    root: Arc<dyn CacheDirectory>,
    layout: HubCacheLayout,
    repository_relative: PathBuf,
}

impl HubCacheReader {
    pub(super) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        file_system: &dyn FileSystem,
    ) -> Result<Self, HubCacheReadError> {
        let root = root.as_ref();
        let layout = HubCacheLayout::shared(root, endpoint, spec)?;
        let authority = file_system.open_cache_authority(root)?;
        Self::from_layout(layout, authority.reader()).map_err(HubCacheReadError::from)
    }

    pub(super) fn from_layout(
        layout: HubCacheLayout,
        root: Arc<dyn CacheDirectory>,
    ) -> Result<Self, ValidationError> {
        let repository_relative = layout
            .repository_directory()
            .file_name()
            .map(PathBuf::from)
            .ok_or_else(|| {
                ValidationError::new(
                    "Hub cache repository boundary",
                    ValidationErrorKind::UnsafePath,
                )
            })?;
        Ok(Self {
            root,
            layout,
            repository_relative,
        })
    }

    pub(super) fn read_index(
        &self,
        revision: &Revision,
    ) -> Result<HubCacheIndex, HubCacheReadError> {
        let direct_commit = CommitId::parse(revision.as_str());
        let missing_repository = if direct_commit.is_ok() {
            MissingRecord::Incomplete
        } else {
            self.layout
                .ref_path(revision)
                .map_err(HubCacheReadError::unsafe_path)?;
            MissingRecord::Missing
        };
        let directory = self.open_repository(missing_repository)?;
        let commit = match direct_commit {
            Ok(commit) => commit,
            Err(_symbolic_revision) => self.read_ref(directory.as_ref(), revision)?,
        };
        let tree_path = self.repository_relative_path(&self.layout.tree_path(&commit))?;
        let bytes = Self::read_regular_record(
            directory.as_ref(),
            &tree_path,
            MAX_TREE_BYTES,
            MissingRecord::Incomplete,
        )?;
        let tree = decode_tree(&bytes).map_err(HubCacheReadError::tree_metadata)?;
        Ok(HubCacheIndex {
            commit,
            tree,
            directory,
        })
    }

    pub(super) const fn layout(&self) -> &HubCacheLayout {
        &self.layout
    }

    pub(super) fn index_from_tree(
        &self,
        commit: &CommitId,
        tree: &HubTree,
    ) -> Result<HubCacheIndex, HubCacheReadError> {
        Ok(HubCacheIndex {
            commit: commit.clone(),
            tree: tree.clone(),
            directory: self.open_repository(MissingRecord::Incomplete)?,
        })
    }

    pub(super) fn read_snapshot_file(
        &self,
        index: &HubCacheIndex,
        path: &RepoPath,
    ) -> Result<HubCacheFile, HubCacheReadError> {
        let entry = index
            .tree
            .files()
            .get(path)
            .ok_or_else(HubCacheReadError::missing)?;
        let snapshot_path = self.layout.snapshot_file(&index.commit, path);
        let snapshot_relative = self.repository_relative_path(&snapshot_path)?;
        let blob_key = compatible_blob_key(entry)?;
        let blob_path = self.layout.blob_path(&blob_key);
        let blob_relative = self.repository_relative_path(&blob_path)?;
        match index.directory.entry_kind(&snapshot_relative)? {
            EntryKind::Missing => return Err(HubCacheReadError::incomplete()),
            EntryKind::RegularFile => {}
            EntryKind::Other => {
                #[cfg(unix)]
                {
                    return Self::read_relative_symlink(
                        index.directory.as_ref(),
                        &snapshot_path,
                        &snapshot_relative,
                        &blob_path,
                        &blob_relative,
                        blob_key,
                        entry,
                    );
                }
                #[cfg(not(unix))]
                {
                    return Err(HubCacheReadError::corrupt());
                }
            }
        }
        let (size, digest) = Self::hash_open_regular_file(
            index.directory.as_ref(),
            &snapshot_relative,
            entry,
            MissingRecord::Incomplete,
        )?;
        let (form, retained_blob) = match index.directory.entry_kind(&blob_relative)? {
            EntryKind::Missing => (HubSnapshotFileForm::SnapshotOnly, None),
            EntryKind::RegularFile => {
                let (blob_size, blob_digest) = Self::hash_open_regular_file(
                    index.directory.as_ref(),
                    &blob_relative,
                    entry,
                    MissingRecord::Incomplete,
                )?;
                if blob_size != size || blob_digest != digest {
                    return Err(HubCacheReadError::corrupt());
                }
                (HubSnapshotFileForm::CopiedWithBlob, Some(blob_path))
            }
            EntryKind::Other => return Err(HubCacheReadError::corrupt()),
        };

        Ok(HubCacheFile {
            path: snapshot_path,
            blob_path: retained_blob,
            size,
            digest,
            form,
            hub_blob_key: blob_key,
        })
    }

    pub(super) fn copy_regular_snapshot_content<W: Write + ?Sized>(
        &self,
        index: &HubCacheIndex,
        path: &RepoPath,
        writer: &mut W,
    ) -> Result<(u64, BlobDigest), HubCacheReadError> {
        let entry = index
            .tree
            .files()
            .get(path)
            .ok_or_else(HubCacheReadError::missing)?;
        let snapshot = self.layout.snapshot_file(&index.commit, path);
        let snapshot_relative = self.repository_relative_path(&snapshot)?;
        let (mut reader, size) = match index.directory.open_regular(&snapshot_relative)? {
            RegularFileOpen::File { reader, size } => (reader, size),
            RegularFileOpen::Missing => return Err(HubCacheReadError::incomplete()),
            RegularFileOpen::Other => return Err(HubCacheReadError::corrupt()),
        };
        if size != entry.size() {
            return Err(HubCacheReadError::corrupt());
        }
        copy_and_validate_content(reader.as_mut(), writer, entry)
    }

    #[cfg(unix)]
    fn read_relative_symlink(
        directory: &dyn CacheDirectory,
        snapshot_path: &Path,
        snapshot_relative: &Path,
        blob_path: &Path,
        blob_relative: &Path,
        hub_blob_key: HubBlobKey,
        entry: &HubTreeEntry,
    ) -> Result<HubCacheFile, HubCacheReadError> {
        let target = match directory.read_link(snapshot_relative) {
            Ok(target) => target,
            Err(source) if source.kind() == io::ErrorKind::InvalidInput => {
                return Err(HubCacheReadError::corrupt());
            }
            Err(source) => return Err(source.into()),
        };
        if target.is_absolute() {
            return Err(HubCacheReadError::unsafe_symlink());
        }
        let expected = canonical_relative_link_target(snapshot_relative, blob_relative)
            .ok_or_else(HubCacheReadError::corrupt)?;
        if target != expected {
            return Err(HubCacheReadError::unsafe_symlink());
        }
        let (size, digest) = Self::hash_open_regular_file(
            directory,
            blob_relative,
            entry,
            MissingRecord::Incomplete,
        )?;
        Ok(HubCacheFile {
            path: snapshot_path.to_path_buf(),
            blob_path: Some(blob_path.to_path_buf()),
            size,
            digest,
            form: HubSnapshotFileForm::RelativeSymlink,
            hub_blob_key,
        })
    }

    fn read_ref(
        &self,
        directory: &dyn CacheDirectory,
        revision: &Revision,
    ) -> Result<CommitId, HubCacheReadError> {
        let path = self
            .layout
            .ref_path(revision)
            .map_err(HubCacheReadError::unsafe_path)?;
        let path = self.repository_relative_path(&path)?;
        let bytes =
            Self::read_regular_record(directory, &path, MAX_REF_BYTES, MissingRecord::Missing)?;
        decode_ref(&bytes).map_err(HubCacheReadError::corrupt_metadata)
    }

    fn read_regular_record(
        directory: &dyn CacheDirectory,
        path: &Path,
        limit: usize,
        missing: MissingRecord,
    ) -> Result<Vec<u8>, HubCacheReadError> {
        let (mut reader, size) = match directory.open_regular(path)? {
            RegularFileOpen::File { reader, size } => (reader, size),
            RegularFileOpen::Missing => return Err(missing.into_error()),
            RegularFileOpen::Other => return Err(HubCacheReadError::corrupt()),
        };
        let limit_u64 = u64::try_from(limit).map_err(|_overflow| HubCacheReadError::corrupt())?;
        if size > limit_u64 {
            return Err(HubCacheReadError::corrupt());
        }
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(limit_u64.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(HubCacheReadError::corrupt());
        }
        Ok(bytes)
    }

    fn hash_open_regular_file(
        directory: &dyn CacheDirectory,
        path: &Path,
        entry: &HubTreeEntry,
        missing: MissingRecord,
    ) -> Result<(u64, BlobDigest), HubCacheReadError> {
        let (mut reader, size) = match directory.open_regular(path)? {
            RegularFileOpen::File { reader, size } => (reader, size),
            RegularFileOpen::Missing => return Err(missing.into_error()),
            RegularFileOpen::Other => return Err(HubCacheReadError::corrupt()),
        };
        if size != entry.size() {
            return Err(HubCacheReadError::corrupt());
        }
        validate_content(reader.as_mut(), entry)
    }

    fn open_repository(
        &self,
        missing: MissingRecord,
    ) -> Result<Arc<dyn CacheDirectory>, HubCacheReadError> {
        match self.root.entry_kind(&self.repository_relative)? {
            EntryKind::Missing => Err(missing.into_error()),
            EntryKind::RegularFile => Err(HubCacheReadError::corrupt()),
            EntryKind::Other => self
                .root
                .open_dir_nofollow(&self.repository_relative)
                .map_err(HubCacheReadError::from),
        }
    }

    fn repository_relative_path(&self, path: &Path) -> Result<PathBuf, HubCacheReadError> {
        path.strip_prefix(self.layout.repository_directory())
            .map(Path::to_path_buf)
            .map_err(|_outside_repository| HubCacheReadError::corrupt())
    }
}

#[derive(Clone, Debug)]
pub(super) struct HubCacheIndex {
    commit: CommitId,
    tree: HubTree,
    directory: Arc<dyn CacheDirectory>,
}

impl HubCacheIndex {
    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) const fn tree(&self) -> &HubTree {
        &self.tree
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HubSnapshotFileForm {
    SnapshotOnly,
    CopiedWithBlob,
    RelativeSymlink,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct HubCacheFile {
    path: PathBuf,
    blob_path: Option<PathBuf>,
    size: u64,
    digest: BlobDigest,
    form: HubSnapshotFileForm,
    hub_blob_key: HubBlobKey,
}

impl HubCacheFile {
    /// Returns the path observed during validation.
    ///
    /// A compatible cache is mutable and does not participate in hf-store
    /// leases. Consumers must reopen and revalidate the bytes rather than
    /// treating this path as permanently bound to `digest`. A later owned
    /// materializer may independently stage the validated bytes.
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn blob_path(&self) -> Option<&Path> {
        self.blob_path.as_deref()
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }

    pub(super) const fn digest(&self) -> BlobDigest {
        self.digest
    }

    pub(super) const fn form(&self) -> HubSnapshotFileForm {
        self.form
    }

    pub(super) const fn hub_blob_key(&self) -> &HubBlobKey {
        &self.hub_blob_key
    }
}

pub(super) fn compatible_blob_key(entry: &HubTreeEntry) -> Result<HubBlobKey, HubCacheReadError> {
    let value = validated_lfs_sha256(entry)?.unwrap_or_else(|| entry.blob_id());
    HubBlobKey::parse(value).map_err(HubCacheReadError::unsafe_path)
}

fn validate_content(
    reader: &mut dyn Read,
    entry: &HubTreeEntry,
) -> Result<(u64, BlobDigest), HubCacheReadError> {
    copy_and_validate_content(reader, &mut io::sink(), entry)
}

pub(super) fn copy_and_validate_content<W: Write + ?Sized>(
    reader: &mut dyn Read,
    writer: &mut W,
    entry: &HubTreeEntry,
) -> Result<(u64, BlobDigest), HubCacheReadError> {
    let expected_lfs = validated_lfs_sha256(entry)?;

    let expected_git =
        (expected_lfs.is_none() && is_lower_hex(entry.blob_id(), 40)).then_some(entry.blob_id());
    let mut git_hasher = expected_git.map(|_expected| {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", entry.size()).as_bytes());
        hasher
    });
    let mut local_hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();

    loop {
        let read_capacity = bounded_read_capacity(entry.size(), size, buffer.len());
        let count = reader.read(&mut buffer[..read_capacity])?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(u64::try_from(count).map_err(|_overflow| HubCacheReadError::corrupt())?)
            .ok_or_else(HubCacheReadError::corrupt)?;
        if size > entry.size() {
            return Err(HubCacheReadError::corrupt());
        }
        writer.write_all(&buffer[..count])?;
        local_hasher.update(&buffer[..count]);
        if let Some(hasher) = git_hasher.as_mut() {
            hasher.update(&buffer[..count]);
        }
    }

    if size != entry.size() {
        return Err(HubCacheReadError::corrupt());
    }

    let digest = BlobDigest::from_bytes(local_hasher.finalize().into());
    if expected_lfs.is_some_and(|expected| digest.to_string() != expected) {
        return Err(HubCacheReadError::corrupt());
    }
    if let (Some(expected), Some(hasher)) = (expected_git, git_hasher) {
        if format!("{:x}", hasher.finalize()) != expected {
            return Err(HubCacheReadError::corrupt());
        }
    }

    Ok((size, digest))
}

fn validated_lfs_sha256(entry: &HubTreeEntry) -> Result<Option<&str>, HubCacheReadError> {
    match (entry.lfs_sha256(), entry.lfs_size()) {
        (Some(sha256), Some(size)) => {
            if is_lower_hex(sha256, 64) && size == entry.size() {
                Ok(Some(sha256))
            } else {
                Err(HubCacheReadError::corrupt())
            }
        }
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(HubCacheReadError::corrupt()),
    }
}

fn bounded_read_capacity(expected_size: u64, current_size: u64, buffer_size: usize) -> usize {
    let remaining = expected_size.saturating_sub(current_size);
    let probe_size = remaining.saturating_add(1);
    usize::try_from(probe_size).map_or(buffer_size, |capacity| capacity.min(buffer_size))
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(unix)]
pub(super) fn canonical_relative_link_target(source: &Path, destination: &Path) -> Option<PathBuf> {
    use std::path::Component;

    let source_parent = source.parent()?;
    let source_components = source_parent
        .components()
        .map(|component| match component {
            Component::Normal(value) => Some(value),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let destination_components = destination
        .components()
        .map(|component| match component {
            Component::Normal(value) => Some(value),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let common = source_components
        .iter()
        .zip(&destination_components)
        .take_while(|(source, destination)| source == destination)
        .count();

    let mut relative = PathBuf::new();
    for _component in &source_components[common..] {
        relative.push("..");
    }
    for component in &destination_components[common..] {
        relative.push(component);
    }
    (!relative.as_os_str().is_empty()).then_some(relative)
}

#[derive(Clone, Copy)]
enum MissingRecord {
    Missing,
    Incomplete,
}

impl MissingRecord {
    fn into_error(self) -> HubCacheReadError {
        match self {
            Self::Missing => HubCacheReadError::missing(),
            Self::Incomplete => HubCacheReadError::incomplete(),
        }
    }
}

#[derive(Debug)]
pub(super) struct HubCacheReadError {
    kind: Box<HubCacheReadErrorKind>,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum HubCacheReadErrorKind {
    Io(SanitizedIo),
    UnsafeFileSystem(SanitizedIo),
    Validation(ValidationError),
    Missing,
    Incomplete,
    Corrupt(Option<HubMetadataError>),
    UnsupportedVersion(HubMetadataError),
    Unsafe(ValidationError),
}

impl HubCacheReadError {
    fn new(kind: HubCacheReadErrorKind) -> Self {
        Self {
            kind: Box::new(kind),
            backtrace: Backtrace::capture(),
        }
    }

    fn missing() -> Self {
        Self::new(HubCacheReadErrorKind::Missing)
    }

    fn incomplete() -> Self {
        Self::new(HubCacheReadErrorKind::Incomplete)
    }

    pub(super) fn corrupt() -> Self {
        Self::new(HubCacheReadErrorKind::Corrupt(None))
    }

    fn corrupt_metadata(source: HubMetadataError) -> Self {
        Self::new(HubCacheReadErrorKind::Corrupt(Some(source)))
    }

    pub(super) fn tree_metadata(source: HubMetadataError) -> Self {
        if source.is_unknown_version() {
            Self::new(HubCacheReadErrorKind::UnsupportedVersion(source))
        } else {
            Self::corrupt_metadata(source)
        }
    }

    fn unsafe_path(source: ValidationError) -> Self {
        Self::new(HubCacheReadErrorKind::Unsafe(source))
    }

    fn unsafe_symlink() -> Self {
        Self::unsafe_path(ValidationError::new(
            "Hub cache snapshot symlink",
            ValidationErrorKind::UnsafePath,
        ))
    }

    pub(super) fn is_missing(&self) -> bool {
        matches!(self.kind.as_ref(), HubCacheReadErrorKind::Missing)
    }

    pub(super) fn is_incomplete(&self) -> bool {
        matches!(self.kind.as_ref(), HubCacheReadErrorKind::Incomplete)
    }

    pub(super) fn is_corrupt(&self) -> bool {
        matches!(self.kind.as_ref(), HubCacheReadErrorKind::Corrupt(_))
    }

    pub(super) fn is_unsupported_version(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            HubCacheReadErrorKind::UnsupportedVersion(_)
        )
    }

    pub(super) fn is_unsafe(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            HubCacheReadErrorKind::Unsafe(_) | HubCacheReadErrorKind::UnsafeFileSystem(_)
        ) || matches!(
            self.kind.as_ref(),
            HubCacheReadErrorKind::Validation(source) if source.is_unsafe_path()
        )
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    #[cfg(test)]
    fn io_kind(&self) -> Option<io::ErrorKind> {
        match self.kind.as_ref() {
            HubCacheReadErrorKind::Io(source) | HubCacheReadErrorKind::UnsafeFileSystem(source) => {
                Some(source.kind())
            }
            HubCacheReadErrorKind::Validation(_)
            | HubCacheReadErrorKind::Missing
            | HubCacheReadErrorKind::Incomplete
            | HubCacheReadErrorKind::Corrupt(_)
            | HubCacheReadErrorKind::UnsupportedVersion(_)
            | HubCacheReadErrorKind::Unsafe(_) => None,
        }
    }
}

impl Display for HubCacheReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind.as_ref() {
            HubCacheReadErrorKind::Io(_) => "Hub cache filesystem operation failed",
            HubCacheReadErrorKind::UnsafeFileSystem(_) => "Hub cache filesystem path is unsafe",
            HubCacheReadErrorKind::Validation(_) => "Hub cache identity validation failed",
            HubCacheReadErrorKind::Missing => "Hub cache revision is missing",
            HubCacheReadErrorKind::Incomplete => "Hub cache revision is incomplete",
            HubCacheReadErrorKind::Corrupt(_) => "Hub cache metadata is corrupt",
            HubCacheReadErrorKind::UnsupportedVersion(_) => {
                "Hub cache metadata version is unsupported"
            }
            HubCacheReadErrorKind::Unsafe(_) => "Hub cache path mapping is unsafe",
        };
        formatter.write_str(message)
    }
}

impl Error for HubCacheReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            HubCacheReadErrorKind::Validation(source) | HubCacheReadErrorKind::Unsafe(source) => {
                Some(source)
            }
            HubCacheReadErrorKind::Corrupt(Some(source))
            | HubCacheReadErrorKind::UnsupportedVersion(source) => Some(source),
            HubCacheReadErrorKind::Missing
            | HubCacheReadErrorKind::Incomplete
            | HubCacheReadErrorKind::Corrupt(None)
            | HubCacheReadErrorKind::Io(_)
            | HubCacheReadErrorKind::UnsafeFileSystem(_) => None,
        }
    }
}

impl From<io::Error> for HubCacheReadError {
    fn from(source: io::Error) -> Self {
        let unsafe_path = is_unsafe_cache_path_error(&source);
        let source = SanitizedIo::new(&source);
        if unsafe_path {
            Self::new(HubCacheReadErrorKind::UnsafeFileSystem(source))
        } else {
            Self::new(HubCacheReadErrorKind::Io(source))
        }
    }
}

impl From<ValidationError> for HubCacheReadError {
    fn from(source: ValidationError) -> Self {
        Self::new(HubCacheReadErrorKind::Validation(source))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tempfile::TempDir;

    use crate::RepositoryId;

    use super::*;
    use crate::cache::publication::OsFileSystem;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const REF_FIXTURE: &[u8] =
        include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/standard-ref-main");
    const TREE_FIXTURE: &[u8] =
        include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/tree-v1.json");
    const CONFIG_GIT_BLOB: &str = "04204c7c9d0e243cb4d1456ba552ab505beb8ea5";
    const CONFIG_SHA256: &str = "f612b89bcdbc401379f644d7e48572e3470f77dcd4c39416405d80952ad7089e";
    const MODEL_SHA256: &str = "9cb7487000bc86ac36ce83c4acfabe8878552be99572a6770f65ab1d048a5c48";

    struct Fixture {
        directory: TempDir,
        reader: HubCacheReader,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let reader = HubCacheReader::shared(directory.path(), &endpoint, &spec, &OsFileSystem)?;
            Ok(Self { directory, reader })
        }

        fn repository(&self) -> &Path {
            self.reader.layout.repository_directory()
        }

        fn write(&self, relative: &str, bytes: &[u8]) -> Result<(), std::io::Error> {
            let path = self.repository().join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, bytes)
        }

        fn write_tree(&self, json: &str) -> Result<(), std::io::Error> {
            self.write(&format!("trees/{COMMIT}.json"), json.as_bytes())
        }
    }

    fn copy_directory(source: &Path, destination: &Path) -> io::Result<()> {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let destination_path = destination.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                copy_directory(&entry.path(), &destination_path)?;
            } else {
                fs::copy(entry.path(), destination_path)?;
            }
        }
        Ok(())
    }

    #[test]
    fn cached_index_resolves_a_slash_ref_and_reads_its_tree()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write("refs/refs/pr/17", REF_FIXTURE)?;
        fixture.write(&format!("trees/{COMMIT}.json"), TREE_FIXTURE)?;

        let index = fixture.reader.read_index(&Revision::parse("refs/pr/17")?)?;

        assert_eq!(index.commit().as_str(), COMMIT);
        assert_eq!(index.tree().files().len(), 2);
        Ok(())
    }

    #[test]
    fn full_commit_reads_the_tree_without_a_ref_record() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write(&format!("trees/{COMMIT}.json"), TREE_FIXTURE)?;

        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        assert_eq!(index.commit(), &CommitId::parse(COMMIT)?);
        Ok(())
    }

    #[test]
    fn reader_distinguishes_missing_ref_from_incomplete_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;

        let missing = fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted a missing ref");
        assert!(missing.is_missing());

        fixture.write("refs/main", REF_FIXTURE)?;
        let incomplete = fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted a ref whose tree is missing");
        assert!(incomplete.is_incomplete());
        Ok(())
    }

    #[test]
    fn reader_distinguishes_corruption_unknown_versions_and_unsafe_refs()
    -> Result<(), Box<dyn std::error::Error>> {
        let corrupt_fixture = Fixture::new()?;
        corrupt_fixture.write("refs/main", format!("{COMMIT}\n").as_bytes())?;
        let corrupt = corrupt_fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted a corrupt ref");
        assert!(corrupt.is_corrupt());

        let unknown_fixture = Fixture::new()?;
        unknown_fixture.write("refs/main", REF_FIXTURE)?;
        unknown_fixture.write(
            &format!("trees/{COMMIT}.json"),
            br#"{"format_version":2,"future_encoding":true}"#,
        )?;
        let unsupported = unknown_fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted an unsupported tree version");
        assert!(unsupported.is_unsupported_version());

        let unsafe_fixture = Fixture::new()?;
        let unsafe_ref = unsafe_fixture
            .reader
            .read_index(&Revision::parse("../escape")?)
            .expect_err("joined an unsafe ref to the cache path");
        assert!(unsafe_ref.is_unsafe());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn reader_rejects_a_repository_junction_as_unsafe() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let outside = TempDir::new()?;
        create_dir_junction(outside.path(), fixture.repository())?;

        let error = fixture
            .reader
            .read_index(&Revision::parse(COMMIT)?)
            .expect_err("followed a repository junction outside the cache root");

        assert!(error.is_unsafe());
        fs::remove_dir(fixture.repository())?;
        Ok(())
    }

    #[test]
    fn reader_rejects_non_file_records_and_oversized_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory_fixture = Fixture::new()?;
        fs::create_dir_all(directory_fixture.repository().join("refs/main"))?;
        let wrong_kind = directory_fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted a directory as a ref record");
        assert!(wrong_kind.is_corrupt());

        let oversized_fixture = Fixture::new()?;
        oversized_fixture.write("refs/main", &[b'a'; MAX_REF_BYTES + 1])?;
        let oversized = oversized_fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("accepted an oversized ref record");
        assert!(oversized.is_corrupt());
        Ok(())
    }

    #[test]
    fn oversized_tree_is_rejected_before_its_reader_is_consumed() {
        struct ReadAttempt(Arc<AtomicBool>);

        impl Read for ReadAttempt {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                self.0.store(true, Ordering::Release);
                Err(io::Error::other("oversized tree reader was consumed"))
            }
        }

        #[derive(Debug)]
        struct OversizedTreeDirectory {
            read_attempted: Arc<AtomicBool>,
        }

        impl CacheDirectory for OversizedTreeDirectory {
            fn open_dir_nofollow(&self, _path: &Path) -> io::Result<Arc<dyn CacheDirectory>> {
                Err(io::Error::other("nested directory open is not supported"))
            }

            fn open_regular(&self, _path: &Path) -> io::Result<RegularFileOpen> {
                Ok(RegularFileOpen::File {
                    reader: Box::new(ReadAttempt(Arc::clone(&self.read_attempted))),
                    size: u64::try_from(MAX_TREE_BYTES)
                        .map_or(u64::MAX, |size| size.saturating_add(1)),
                })
            }

            fn entry_kind(&self, _path: &Path) -> io::Result<EntryKind> {
                Ok(EntryKind::RegularFile)
            }

            fn read_link(&self, _path: &Path) -> io::Result<PathBuf> {
                Err(io::Error::other("link reads are not supported"))
            }
        }

        let read_attempted = Arc::new(AtomicBool::new(false));
        let directory = OversizedTreeDirectory {
            read_attempted: Arc::clone(&read_attempted),
        };

        let error = HubCacheReader::read_regular_record(
            &directory,
            Path::new("trees/oversized.json"),
            MAX_TREE_BYTES,
            MissingRecord::Incomplete,
        )
        .expect_err("accepted an oversized tree record");

        assert!(error.is_corrupt());
        assert!(!read_attempted.load(Ordering::Acquire));
    }

    #[test]
    fn arbitrary_directory_and_reader_errors_are_redacted_without_losing_the_io_kind() {
        for return_reader in [false, true] {
            let directory = SecretDirectory { return_reader };
            let error = HubCacheReader::read_regular_record(
                &directory,
                Path::new("trees/secret.json"),
                1,
                MissingRecord::Incomplete,
            )
            .expect_err("accepted an injected I/O failure");

            assert_eq!(error.io_kind(), Some(io::ErrorKind::PermissionDenied));
            assert_secret_absent_from_error_chain(&error);
        }
    }

    const SECRET_ERROR_SENTINEL: &str = "hf_secret_signed_url_sentinel";

    struct SecretError;

    impl fmt::Debug for SecretError {
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
            Err(secret_io_error())
        }
    }

    #[derive(Debug)]
    struct SecretDirectory {
        return_reader: bool,
    }

    impl CacheDirectory for SecretDirectory {
        fn open_dir_nofollow(&self, _path: &Path) -> io::Result<Arc<dyn CacheDirectory>> {
            Err(io::Error::other("nested directory open is not supported"))
        }

        fn open_regular(&self, _path: &Path) -> io::Result<RegularFileOpen> {
            if self.return_reader {
                Ok(RegularFileOpen::File {
                    reader: Box::new(SecretReader),
                    size: 1,
                })
            } else {
                Err(secret_io_error())
            }
        }

        fn entry_kind(&self, _path: &Path) -> io::Result<EntryKind> {
            Ok(EntryKind::RegularFile)
        }

        fn read_link(&self, _path: &Path) -> io::Result<PathBuf> {
            Err(io::Error::other("link reads are not supported"))
        }
    }

    fn secret_io_error() -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, SecretError)
    }

    fn assert_secret_absent_from_error_chain(error: &(dyn Error + 'static)) {
        let mut current = Some(error);
        while let Some(source) = current {
            assert!(!source.to_string().contains(SECRET_ERROR_SENTINEL));
            assert!(!format!("{source:?}").contains(SECRET_ERROR_SENTINEL));
            current = source.source();
        }
    }

    #[test]
    fn reader_reuses_the_python_written_regular_snapshot_forms()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("huggingface_hub-v1.24.0")
            .join("cache");
        let endpoint = Endpoint::hugging_face();
        let cases = [
            (
                RepositorySpec::model(RepositoryId::parse("fixture-model")?),
                Revision::parse("main")?,
                crate::RepoPath::parse("config.json")?,
                HubSnapshotFileForm::SnapshotOnly,
                "da2dcf17b64bf30e3ac0d1353b6a7fcdbb75a3255c953c8c9c38cb7f4bc92dcc",
            ),
            (
                RepositorySpec::dataset(RepositoryId::parse("fixture-org/fixture-dataset")?),
                Revision::parse("refs/pr/7")?,
                crate::RepoPath::parse("data/train.jsonl")?,
                HubSnapshotFileForm::CopiedWithBlob,
                "c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844",
            ),
        ];

        for (spec, revision, path, expected_form, expected_digest) in cases {
            let reader = HubCacheReader::shared(&cache_root, &endpoint, &spec, &OsFileSystem)?;
            let index = reader.read_index(&revision)?;
            let file = reader.read_snapshot_file(&index, &path)?;

            assert_eq!(file.form(), expected_form);
            assert_eq!(file.digest().to_string(), expected_digest);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_reuses_the_python_written_space_fixture_with_a_runtime_symlink()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        const SPACE_COMMIT: &str = "3333333333333333333333333333333333333333";
        const SPACE_BLOB: &str = "8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05";
        const SPACE_DIGEST: &str =
            "329ab5ce0c3179d1dfb17f3fddc1c420ec9f04d2ad1e6a3bea07e09b278b806e";

        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("huggingface_hub-v1.24.0")
            .join("cache")
            .join("spaces--fixture-org--fixture-space");
        let directory = TempDir::new()?;
        let repository = directory.path().join("spaces--fixture-org--fixture-space");
        copy_directory(&source, &repository)?;
        let link = repository.join(format!("snapshots/{SPACE_COMMIT}/src/app.py"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../../blobs/{SPACE_BLOB}"), &link)?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::space(RepositoryId::parse("fixture-org/fixture-space")?);
        let reader = HubCacheReader::shared(directory.path(), &endpoint, &spec, &OsFileSystem)?;
        let index = reader.read_index(&Revision::parse("main")?)?;

        let file = reader.read_snapshot_file(&index, &crate::RepoPath::parse("src/app.py")?)?;

        assert_eq!(file.form(), HubSnapshotFileForm::RelativeSymlink);
        assert_eq!(file.digest().to_string(), SPACE_DIGEST);
        Ok(())
    }

    #[test]
    fn reads_a_snapshot_only_regular_file_and_validates_its_git_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write(&format!("snapshots/{COMMIT}/config.json"), b"config\n")?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let file = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)?;

        assert_eq!(file.form(), HubSnapshotFileForm::SnapshotOnly);
        assert_eq!(file.size(), 7);
        assert_eq!(file.digest().to_string(), CONFIG_SHA256);
        assert_eq!(
            file.path(),
            fixture
                .repository()
                .join(format!("snapshots/{COMMIT}/config.json"))
        );
        Ok(())
    }

    #[test]
    fn reads_a_regular_snapshot_copy_with_its_retained_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"nested/model.bin":{{"size":11,"blob_id":"1111111111111111111111111111111111111111","lfs_sha256":"{MODEL_SHA256}","lfs_size":11}}}}}}"#,
        ))?;
        fixture.write(
            &format!("snapshots/{COMMIT}/nested/model.bin"),
            b"model bytes",
        )?;
        fixture.write(&format!("blobs/{MODEL_SHA256}"), b"model bytes")?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let file = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("nested/model.bin")?)?;

        assert_eq!(file.form(), HubSnapshotFileForm::CopiedWithBlob);
        assert_eq!(file.digest().to_string(), MODEL_SHA256);
        assert_eq!(
            file.blob_path(),
            Some(
                fixture
                    .repository()
                    .join(format!("blobs/{MODEL_SHA256}"))
                    .as_path()
            )
        );
        Ok(())
    }

    #[test]
    fn regular_snapshot_validation_rejects_missing_or_changed_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let missing = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("accepted a missing snapshot entry");
        assert!(missing.is_incomplete());

        fixture.write(&format!("snapshots/{COMMIT}/config.json"), b"changed")?;
        let changed = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("accepted content with the wrong Git blob identity");
        assert!(changed.is_corrupt());
        Ok(())
    }

    #[test]
    fn regular_snapshot_validation_rejects_a_conflicting_retained_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"model.bin":{{"size":11,"blob_id":"1111111111111111111111111111111111111111","lfs_sha256":"{MODEL_SHA256}","lfs_size":11}}}}}}"#,
        ))?;
        fixture.write(&format!("snapshots/{COMMIT}/model.bin"), b"model bytes")?;
        fixture.write(&format!("blobs/{MODEL_SHA256}"), b"wrong bytes")?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let error = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("model.bin")?)
            .expect_err("accepted a conflicting retained blob");

        assert!(error.is_corrupt());
        Ok(())
    }

    #[test]
    fn content_validation_reads_only_the_first_excess_byte()
    -> Result<(), Box<dyn std::error::Error>> {
        struct CountingReader {
            remaining: usize,
            consumed: usize,
        }

        impl Read for CountingReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = buffer.len().min(self.remaining);
                buffer[..count].fill(b'x');
                self.remaining -= count;
                self.consumed += count;
                Ok(count)
            }
        }

        let entry = HubTreeEntry::new(0, "opaque-validator")?;
        let mut reader = CountingReader {
            remaining: 64 * 1024,
            consumed: 0,
        };

        let error = validate_content(&mut reader, &entry)
            .expect_err("accepted content larger than the tree entry");

        assert!(error.is_corrupt());
        assert_eq!(reader.consumed, 1);
        Ok(())
    }

    #[test]
    fn streaming_validation_copies_exact_bytes_and_rejects_invalid_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let entry = HubTreeEntry::new(7, CONFIG_GIT_BLOB)?;
        let mut valid_reader = b"config\n".as_slice();
        let mut valid_copy = Vec::new();

        let (size, digest) = copy_and_validate_content(&mut valid_reader, &mut valid_copy, &entry)?;

        assert_eq!(valid_copy, b"config\n");
        assert_eq!(size, 7);
        assert_eq!(digest.to_string(), CONFIG_SHA256);

        let mut invalid_reader = b"changed".as_slice();
        let mut invalid_copy = Vec::new();
        let error = copy_and_validate_content(&mut invalid_reader, &mut invalid_copy, &entry)
            .expect_err("accepted copied content with the wrong Git blob identity");

        assert!(error.is_corrupt());
        Ok(())
    }

    #[test]
    fn malformed_lfs_identity_is_rejected_before_content_is_read()
    -> Result<(), Box<dyn std::error::Error>> {
        struct ReadAttempt {
            attempted: bool,
        }

        impl Read for ReadAttempt {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                self.attempted = true;
                Err(io::Error::other("content must not be read"))
            }
        }

        let entry = HubTreeEntry::new(1, "opaque-pointer")?.with_lfs("A".repeat(64), 1)?;
        let mut reader = ReadAttempt { attempted: false };

        let error = validate_content(&mut reader, &entry)
            .expect_err("accepted a non-lowercase LFS SHA-256 identity");

        assert!(error.is_corrupt());
        assert!(!reader.attempted);
        Ok(())
    }

    #[test]
    fn malformed_or_inconsistent_lfs_metadata_is_always_corrupt()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_digests = ["a".repeat(63), "g".repeat(64), "A".repeat(64)];
        for digest in invalid_digests {
            let entry = HubTreeEntry::new(1, "opaque-pointer")?.with_lfs(digest, 1)?;
            assert!(
                compatible_blob_key(&entry)
                    .expect_err("accepted malformed LFS metadata")
                    .is_corrupt()
            );
        }

        let entry = HubTreeEntry::new(1, "opaque-pointer")?.with_lfs("a".repeat(64), 2)?;
        assert!(
            compatible_blob_key(&entry)
                .expect_err("accepted an LFS size that disagrees with the tree entry")
                .is_corrupt()
        );
        Ok(())
    }

    #[test]
    fn opaque_non_lfs_identity_still_computes_a_local_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"opaque";
        let entry = HubTreeEntry::new(bytes.len() as u64, "future-validator")?;
        let mut reader = bytes.as_slice();

        let (size, digest) = validate_content(&mut reader, &entry)?;

        assert_eq!(size, bytes.len() as u64);
        assert_eq!(digest, BlobDigest::for_bytes(bytes));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_reader_rejects_a_final_file_reparse_point() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::windows::fs::symlink_file;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        let replacement_directory = TempDir::new()?;
        let replacement = replacement_directory.path().join("config.json");
        fs::write(&replacement, b"config\n")?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        if let Err(error) = symlink_file(&replacement, &link) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return Ok(());
            }
            return Err(error.into());
        }
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let error = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a final file reparse point");

        assert!(error.is_corrupt());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_reader_rejects_a_directory_reparse_point_below_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::symlink_dir;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write("redirected-snapshot/config.json", b"config\n")?;
        fs::create_dir_all(fixture.repository().join("snapshots"))?;
        let target = fixture.repository().join("redirected-snapshot");
        let link = fixture.repository().join(format!("snapshots/{COMMIT}"));
        if let Err(error) = symlink_dir(target, link) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return Ok(());
            }
            return Err(error.into());
        }
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a directory reparse point below the repository");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reads_a_contained_relative_snapshot_symlink() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"nested/config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write(&format!("blobs/{CONFIG_GIT_BLOB}"), b"config\n")?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/nested/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let file = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("nested/config.json")?)?;

        assert_eq!(file.form(), HubSnapshotFileForm::RelativeSymlink);
        assert_eq!(file.digest().to_string(), CONFIG_SHA256);
        assert_eq!(
            file.blob_path(),
            Some(
                fixture
                    .repository()
                    .join(format!("blobs/{CONFIG_GIT_BLOB}"))
                    .as_path()
            )
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_a_ref_ancestor_redirected_inside_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write("redirected-refs/main", REF_FIXTURE)?;
        symlink("redirected-refs", fixture.repository().join("refs"))?;

        fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("followed a symlinked refs ancestor");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_a_tree_ancestor_redirected_inside_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write("refs/main", REF_FIXTURE)?;
        fixture.write(&format!("redirected-trees/{COMMIT}.json"), TREE_FIXTURE)?;
        symlink("redirected-trees", fixture.repository().join("trees"))?;

        fixture
            .reader
            .read_index(&Revision::parse("main")?)
            .expect_err("followed a symlinked trees ancestor");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_a_snapshot_commit_ancestor_redirected_inside_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write("redirected-snapshot/config.json", b"config\n")?;
        fs::create_dir_all(fixture.repository().join("snapshots"))?;
        symlink(
            "../redirected-snapshot",
            fixture.repository().join(format!("snapshots/{COMMIT}")),
        )?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a symlinked snapshot commit ancestor");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_a_nested_snapshot_ancestor_redirected_inside_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"nested/config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write("redirected-nested/config.json", b"config\n")?;
        let snapshot = fixture.repository().join(format!("snapshots/{COMMIT}"));
        fs::create_dir_all(&snapshot)?;
        symlink("../../redirected-nested", snapshot.join("nested"))?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("nested/config.json")?)
            .expect_err("followed a symlinked nested snapshot ancestor");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_a_blob_ancestor_redirected_inside_the_repository()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write(&format!("redirected-blobs/{CONFIG_GIT_BLOB}"), b"config\n")?;
        symlink("redirected-blobs", fixture.repository().join("blobs"))?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a symlinked blobs ancestor");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn stable_open_rechecks_the_final_entry_without_following_it()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        #[derive(Debug)]
        struct SwapBeforeOpenDirectory {
            inner: Arc<dyn CacheDirectory>,
            relative_path: PathBuf,
            absolute_path: PathBuf,
            replacement: PathBuf,
            swapped: AtomicBool,
        }

        impl CacheDirectory for SwapBeforeOpenDirectory {
            fn open_dir_nofollow(&self, path: &Path) -> io::Result<Arc<dyn CacheDirectory>> {
                self.inner.open_dir_nofollow(path)
            }

            fn open_regular(&self, path: &Path) -> io::Result<RegularFileOpen> {
                if path == self.relative_path && !self.swapped.swap(true, Ordering::AcqRel) {
                    fs::remove_file(&self.absolute_path)?;
                    symlink(&self.replacement, &self.absolute_path)?;
                }
                self.inner.open_regular(path)
            }

            fn entry_kind(&self, path: &Path) -> io::Result<EntryKind> {
                self.inner.entry_kind(path)
            }

            fn read_link(&self, path: &Path) -> io::Result<PathBuf> {
                self.inner.read_link(path)
            }
        }

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        let absolute_path = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/config.json"));
        fixture.write(&format!("snapshots/{COMMIT}/config.json"), b"config\n")?;
        let replacement_directory = TempDir::new()?;
        let replacement = replacement_directory.path().join("config.json");
        fs::write(&replacement, b"config\n")?;
        let mut index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;
        let inner = Arc::clone(&index.directory);
        index.directory = Arc::new(SwapBeforeOpenDirectory {
            inner,
            relative_path: PathBuf::from(format!("snapshots/{COMMIT}/config.json")),
            absolute_path,
            replacement,
            swapped: AtomicBool::new(false),
        });

        let error = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a final symlink installed between classification and open");

        assert!(error.is_unsafe());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlink_reader_rejects_noncanonical_parent_traversal()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"nested/config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        fixture.write(&format!("blobs/{CONFIG_GIT_BLOB}"), b"config\n")?;
        let outside = TempDir::new()?;
        symlink(outside.path(), fixture.repository().join("evil"))?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/nested/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../../evil/../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let error = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("nested/config.json")?)
            .expect_err("accepted a noncanonical symlink that can escape through an ancestor");

        assert!(error.is_unsafe());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlink_reader_rejects_a_blob_directory_redirected_outside_the_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        let outside = TempDir::new()?;
        fs::write(outside.path().join(CONFIG_GIT_BLOB), b"config\n")?;
        symlink(outside.path(), fixture.repository().join("blobs"))?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("followed a blob-directory symlink outside the cache boundary");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn relative_cache_root_with_a_leading_parent_reads_a_canonical_symlink()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let current = std::env::current_dir()?;
        let parent = current.parent().ok_or("current directory has no parent")?;
        let directory = tempfile::Builder::new()
            .prefix("hf-store-relative-root-")
            .tempdir_in(parent)?;
        let directory_name = directory
            .path()
            .file_name()
            .ok_or("temporary directory has no file name")?;
        let root = PathBuf::from("..").join(directory_name).join("cache");
        fs::create_dir_all(&root)?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let reader = HubCacheReader::shared(&root, &endpoint, &spec, &OsFileSystem)?;
        let repository = reader.layout.repository_directory();
        let tree_path = repository.join(format!("trees/{COMMIT}.json"));
        fs::create_dir_all(tree_path.parent().ok_or("tree has no parent")?)?;
        fs::write(
            tree_path,
            format!(
                r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
            ),
        )?;
        let blob = repository.join(format!("blobs/{CONFIG_GIT_BLOB}"));
        fs::create_dir_all(blob.parent().ok_or("blob has no parent")?)?;
        fs::write(blob, b"config\n")?;
        let link = repository.join(format!("snapshots/{COMMIT}/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = reader.read_index(&Revision::parse(COMMIT)?)?;

        let file = reader.read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)?;

        assert_eq!(file.form(), HubSnapshotFileForm::RelativeSymlink);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlink_reader_rejects_escaping_and_absolute_targets()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        for target in ["../../../../outside", "/absolute/outside"] {
            let fixture = Fixture::new()?;
            fixture.write_tree(&format!(
                r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
            ))?;
            let link = fixture
                .repository()
                .join(format!("snapshots/{COMMIT}/config.json"));
            fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
            symlink(target, &link)?;
            let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

            let error = fixture
                .reader
                .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
                .expect_err("accepted an escaping snapshot symlink");
            assert!(error.is_unsafe());
        }

        Ok(())
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

    #[cfg(unix)]
    #[test]
    fn dangling_canonical_symlink_is_incomplete() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let fixture = Fixture::new()?;
        fixture.write_tree(&format!(
            r#"{{"format_version":1,"files":{{"config.json":{{"size":7,"blob_id":"{CONFIG_GIT_BLOB}"}}}}}}"#,
        ))?;
        let link = fixture
            .repository()
            .join(format!("snapshots/{COMMIT}/config.json"));
        fs::create_dir_all(link.parent().ok_or("snapshot link has no parent")?)?;
        symlink(format!("../../blobs/{CONFIG_GIT_BLOB}"), &link)?;
        let index = fixture.reader.read_index(&Revision::parse(COMMIT)?)?;

        let error = fixture
            .reader
            .read_snapshot_file(&index, &crate::RepoPath::parse("config.json")?)
            .expect_err("accepted a broken snapshot symlink");

        assert!(error.is_incomplete());
        Ok(())
    }
}
