//! Capability-rooted filesystem mutation primitives for cache state.

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use cap_fs_ext::OpenOptionsSyncExt;
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};

#[cfg(windows)]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

#[derive(Debug)]
struct UnsafeCachePathError(&'static str);

impl fmt::Display for UnsafeCachePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for UnsafeCachePathError {}

pub(super) fn is_unsafe_cache_path_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<UnsafeCachePathError>())
        .is_some()
}

pub(super) fn unsafe_cache_path(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, UnsafeCachePathError(message))
}

#[cfg(windows)]
pub(super) fn is_reparse_point(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
pub(super) const fn is_reparse_point(_metadata: &cap_std::fs::Metadata) -> bool {
    false
}

fn is_redirect(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink() || is_reparse_point(metadata)
}

/// The observed kind of an entry without following its final component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RootedEntryKind {
    /// No entry currently exists at the requested location.
    Missing,
    /// The entry is a regular file.
    RegularFile,
    /// The entry is a directory.
    Directory,
    /// The entry is a symbolic link or another filesystem-specific object.
    Other,
}

/// The result of a bounded, no-follow regular-file read.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum RootedRead {
    /// No entry currently exists at the requested location.
    Missing,
    /// The entry exists but is not a regular file.
    Other,
    /// The complete regular-file bytes were read within the requested bound.
    Bytes(Vec<u8>),
}

/// The outcome of create-once publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CreateOnceOutcome {
    /// This call published the destination.
    Created,
    /// Another complete regular file already occupied the destination.
    Existing,
}

/// The outcome of attempting to create a relative symbolic link once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RelativeSymlinkOutcome {
    /// This call created the symbolic link.
    Created,
    /// An entry already occupied the destination and was left unchanged.
    Existing,
    /// Symbolic-link creation is unavailable on this platform or filesystem.
    Unsupported,
}

/// A validated unique name fragment for same-directory staging files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StagingName(Box<str>);

impl StagingName {
    /// Validates an operation-unique ASCII staging name fragment.
    pub(super) fn new(value: &str) -> io::Result<Self> {
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(invalid_input("invalid cache staging name"));
        }
        Ok(Self(value.into()))
    }
}

impl fmt::Display for StagingName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub(super) trait RootedWrite: Write + Send {
    fn sync_all(&self) -> io::Result<()>;
}

impl RootedWrite for cap_std::fs::File {
    fn sync_all(&self) -> io::Result<()> {
        cap_std::fs::File::sync_all(self)
    }
}

pub(super) trait RootedLockGuard: fmt::Debug + Send {}

pub(super) enum RootedRegularFile {
    File {
        reader: Box<dyn Read + Send>,
        size: u64,
    },
    Missing,
    Other,
}

pub(super) trait RootedFileSystem: fmt::Debug + Send + Sync {
    fn ensure_dir(&self, path: &Path) -> io::Result<()>;
    fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind>;
    fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile>;
    fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead>;
    fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn install_staged_create_once(
        &self,
        staging: &Path,
        destination: &Path,
    ) -> io::Result<CreateOnceOutcome>;
    fn create_once(
        &self,
        path: &Path,
        bytes: &[u8],
        staging: &StagingName,
    ) -> io::Result<CreateOnceOutcome>;
    fn create_relative_symlink_once(
        &self,
        _path: &Path,
        _target: &Path,
    ) -> io::Result<RelativeSymlinkOutcome> {
        Ok(RelativeSymlinkOutcome::Unsupported)
    }
    fn copy_regular_create_once(
        &self,
        source: &Path,
        destination: &Path,
        staging: &StagingName,
    ) -> io::Result<CreateOnceOutcome> {
        match self.entry_kind(destination)? {
            RootedEntryKind::Missing => {}
            RootedEntryKind::RegularFile => return Ok(CreateOnceOutcome::Existing),
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                return Err(invalid_data("cache copy destination is not a regular file"));
            }
        }

        let (mut reader, expected_size) = match self.open_regular(source)? {
            RootedRegularFile::File { reader, size } => (reader, size),
            RootedRegularFile::Missing => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "cache copy source does not exist",
                ));
            }
            RootedRegularFile::Other => {
                return Err(invalid_data("cache copy source is not a regular file"));
            }
        };

        let staging_path = staging_path(destination, staging)?;
        let mut writer = self.create_new(&staging_path)?;
        let staged = io::copy(&mut reader, &mut writer)
            .and_then(|copied| {
                if copied == expected_size {
                    Ok(())
                } else {
                    Err(invalid_data("cache copy source changed while being copied"))
                }
            })
            .and_then(|()| writer.sync_all());
        drop(writer);

        let publication = staged.and_then(|()| {
            let outcome = self.install_staged_create_once(&staging_path, destination)?;
            if outcome == CreateOnceOutcome::Existing
                && self.entry_kind(destination)? != RootedEntryKind::RegularFile
            {
                return Err(invalid_data("cache copy destination is not a regular file"));
            }
            Ok(outcome)
        });
        let cleanup = self.remove_file(&staging_path);
        finish_staging_cleanup(publication, cleanup)
    }
    fn replace(&self, path: &Path, bytes: &[u8], staging: &StagingName) -> io::Result<()>;
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn RootedLockGuard>>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
}

/// An open capability anchored to one caller-authorized cache root.
#[derive(Debug)]
pub(super) struct CacheRoot {
    root: Dir,
}

impl CacheRoot {
    /// Opens an existing directory as the only ambient filesystem authority.
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        let root = Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
        Ok(Self { root })
    }

    /// Takes ownership of an already-open caller-authorized cache root.
    pub(super) const fn from_dir(root: Dir) -> Self {
        Self { root }
    }

    fn open_dir_chain(&self, path: &Path) -> io::Result<Dir> {
        let mut directory = self.root.try_clone()?;
        for component in normal_components(path)? {
            directory = open_child_directory(&directory, component)?;
        }
        Ok(directory)
    }

    fn ensure_dir_chain(&self, path: &Path) -> io::Result<Dir> {
        let mut directory = self.root.try_clone()?;
        for component in normal_components(path)? {
            match open_child_directory(&directory, component) {
                Ok(next) => directory = next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match directory.create_dir(component) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error),
                    }
                    directory = open_child_directory(&directory, component)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(directory)
    }

    fn open_parent_and_name(&self, path: &Path, create: bool) -> io::Result<(Dir, PathBuf)> {
        let Some(Component::Normal(name)) = path.components().next_back() else {
            return Err(invalid_input(
                "cache path requires a final normal component",
            ));
        };
        let Some(parent) = path.parent() else {
            return Err(invalid_input("cache path has no parent"));
        };
        let directory = if create {
            self.ensure_dir_chain(parent)?
        } else {
            self.open_dir_chain(parent)?
        };
        Ok((directory, PathBuf::from(name)))
    }
}

impl RootedFileSystem for CacheRoot {
    fn ensure_dir(&self, path: &Path) -> io::Result<()> {
        let _directory = self.ensure_dir_chain(path)?;
        Ok(())
    }

    fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
        if path.as_os_str().is_empty() {
            return Ok(RootedEntryKind::Directory);
        }
        let (parent, name) = match self.open_parent_and_name(path, false) {
            Ok(location) => location,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RootedEntryKind::Missing);
            }
            Err(error) => return Err(error),
        };
        match parent.symlink_metadata(name) {
            Ok(metadata) if is_redirect(&metadata) => {
                Err(unsafe_cache_path("cache entry is a link or reparse point"))
            }
            Ok(metadata) if metadata.file_type().is_file() => Ok(RootedEntryKind::RegularFile),
            Ok(metadata) if metadata.file_type().is_dir() => Ok(RootedEntryKind::Directory),
            Ok(_metadata) => Ok(RootedEntryKind::Other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(RootedEntryKind::Missing),
            Err(error) => Err(error),
        }
    }

    fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
        let (parent, name) = match self.open_parent_and_name(path, false) {
            Ok(location) => location,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RootedRegularFile::Missing);
            }
            Err(error) => return Err(error),
        };
        match parent.symlink_metadata(&name) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RootedRegularFile::Missing);
            }
            Err(error) => return Err(error),
            Ok(metadata) if is_redirect(&metadata) => {
                return Err(unsafe_cache_path("cache file is a link or reparse point"));
            }
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_metadata) => return Ok(RootedRegularFile::Other),
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        #[cfg(unix)]
        options.nonblock(true);
        let file = match parent.open_with(name, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RootedRegularFile::Missing);
            }
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if is_reparse_point(&metadata) {
            return Err(unsafe_cache_path("opened cache file is a reparse point"));
        }
        if !metadata.file_type().is_file() {
            return Ok(RootedRegularFile::Other);
        }
        Ok(RootedRegularFile::File {
            reader: Box::new(file),
            size: metadata.len(),
        })
    }

    fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead> {
        let (mut reader, size) = match self.open_regular(path)? {
            RootedRegularFile::File { reader, size } => (reader, size),
            RootedRegularFile::Missing => return Ok(RootedRead::Missing),
            RootedRegularFile::Other => return Ok(RootedRead::Other),
        };
        let limit_u64 = u64::try_from(limit).map_err(io::Error::other)?;
        if size > limit_u64 {
            return Err(invalid_data(
                "cache record exceeds its configured size limit",
            ));
        }
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(limit_u64.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(invalid_data(
                "cache record exceeds its configured size limit",
            ));
        }
        Ok(RootedRead::Bytes(bytes))
    }

    fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>> {
        let (parent, name) = self.open_parent_and_name(path, true)?;
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        let file = parent.open_with(name, &options)?;
        let metadata = file.metadata()?;
        if is_reparse_point(&metadata) {
            return Err(unsafe_cache_path("created cache file is a reparse point"));
        }
        if !metadata.file_type().is_file() {
            return Err(invalid_data("created cache entry is not a regular file"));
        }
        Ok(Box::new(file))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let (parent, name) = self.open_parent_and_name(path, false)?;
        parent.remove_file(name)
    }

    fn install_staged_create_once(
        &self,
        staging: &Path,
        destination: &Path,
    ) -> io::Result<CreateOnceOutcome> {
        if self.entry_kind(staging)? != RootedEntryKind::RegularFile {
            return Err(invalid_data("cache staging entry is not a regular file"));
        }
        let (source_parent, source_name) = self.open_parent_and_name(staging, false)?;
        let (destination_parent, destination_name) =
            self.open_parent_and_name(destination, true)?;
        match source_parent.hard_link(source_name, &destination_parent, destination_name) {
            Ok(()) => Ok(CreateOnceOutcome::Created),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Ok(CreateOnceOutcome::Existing)
            }
            Err(error) => Err(error),
        }
    }

    fn create_once(
        &self,
        path: &Path,
        bytes: &[u8],
        staging: &StagingName,
    ) -> io::Result<CreateOnceOutcome> {
        match self.entry_kind(path)? {
            RootedEntryKind::Missing => {}
            RootedEntryKind::RegularFile => return Ok(CreateOnceOutcome::Existing),
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                return Err(invalid_data(
                    "cache create-once destination is not a regular file",
                ));
            }
        }
        let staging_path = staging_path(path, staging)?;
        let mut file = self.create_new(&staging_path)?;
        let staged = file.write_all(bytes).and_then(|()| file.sync_all());
        drop(file);
        let publication = staged.and_then(|()| {
            let outcome = self.install_staged_create_once(&staging_path, path)?;
            match outcome {
                CreateOnceOutcome::Created => {
                    match self.read_regular_bounded(path, bytes.len())? {
                        RootedRead::Bytes(actual) if actual == bytes => {}
                        RootedRead::Missing | RootedRead::Other | RootedRead::Bytes(_) => {
                            return Err(invalid_data(
                                "published cache record failed final validation",
                            ));
                        }
                    }
                }
                CreateOnceOutcome::Existing
                    if self.entry_kind(path)? != RootedEntryKind::RegularFile =>
                {
                    return Err(invalid_data(
                        "cache create-once destination is not a regular file",
                    ));
                }
                CreateOnceOutcome::Existing => {}
            }
            Ok(outcome)
        });
        let _cleanup_result = self.remove_file(&staging_path);
        publication
    }

    fn create_relative_symlink_once(
        &self,
        path: &Path,
        target: &Path,
    ) -> io::Result<RelativeSymlinkOutcome> {
        validate_relative_symlink_target(path, target)?;

        #[cfg(unix)]
        {
            let (parent, name) = self.open_parent_and_name(path, true)?;
            return match parent.symlink_contents(target, name) {
                Ok(()) => Ok(RelativeSymlinkOutcome::Created),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    Ok(RelativeSymlinkOutcome::Existing)
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
                    ) =>
                {
                    Ok(RelativeSymlinkOutcome::Unsupported)
                }
                Err(error) => Err(error),
            };
        }

        #[cfg(not(unix))]
        {
            Ok(RelativeSymlinkOutcome::Unsupported)
        }
    }

    fn replace(&self, path: &Path, bytes: &[u8], staging: &StagingName) -> io::Result<()> {
        match self.entry_kind(path)? {
            RootedEntryKind::Missing | RootedEntryKind::RegularFile => {}
            RootedEntryKind::Directory | RootedEntryKind::Other => {
                return Err(invalid_data(
                    "cache replacement destination is not a regular file",
                ));
            }
        }
        let staging_path = staging_path(path, staging)?;
        let mut file = self.create_new(&staging_path)?;
        let staged = file.write_all(bytes).and_then(|()| file.sync_all());
        drop(file);
        let replacement = staged
            .and_then(|()| {
                let (staging_parent, staging_name) =
                    self.open_parent_and_name(&staging_path, false)?;
                let (destination_parent, destination_name) =
                    self.open_parent_and_name(path, false)?;
                staging_parent.rename(staging_name, &destination_parent, destination_name)
            })
            .and_then(|()| match self.read_regular_bounded(path, bytes.len())? {
                RootedRead::Bytes(actual) if actual == bytes => Ok(()),
                RootedRead::Missing | RootedRead::Other | RootedRead::Bytes(_) => Err(
                    invalid_data("replaced cache record failed final validation"),
                ),
            });
        if replacement.is_err() {
            let _cleanup_result = self.remove_file(&staging_path);
        }
        replacement
    }

    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn RootedLockGuard>> {
        let (parent, name) = self.open_parent_and_name(path, true)?;
        match parent.symlink_metadata(&name) {
            Ok(metadata) if is_redirect(&metadata) => {
                return Err(unsafe_cache_path("cache lock is a link or reparse point"));
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(invalid_data("cache lock entry is not a regular file"));
            }
            Ok(_metadata) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let mut options = OpenOptions::new();
        options
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .follow(FollowSymlinks::No);
        let file = parent.open_with(name, &options)?;
        let metadata = file.metadata()?;
        if is_reparse_point(&metadata) {
            return Err(unsafe_cache_path("opened cache lock is a reparse point"));
        }
        if !metadata.file_type().is_file() {
            return Err(invalid_data("cache lock entry is not a regular file"));
        }
        let file = file.into_std();
        fs4::FileExt::lock(&file)?;
        Ok(Box::new(OsRootedLockGuard { _file: file }))
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        let directory = self.open_dir_chain(path)?;
        sync_open_directory(&directory)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let directory = self.open_dir_chain(path)?;
        directory
            .entries()?
            .map(|entry| entry.map(|entry| path.join(entry.file_name())))
            .collect()
    }
}

#[derive(Debug)]
struct OsRootedLockGuard {
    _file: File,
}

impl RootedLockGuard for OsRootedLockGuard {}

fn open_child_directory(parent: &Dir, name: &std::ffi::OsStr) -> io::Result<Dir> {
    let metadata = parent.symlink_metadata(name)?;
    if is_redirect(&metadata) {
        return Err(unsafe_cache_path(
            "cache directory component is a link or reparse point",
        ));
    }
    if !metadata.file_type().is_dir() {
        return Err(invalid_data("cache directory component is not a directory"));
    }

    let directory = parent.open_dir_nofollow(name)?;
    let opened = directory.dir_metadata()?;
    if is_reparse_point(&opened) {
        return Err(unsafe_cache_path(
            "opened cache directory is a reparse point",
        ));
    }
    if !opened.file_type().is_dir() {
        return Err(invalid_data("opened cache entry is not a directory"));
    }
    Ok(directory)
}

fn normal_components(path: &Path) -> io::Result<Vec<&std::ffi::OsStr>> {
    path.components()
        .map(|component| match component {
            Component::Normal(value) => Ok(value),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => Err(invalid_input(
                "cache capability path must contain only relative normal components",
            )),
        })
        .collect()
}

fn validate_relative_symlink_target(destination: &Path, target: &Path) -> io::Result<()> {
    let destination_components = normal_components(destination)?;
    if destination_components.is_empty() {
        return Err(invalid_input(
            "cache symlink destination requires a normal component",
        ));
    }
    if target.as_os_str().is_empty() {
        return Err(invalid_input("cache symlink target must not be empty"));
    }
    let mut depth = destination_components.len().saturating_sub(1);
    for component in target.components() {
        match component {
            Component::Normal(_) => depth = depth.saturating_add(1),
            Component::ParentDir => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    invalid_input("cache symlink target escapes the authorized root")
                })?;
            }
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {
                return Err(invalid_input(
                    "cache symlink target must be relative and contain only normal or parent components",
                ));
            }
        }
    }
    Ok(())
}

fn staging_path(path: &Path, staging: &StagingName) -> io::Result<PathBuf> {
    let Some(parent) = path.parent() else {
        return Err(invalid_input("cache path has no parent"));
    };
    Ok(parent.join(format!(".hf-store-{staging}.tmp")))
}

fn finish_staging_cleanup<T>(result: io::Result<T>, cleanup: io::Result<()>) -> io::Result<T> {
    match result {
        Err(error) => Err(error),
        Ok(value) => match cleanup {
            Ok(()) => Ok(value),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(value),
            Err(error) => Err(error),
        },
    }
}

#[cfg(unix)]
fn sync_open_directory(directory: &Dir) -> io::Result<()> {
    directory.open(".")?.sync_all()
}

#[cfg(not(unix))]
fn sync_open_directory(directory: &Dir) -> io::Result<()> {
    directory.dir_metadata().map(|_metadata| ())
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::Path;

    use tempfile::TempDir;

    #[cfg(unix)]
    use super::RelativeSymlinkOutcome;
    use super::{
        CacheRoot, CreateOnceOutcome, RootedEntryKind, RootedFileSystem, RootedRead, StagingName,
    };

    #[test]
    fn creates_missing_directories_and_reads_bounded_record() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;

        root.ensure_dir(Path::new("repos/model/sidecar"))?;
        let staging = StagingName::new("create-record")?;
        let outcome = root.create_once(
            Path::new("repos/model/sidecar/format.json"),
            b"format-v1",
            &staging,
        )?;

        assert_eq!(outcome, CreateOnceOutcome::Created);
        assert_eq!(
            root.read_regular_bounded(Path::new("repos/model/sidecar/format.json"), 32)?,
            RootedRead::Bytes(b"format-v1".to_vec())
        );
        assert_eq!(
            root.entry_kind(Path::new("repos/model/sidecar"))?,
            RootedEntryKind::Directory
        );
        Ok(())
    }

    #[test]
    fn create_once_preserves_the_first_complete_record() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;

        let first = root.create_once(
            Path::new("records/item"),
            b"first",
            &StagingName::new("first-writer")?,
        )?;
        let second = root.create_once(
            Path::new("records/item"),
            b"second",
            &StagingName::new("second-writer")?,
        )?;

        assert_eq!(first, CreateOnceOutcome::Created);
        assert_eq!(second, CreateOnceOutcome::Existing);
        assert_eq!(
            root.read_regular_bounded(Path::new("records/item"), 16)?,
            RootedRead::Bytes(b"first".to_vec())
        );
        Ok(())
    }

    #[test]
    fn replace_atomically_changes_a_complete_record() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let path = Path::new("refs/main");

        root.replace(path, b"old-complete", &StagingName::new("old")?)?;
        root.replace(path, b"new-complete", &StagingName::new("new")?)?;

        assert_eq!(
            root.read_regular_bounded(path, 32)?,
            RootedRead::Bytes(b"new-complete".to_vec())
        );
        Ok(())
    }

    #[test]
    fn rejects_parent_components_without_writing_outside_root() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("escaped-record");

        let error = root
            .replace(
                Path::new("../escaped-record"),
                b"escape",
                &StagingName::new("escape")?,
            )
            .expect_err("parent traversal must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!outside.try_exists()?);
        Ok(())
    }

    #[test]
    fn rejects_a_preexisting_link_ancestor_without_writing_outside_root() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("outside");
        fs::create_dir(&outside)?;
        if !create_dir_link(&outside, &fixture.cache.join("escape"))? {
            return Ok(());
        }

        let error = root
            .replace(
                Path::new("escape/record"),
                b"must-not-escape",
                &StagingName::new("link-escape")?,
            )
            .expect_err("a link ancestor must be rejected");

        assert_ne!(error.kind(), io::ErrorKind::NotFound);
        assert!(!outside.join("record").try_exists()?);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn rejects_a_junction_ancestor_without_writing_outside_root() -> io::Result<()> {
        use std::os::windows::fs::MetadataExt as _;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("junction-target");
        let junction = fixture.cache.join("escape-junction");
        fs::create_dir(&outside)?;
        create_dir_junction(&outside, &junction)?;
        assert_ne!(
            fs::symlink_metadata(&junction)?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT,
            0
        );

        let error = root
            .replace(
                Path::new("escape-junction/record"),
                b"must-not-escape",
                &StagingName::new("junction-escape")?,
            )
            .expect_err("a junction ancestor must be rejected");

        assert!(super::is_unsafe_cache_path_error(&error));
        assert!(!outside.join("record").try_exists()?);
        fs::remove_dir(junction)?;
        Ok(())
    }

    #[test]
    fn rejects_final_special_entries_without_changing_their_targets() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        fs::create_dir(fixture.cache.join("special"))?;

        let replace_error = root
            .replace(
                Path::new("special"),
                b"replacement",
                &StagingName::new("special-replace")?,
            )
            .expect_err("a directory destination must be rejected");
        let create_error = root
            .create_once(
                Path::new("special"),
                b"replacement",
                &StagingName::new("special-create")?,
            )
            .expect_err("a directory destination must be rejected");

        assert_eq!(replace_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(create_error.kind(), io::ErrorKind::InvalidData);
        assert!(fixture.cache.join("special").is_dir());
        Ok(())
    }

    #[test]
    fn rejects_final_file_links_for_writes_and_locks() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("outside-file");
        fs::write(&outside, b"outside")?;
        let link = fixture.cache.join("redirected-file");
        if !create_file_link(&outside, &link)? {
            return Ok(());
        }
        let relative = Path::new("redirected-file");

        root.replace(relative, b"replacement", &StagingName::new("replace-link")?)
            .expect_err("replacement followed a final file link");
        root.create_once(relative, b"new", &StagingName::new("create-link")?)
            .expect_err("create-once followed a final file link");
        root.lock_exclusive(relative)
            .expect_err("locking followed a final file link");

        assert_eq!(fs::read(outside)?, b"outside");
        Ok(())
    }

    #[test]
    fn rooted_locks_and_directory_sync_use_the_open_capability() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        root.ensure_dir(Path::new("locks"))?;

        let _guard = root.lock_exclusive(Path::new("locks/item.lock"))?;
        root.sync_directory(Path::new("locks"))?;

        assert_eq!(
            root.entry_kind(Path::new("locks/item.lock"))?,
            RootedEntryKind::RegularFile
        );
        Ok(())
    }

    #[test]
    fn bounded_read_rejects_oversized_records() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        root.replace(
            Path::new("record"),
            b"five!",
            &StagingName::new("oversized")?,
        )?;

        let error = root
            .read_regular_bounded(Path::new("record"), 4)
            .expect_err("oversized records must not be returned");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        Ok(())
    }

    #[test]
    fn staging_names_are_restricted_to_one_safe_component() {
        for invalid in [
            "",
            "../escape",
            "contains.dot",
            "contains/slash",
            "contains:ads",
        ] {
            let error = StagingName::new(invalid).expect_err("unsafe staging name must fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[cfg(unix)]
    #[test]
    fn relative_symlink_create_once_preserves_the_first_entry() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let path = Path::new("models--org--repo/snapshots/commit/config.json");
        let target = Path::new("../../blobs/object-id");

        let first = root.create_relative_symlink_once(path, target)?;
        let second = root.create_relative_symlink_once(path, Path::new("../../blobs/other"))?;

        assert_eq!(first, RelativeSymlinkOutcome::Created);
        assert_eq!(second, RelativeSymlinkOutcome::Existing);
        assert_eq!(fs::read_link(fixture.cache.join(path))?, target);
        Ok(())
    }

    #[test]
    fn relative_symlink_rejects_unsafe_targets_before_writing() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let destination = Path::new("snapshots/commit/file");

        for target in [
            Path::new(""),
            Path::new("."),
            Path::new("/absolute"),
            Path::new("../../../outside"),
        ] {
            let error = root
                .create_relative_symlink_once(destination, target)
                .expect_err("unsafe relative-link target must fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }

        assert!(!fixture.cache.join(destination).try_exists()?);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn relative_symlink_rejects_a_linked_destination_ancestor() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("outside-snapshots");
        fs::create_dir(&outside)?;
        std::os::unix::fs::symlink(&outside, fixture.cache.join("snapshots"))?;

        let error = root
            .create_relative_symlink_once(
                Path::new("snapshots/commit/file"),
                Path::new("../../blobs/object-id"),
            )
            .expect_err("a linked destination ancestor must fail");

        assert!(super::is_unsafe_cache_path_error(&error));
        assert!(!outside.join("commit/file").try_exists()?);
        Ok(())
    }

    #[test]
    fn regular_copy_create_once_is_independent_and_cleans_staging() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let source = Path::new("blobs/object-id");
        let destination = Path::new("snapshots/commit/model.bin");
        root.replace(source, b"immutable-source", &StagingName::new("source")?)?;

        let outcome = root.copy_regular_create_once(
            source,
            destination,
            &StagingName::new("snapshot-copy")?,
        )?;
        fs::write(fixture.cache.join(destination), b"changed-copy")?;

        assert_eq!(outcome, CreateOnceOutcome::Created);
        assert_eq!(fs::read(fixture.cache.join(source))?, b"immutable-source");
        assert!(
            !fixture
                .cache
                .join("snapshots/commit/.hf-store-snapshot-copy.tmp")
                .try_exists()?
        );
        Ok(())
    }

    #[test]
    fn regular_copy_rejects_a_link_source_without_publishing() -> io::Result<()> {
        let fixture = Fixture::new()?;
        let root = fixture.root()?;
        let outside = fixture.base.path().join("outside-blob");
        fs::write(&outside, b"outside")?;
        root.ensure_dir(Path::new("blobs"))?;
        if !create_file_link(&outside, &fixture.cache.join("blobs/redirect"))? {
            return Ok(());
        }
        let destination = Path::new("snapshots/commit/model.bin");

        let error = root
            .copy_regular_create_once(
                Path::new("blobs/redirect"),
                destination,
                &StagingName::new("link-source")?,
            )
            .expect_err("a linked copy source must fail");

        assert!(super::is_unsafe_cache_path_error(&error));
        assert!(!fixture.cache.join(destination).try_exists()?);
        Ok(())
    }

    struct Fixture {
        base: TempDir,
        cache: std::path::PathBuf,
    }

    impl Fixture {
        fn new() -> io::Result<Self> {
            let base = TempDir::new()?;
            let cache = base.path().join("cache");
            fs::create_dir(&cache)?;
            Ok(Self { base, cache })
        }

        fn root(&self) -> io::Result<CacheRoot> {
            CacheRoot::open(&self.cache)
        }
    }

    #[cfg(unix)]
    fn create_dir_link(target: &Path, link: &Path) -> io::Result<bool> {
        std::os::unix::fs::symlink(target, link)?;
        Ok(true)
    }

    #[cfg(windows)]
    fn create_dir_link(target: &Path, link: &Path) -> io::Result<bool> {
        match std::os::windows::fs::symlink_dir(target, link) {
            Ok(()) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn create_file_link(target: &Path, link: &Path) -> io::Result<bool> {
        std::os::unix::fs::symlink(target, link)?;
        Ok(true)
    }

    #[cfg(windows)]
    fn create_file_link(target: &Path, link: &Path) -> io::Result<bool> {
        match std::os::windows::fs::symlink_file(target, link) {
            Ok(()) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
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
