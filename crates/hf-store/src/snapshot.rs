use std::path::{Path, PathBuf};

use crate::cache::{AcquiredSnapshot, AcquiredSnapshotFile};
use crate::cache::{MaterializedLocalDir, MaterializedLocalDirFile};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision, SelectionId};

/// A validated immutable repository selection retained by the cache.
///
/// Keep this handle alive while downstream code uses its file paths. A future
/// compatible release may strengthen its internal reader lease without changing
/// the file-oriented API.
#[derive(Clone, Debug)]
pub struct Snapshot {
    root: PathBuf,
    endpoint: Endpoint,
    repository: RepositorySpec,
    requested_revision: Revision,
    commit: CommitId,
    selection: SelectionId,
    files: Box<[SnapshotFile]>,
    reused: bool,
    _lease_owner: AcquiredSnapshot,
}

impl Snapshot {
    pub(crate) fn from_acquired(
        endpoint: Endpoint,
        repository: RepositorySpec,
        requested_revision: Revision,
        acquired: &AcquiredSnapshot,
        reused: bool,
    ) -> Self {
        let files = acquired
            .files()
            .iter()
            .map(SnapshotFile::from)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            root: acquired.root().to_path_buf(),
            endpoint,
            repository,
            requested_revision,
            commit: acquired.commit().clone(),
            selection: *acquired.selection(),
            files,
            reused,
            _lease_owner: acquired.clone(),
        }
    }

    /// Returns the validated endpoint identity.
    #[must_use]
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Returns the validated snapshot directory.
    ///
    /// Retain this handle while downstream code uses the path so its shared
    /// reader lease remains active.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.root
    }

    /// Returns the repository identity.
    #[must_use]
    pub const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    /// Returns the revision originally requested by the caller.
    #[must_use]
    pub const fn requested_revision(&self) -> &Revision {
        &self.requested_revision
    }

    /// Returns the resolved immutable commit.
    #[must_use]
    pub const fn commit(&self) -> &CommitId {
        &self.commit
    }

    /// Returns the identity of the exact selected path set.
    #[must_use]
    pub const fn selection_id(&self) -> &SelectionId {
        &self.selection
    }

    /// Returns selected files in canonical repository-path order.
    #[must_use]
    pub fn files(&self) -> &[SnapshotFile] {
        &self.files
    }

    /// Finds one selected file by its validated repository path.
    #[must_use]
    pub fn file(&self, path: &RepoPath) -> Option<&SnapshotFile> {
        self.files
            .binary_search_by(|file| file.path.cmp(path))
            .ok()
            .map(|index| &self.files[index])
    }

    /// Returns whether all file body bytes came from an existing complete cache snapshot.
    #[must_use]
    pub const fn was_reused(&self) -> bool {
        self.reused
    }
}

/// One validated file tied to its owning [`Snapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotFile {
    path: RepoPath,
    local_path: PathBuf,
    sha256: Box<str>,
    size: u64,
}

impl SnapshotFile {
    /// Returns the canonical repository-relative path.
    #[must_use]
    pub const fn path(&self) -> &RepoPath {
        &self.path
    }

    /// Returns the validated local content path.
    #[must_use]
    pub fn local_path(&self) -> &Path {
        &self.local_path
    }

    /// Returns the always-computed lowercase local SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns the validated file size.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

impl From<&AcquiredSnapshotFile> for SnapshotFile {
    fn from(file: &AcquiredSnapshotFile) -> Self {
        Self {
            path: file.path().clone(),
            local_path: file.content_path().to_path_buf(),
            sha256: file.digest().to_string().into(),
            size: file.size(),
        }
    }
}

/// A validated mutable `local_dir` completion result.
#[derive(Clone, Debug)]
pub struct LocalDirectory {
    root: PathBuf,
    endpoint: Endpoint,
    repository: RepositorySpec,
    requested_revision: Revision,
    commit: CommitId,
    selection: SelectionId,
    files: Box<[LocalDirectoryFile]>,
}

impl LocalDirectory {
    pub(crate) fn from_materialized(
        endpoint: Endpoint,
        repository: RepositorySpec,
        requested_revision: Revision,
        materialized: MaterializedLocalDir,
    ) -> Self {
        Self {
            root: materialized.root,
            endpoint,
            repository,
            requested_revision,
            commit: materialized.commit,
            selection: materialized.selection,
            files: materialized
                .files
                .iter()
                .map(LocalDirectoryFile::from)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Returns the caller-owned local directory root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the validated endpoint identity.
    #[must_use]
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Returns the repository identity.
    #[must_use]
    pub const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    /// Returns the originally requested revision.
    #[must_use]
    pub const fn requested_revision(&self) -> &Revision {
        &self.requested_revision
    }

    /// Returns the resolved immutable commit.
    #[must_use]
    pub const fn commit(&self) -> &CommitId {
        &self.commit
    }

    /// Returns the exact selected path-set identity.
    #[must_use]
    pub const fn selection_id(&self) -> &SelectionId {
        &self.selection
    }

    /// Returns validated selected files in canonical order.
    #[must_use]
    pub fn files(&self) -> &[LocalDirectoryFile] {
        &self.files
    }
}

/// One validated file in a [`LocalDirectory`] result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalDirectoryFile {
    path: RepoPath,
    local_path: PathBuf,
    sha256: Box<str>,
    size: u64,
}

impl From<&MaterializedLocalDirFile> for LocalDirectoryFile {
    fn from(file: &MaterializedLocalDirFile) -> Self {
        Self {
            path: file.path.clone(),
            local_path: file.local_path.clone(),
            sha256: file.digest.to_string().into(),
            size: file.size,
        }
    }
}

impl LocalDirectoryFile {
    /// Returns the canonical repository path.
    #[must_use]
    pub const fn path(&self) -> &RepoPath {
        &self.path
    }

    /// Returns the independently materialized local path.
    #[must_use]
    pub fn local_path(&self) -> &Path {
        &self.local_path
    }

    /// Returns the validated lowercase local SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns the validated size.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}
