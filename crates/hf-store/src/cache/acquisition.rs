use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{CacheFailure, HubOperationError};
#[cfg(feature = "network")]
use crate::progress::ProgressObserver;
#[cfg(feature = "network")]
use crate::transfer::{RetryPolicy, TokioRetryClock};
#[cfg(feature = "network")]
use crate::{AuthToken, CancellationToken};
use crate::{CommitId, Endpoint, FetchPlan, RepoPath, RepositorySpec, Revision};

use super::CacheView;
use super::compatible_cache::{CompatibleCacheError, CompatibleCacheOffline, CompatibleSnapshot};
use super::key::{BlobDigest, SelectionId};
use super::publication::{
    CacheError, CacheKernel, Effects, OwnedSnapshotFile, OwnedSnapshotRead, SnapshotLease,
};
use super::standard_cache::StandardCacheWriter;

#[derive(Clone, Debug)]
pub(crate) struct AcquisitionCache {
    transfer: Arc<CacheKernel>,
    compatible_writer: Option<StandardCacheWriter>,
    compatible_offline: Option<CompatibleCacheOffline>,
    view: CacheView,
}

impl AcquisitionCache {
    pub(crate) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        repository: &RepositorySpec,
        view: CacheView,
    ) -> Result<Self, HubOperationError> {
        let effects = Effects::production();
        let transfer = CacheKernel::new(root.as_ref(), endpoint, repository, effects.clone())
            .map_err(map_cache_error)?;
        transfer.initialize().map_err(map_cache_error)?;
        let (compatible_writer, compatible_offline) = match view {
            CacheView::Owned => (None, None),
            CacheView::Compatible => (
                Some(
                    StandardCacheWriter::shared(
                        root.as_ref(),
                        endpoint,
                        repository,
                        effects.clone(),
                    )
                    .map_err(map_compatible_error)?,
                ),
                Some(
                    CompatibleCacheOffline::shared(root, endpoint, repository, effects)
                        .map_err(map_compatible_error)?,
                ),
            ),
        };
        Ok(Self {
            transfer: Arc::new(transfer),
            compatible_writer,
            compatible_offline,
            view,
        })
    }

    pub(crate) fn open_plan(
        &self,
        plan: &FetchPlan,
    ) -> Result<AcquiredSnapshot, HubOperationError> {
        let revision =
            Revision::parse(plan.commit().as_str()).map_err(HubOperationError::validation)?;
        let paths = plan
            .files()
            .iter()
            .map(|file| file.path().clone())
            .collect::<Vec<_>>();
        match self.view {
            CacheView::Owned => self
                .transfer
                .open_owned_snapshot(&revision, &paths)
                .map(|files| {
                    AcquiredSnapshot::from_owned(plan.commit().clone(), *plan.selection_id(), files)
                })
                .map_err(map_cache_error),
            CacheView::Compatible => self
                .compatible_offline
                .as_ref()
                .ok_or_else(HubOperationError::protocol)?
                .open(&revision, &paths)
                .map(AcquiredSnapshot::from)
                .map_err(map_compatible_error),
        }
    }

    #[cfg(feature = "network")]
    #[allow(
        clippy::too_many_arguments,
        reason = "the online file boundary keeps request and operation policy explicit"
    )]
    pub(crate) async fn download_file(
        &self,
        protocol: Arc<crate::hub_protocol::HubProtocol>,
        plan: &FetchPlan,
        file: &crate::PlannedFile,
        authorization: Option<AuthToken>,
        retry_policy: RetryPolicy,
        cancellation: CancellationToken,
        progress: Arc<dyn ProgressObserver>,
    ) -> Result<BlobDigest, HubOperationError> {
        self.transfer
            .download_file(
                protocol,
                plan.repository().clone(),
                plan.commit().clone(),
                file.path().clone(),
                file.entry().clone(),
                authorization,
                retry_policy,
                &TokioRetryClock,
                cancellation,
                progress,
            )
            .await
    }

    pub(crate) fn activate(
        &self,
        plan: &FetchPlan,
        digests: &BTreeMap<RepoPath, BlobDigest>,
    ) -> Result<AcquiredSnapshot, HubOperationError> {
        match self.view {
            CacheView::Owned => self.activate_owned(plan, digests),
            CacheView::Compatible => self.activate_compatible(plan, digests),
        }
    }

    fn activate_owned(
        &self,
        plan: &FetchPlan,
        digests: &BTreeMap<RepoPath, BlobDigest>,
    ) -> Result<AcquiredSnapshot, HubOperationError> {
        let files = plan
            .files()
            .iter()
            .map(|file| {
                digests
                    .get(file.path())
                    .copied()
                    .map(|digest| (file.path().clone(), digest, file.size()))
                    .ok_or_else(|| HubOperationError::cache(CacheFailure::Incomplete))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let selected = self
            .transfer
            .publish_owned_snapshot(plan.commit(), plan.selection_id(), &files)
            .map_err(map_cache_error)?;
        if CommitId::parse(plan.requested_revision().as_str()).is_err() {
            self.transfer
                .write_ref(plan.requested_revision(), plan.commit())
                .map_err(map_cache_error)?;
        }
        Ok(AcquiredSnapshot::from_owned(
            plan.commit().clone(),
            *plan.selection_id(),
            selected,
        ))
    }

    fn activate_compatible(
        &self,
        plan: &FetchPlan,
        digests: &BTreeMap<RepoPath, BlobDigest>,
    ) -> Result<AcquiredSnapshot, HubOperationError> {
        let paths = plan
            .files()
            .iter()
            .map(|file| file.path().clone())
            .collect::<Vec<_>>();
        let snapshot = self
            .compatible_writer
            .as_ref()
            .ok_or_else(HubOperationError::protocol)?
            .publish(
                plan.requested_revision(),
                plan.commit(),
                plan.tree(),
                &paths,
                |path| {
                    let digest = digests.get(path).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "validated cache blob is missing")
                    })?;
                    let size = plan
                        .files()
                        .iter()
                        .find(|file| file.path() == path)
                        .map(crate::PlannedFile::size)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::NotFound, "planned cache file is missing")
                        })?;
                    self.transfer
                        .open_blob(digest, size)
                        .map_err(|_source| io::Error::other("validated cache blob open failed"))?
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::NotFound,
                                "validated cache blob is missing",
                            )
                        })
                },
            )
            .map_err(map_compatible_error)?;
        Ok(snapshot.into())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OfflineCache {
    backend: OfflineBackend,
}

#[derive(Clone, Debug)]
enum OfflineBackend {
    Owned(Box<CacheKernel>),
    Compatible(Box<CompatibleCacheOffline>),
}

impl OfflineCache {
    pub(crate) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        repository: &RepositorySpec,
        view: CacheView,
    ) -> Result<Self, HubOperationError> {
        let effects = Effects::production();
        let backend = match view {
            CacheView::Owned => OfflineBackend::Owned(Box::new(
                CacheKernel::new(root, endpoint, repository, effects).map_err(map_cache_error)?,
            )),
            CacheView::Compatible => OfflineBackend::Compatible(Box::new(
                CompatibleCacheOffline::shared(root, endpoint, repository, effects)
                    .map_err(map_compatible_error)?,
            )),
        };
        Ok(Self { backend })
    }

    pub(crate) fn open(
        &self,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> Result<AcquiredSnapshot, HubOperationError> {
        match &self.backend {
            OfflineBackend::Owned(cache) => {
                let commit = match CommitId::parse(revision.as_str()) {
                    Ok(commit) => commit,
                    Err(_symbolic) => cache.read_ref(revision).map_err(map_cache_error)?,
                };
                let mut selected = paths.to_vec();
                selected.sort_unstable();
                selected.dedup();
                let selection =
                    SelectionId::derive(&selected).map_err(HubOperationError::validation)?;
                cache
                    .open_owned_snapshot(revision, &selected)
                    .map(|files| AcquiredSnapshot::from_owned(commit, selection, files))
                    .map_err(map_cache_error)
            }
            OfflineBackend::Compatible(cache) => cache
                .open(revision, paths)
                .map(AcquiredSnapshot::from)
                .map_err(map_compatible_error),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcquiredSnapshot {
    commit: CommitId,
    selection: SelectionId,
    files: Box<[AcquiredSnapshotFile]>,
    lease: Arc<SnapshotLease>,
}

impl AcquiredSnapshot {
    fn from_owned(commit: CommitId, selection: SelectionId, snapshot: OwnedSnapshotRead) -> Self {
        let (files, lease) = snapshot.into_parts();
        Self {
            commit,
            selection,
            files: files
                .into_iter()
                .map(AcquiredSnapshotFile::from)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            lease,
        }
    }

    pub(crate) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(crate) const fn selection(&self) -> &SelectionId {
        &self.selection
    }

    pub(crate) fn files(&self) -> &[AcquiredSnapshotFile] {
        &self.files
    }
}

impl From<CompatibleSnapshot> for AcquiredSnapshot {
    fn from(snapshot: CompatibleSnapshot) -> Self {
        let files = snapshot
            .files()
            .iter()
            .map(|file| AcquiredSnapshotFile {
                path: file.path().clone(),
                content_path: file.content_path().to_path_buf(),
                digest: file.digest(),
                size: file.size(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            commit: snapshot.commit().clone(),
            selection: *snapshot.selection(),
            files,
            lease: Arc::clone(snapshot.lease()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcquiredSnapshotFile {
    path: RepoPath,
    content_path: PathBuf,
    digest: BlobDigest,
    size: u64,
}

impl From<OwnedSnapshotFile> for AcquiredSnapshotFile {
    fn from(file: OwnedSnapshotFile) -> Self {
        Self {
            path: file.path().clone(),
            content_path: file.content_path().to_path_buf(),
            digest: file.digest(),
            size: file.size(),
        }
    }
}

impl AcquiredSnapshotFile {
    pub(crate) const fn path(&self) -> &RepoPath {
        &self.path
    }

    pub(crate) fn content_path(&self) -> &Path {
        &self.content_path
    }

    pub(crate) const fn digest(&self) -> BlobDigest {
        self.digest
    }

    pub(crate) const fn size(&self) -> u64 {
        self.size
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the private detailed cause is deliberately consumed at the public classification boundary"
)]
fn map_compatible_error(error: CompatibleCacheError) -> HubOperationError {
    let failure = if error.is_incomplete() {
        CacheFailure::Incomplete
    } else if error.is_unsupported_version() {
        CacheFailure::UnsupportedVersion
    } else if error.is_corrupt() || error.is_unsafe() {
        CacheFailure::Corrupt
    } else {
        CacheFailure::Io
    };
    HubOperationError::cache(failure)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the private detailed cause is deliberately consumed at the public classification boundary"
)]
fn map_cache_error(error: CacheError) -> HubOperationError {
    let failure = if error.is_not_found() {
        CacheFailure::Incomplete
    } else if error.is_unsupported_record() {
        CacheFailure::UnsupportedVersion
    } else if error.is_corrupt_record() || error.is_corrupt_existing_blob() || error.is_unsafe() {
        CacheFailure::Corrupt
    } else {
        CacheFailure::Io
    };
    HubOperationError::cache(failure)
}
