use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "network")]
use crate::AuthToken;
use crate::error::{CacheFailure, HubOperationError};
#[cfg(feature = "network")]
use crate::progress::ProgressObserver;
#[cfg(feature = "network")]
use crate::transfer::{RetryPolicy, TokioRetryClock};
use crate::{CancellationToken, CommitId, Endpoint, FetchPlan, RepoPath, RepositorySpec, Revision};

use super::CacheView;
use super::compatible_cache::{CompatibleCacheError, CompatibleCacheOffline, CompatibleSnapshot};
use super::hub_cache::HubSnapshotFileForm;
use super::key::{BlobDigest, SelectionId};
use super::local_dir_completion::{LocalDirOfflineError, LocalDirOfflineReader};
use super::local_dir_layout::HubLocalDirLayout;
use super::local_dir_materialization::{ExistingFilePolicy, LocalDirFileTarget};
use super::local_dir_reconciliation::{
    LocalDirReconciler, LocalDirReconciliationOutcome, LocalDirReconciliationPlan,
    OwnedBlobCandidates, ThreadYieldWait,
};
use super::publication::{
    CacheError, CacheKernel, Effects, OwnedSnapshotFile, OwnedSnapshotRead, SnapshotLease,
};
use super::rooted_fs::{CacheRoot, RootedFileSystem};
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

    pub(crate) fn materialize_local_dir(
        &self,
        plan: &FetchPlan,
        snapshot: &AcquiredSnapshot,
        destination: &Path,
        replace_existing: bool,
        cancellation: &CancellationToken,
    ) -> Result<MaterializedLocalDir, HubOperationError> {
        std::fs::create_dir_all(destination)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let layout = HubLocalDirLayout::new(destination, plan.endpoint(), plan.repository())
            .map_err(HubOperationError::validation)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(
            CacheRoot::open(destination)
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?,
        );
        let targets = plan
            .files()
            .iter()
            .map(|file| {
                let digest = snapshot
                    .files()
                    .iter()
                    .find(|cached| cached.path() == file.path())
                    .map(AcquiredSnapshotFile::digest)
                    .ok_or_else(|| HubOperationError::cache(CacheFailure::Incomplete))?;
                Ok(LocalDirFileTarget::new(
                    file.path().clone(),
                    file.entry().clone(),
                    digest,
                ))
            })
            .collect::<Result<Vec<_>, HubOperationError>>()?;
        let reconciliation = LocalDirReconciliationPlan::new(
            layout.clone(),
            plan.commit().clone(),
            plan.selection(),
            targets,
        )
        .map_err(map_local_dir_error)?;
        let reconciler =
            LocalDirReconciler::new(root, Effects::production(), Arc::new(ThreadYieldWait));
        let mut candidates = OwnedBlobCandidates::new(self.transfer.as_ref().clone());
        let policy = if replace_existing {
            ExistingFilePolicy::ReplaceRegularFile
        } else {
            ExistingFilePolicy::Reject
        };
        let report = match reconciler
            .reconcile(&reconciliation, &mut candidates, policy, cancellation)
            .map_err(map_local_dir_error)?
        {
            LocalDirReconciliationOutcome::Reconciled(report) => report,
            LocalDirReconciliationOutcome::NeedsTransport(_demand) => {
                return Err(HubOperationError::cache(CacheFailure::Incomplete));
            }
        };
        let files = report
            .files()
            .iter()
            .map(|file| {
                Ok(MaterializedLocalDirFile {
                    path: file.path().clone(),
                    local_path: layout
                        .file_path(file.path())
                        .map_err(HubOperationError::validation)?,
                    digest: file.digest(),
                    size: file.size(),
                })
            })
            .collect::<Result<Vec<_>, HubOperationError>>()?;
        Ok(MaterializedLocalDir {
            root: destination.to_path_buf(),
            commit: report.commit().clone(),
            selection: *report.selection_id(),
            files: files.into_boxed_slice(),
        })
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

    pub(crate) fn open_local_dir(
        root: &Path,
        endpoint: &Endpoint,
        repository: &RepositorySpec,
        commit: &CommitId,
        paths: &[RepoPath],
    ) -> Result<MaterializedLocalDir, HubOperationError> {
        let layout = HubLocalDirLayout::new(root, endpoint, repository)
            .map_err(HubOperationError::validation)?;
        let rooted: Arc<dyn RootedFileSystem> = Arc::new(
            CacheRoot::open(root).map_err(|_source| HubOperationError::cache(CacheFailure::Io))?,
        );
        let mut selected = paths.to_vec();
        selected.sort_unstable();
        selected.dedup();
        let selection = SelectionId::derive(&selected).map_err(HubOperationError::validation)?;
        let snapshot = LocalDirOfflineReader::new(layout, rooted)
            .map_err(map_local_dir_offline_error)?
            .open(commit, &selection)
            .map_err(map_local_dir_offline_error)?;
        let files = snapshot
            .files()
            .iter()
            .map(|file| MaterializedLocalDirFile {
                path: file.path().clone(),
                local_path: file.destination().to_path_buf(),
                digest: file.digest(),
                size: file.size(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(MaterializedLocalDir {
            root: root.to_path_buf(),
            commit: snapshot.commit().clone(),
            selection: *snapshot.selection(),
            files,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcquiredSnapshot {
    mode: crate::CacheMode,
    root: PathBuf,
    commit: CommitId,
    selection: SelectionId,
    files: Box<[AcquiredSnapshotFile]>,
    lease: Arc<SnapshotLease>,
}

impl AcquiredSnapshot {
    fn from_owned(commit: CommitId, selection: SelectionId, snapshot: OwnedSnapshotRead) -> Self {
        let (root, files, lease) = snapshot.into_parts();
        Self {
            mode: crate::CacheMode::Owned,
            root,
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

    pub(crate) const fn mode(&self) -> crate::CacheMode {
        self.mode
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
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
                form: match file.form() {
                    HubSnapshotFileForm::SnapshotOnly => AcquiredSnapshotFileForm::SnapshotOnly,
                    HubSnapshotFileForm::CopiedWithBlob => AcquiredSnapshotFileForm::CopiedWithBlob,
                    HubSnapshotFileForm::RelativeSymlink => {
                        AcquiredSnapshotFileForm::RelativeSymlink
                    }
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            mode: crate::CacheMode::Compatible,
            root: snapshot.root().to_path_buf(),
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
    form: AcquiredSnapshotFileForm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AcquiredSnapshotFileForm {
    Owned,
    SnapshotOnly,
    CopiedWithBlob,
    RelativeSymlink,
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedLocalDir {
    pub(crate) root: PathBuf,
    pub(crate) commit: CommitId,
    pub(crate) selection: SelectionId,
    pub(crate) files: Box<[MaterializedLocalDirFile]>,
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedLocalDirFile {
    pub(crate) path: RepoPath,
    pub(crate) local_path: PathBuf,
    pub(crate) digest: BlobDigest,
    pub(crate) size: u64,
}

impl From<OwnedSnapshotFile> for AcquiredSnapshotFile {
    fn from(file: OwnedSnapshotFile) -> Self {
        Self {
            path: file.path().clone(),
            content_path: file.content_path().to_path_buf(),
            digest: file.digest(),
            size: file.size(),
            form: AcquiredSnapshotFileForm::Owned,
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

    pub(crate) const fn form(&self) -> AcquiredSnapshotFileForm {
        self.form
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "the private detailed cause is deliberately consumed at the public classification boundary"
)]
fn map_local_dir_offline_error(error: LocalDirOfflineError) -> HubOperationError {
    let failure = if error.is_incomplete() || error.is_stale() {
        CacheFailure::Incomplete
    } else {
        CacheFailure::Corrupt
    };
    HubOperationError::cache(failure)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the private detailed cause is deliberately consumed at the public classification boundary"
)]
fn map_local_dir_error(
    error: super::local_dir_reconciliation::LocalDirReconciliationError,
) -> HubOperationError {
    if error.is_cancelled() {
        HubOperationError::cancelled()
    } else if error.is_plan_invalid() {
        HubOperationError::protocol()
    } else if error.is_conflict() || error.is_final_validation() {
        HubOperationError::cache(CacheFailure::Corrupt)
    } else {
        HubOperationError::cache(CacheFailure::Io)
    }
}
