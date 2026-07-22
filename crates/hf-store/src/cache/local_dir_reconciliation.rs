//! Commit-bound multi-file reconciliation for user-owned local directories.

use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, RepoPath};

use super::filter::RepositorySelection;
use super::hub_metadata::{HubMetadataError, HubTree};
use super::key::{BlobDigest, SelectionId};
use super::local_dir_bookkeeping::{LocalDirBookkeepingWriteError, LocalDirBookkeepingWriter};
use super::local_dir_completion::{
    LocalDirCompletionError, LocalDirCompletionFile, LocalDirCompletionWriter,
};
use super::local_dir_layout::HubLocalDirLayout;
use super::local_dir_materialization::{
    Cancellation, ExistingFilePolicy, LocalDirDestinationInspection, LocalDirFileDisposition,
    LocalDirFileMaterializer, LocalDirFileTarget, LocalDirMaterializationError,
};
use super::publication::{CacheError, CacheKernel, Effects};
use super::rooted_fs::{RootedFileSystem, RootedLockAttempt};
use super::sanitized_io::SanitizedIo;

/// A canonical, immutable set of commit-bound files to reconcile.
#[derive(Clone)]
pub(super) struct LocalDirReconciliationPlan {
    layout: HubLocalDirLayout,
    commit: CommitId,
    selection_id: SelectionId,
    targets: Box<[LocalDirFileTarget]>,
    coordination_lock_relative: PathBuf,
}

impl LocalDirReconciliationPlan {
    /// Validates the complete plan before a lock, candidate source, or write is used.
    pub(super) fn new(
        layout: HubLocalDirLayout,
        commit: CommitId,
        selection: &RepositorySelection,
        targets: impl IntoIterator<Item = LocalDirFileTarget>,
    ) -> Result<Self, LocalDirReconciliationError> {
        let mut targets = targets.into_iter().collect::<Vec<_>>();
        targets.sort_unstable_by(|left, right| left.path().cmp(right.path()));

        let exact_match = targets.len() == selection.paths().len()
            && targets
                .iter()
                .zip(selection.paths())
                .all(|(target, selected)| target.path() == selected);
        if !exact_match {
            return Err(LocalDirReconciliationError::plan(ValidationError::new(
                "local directory reconciliation plan",
                ValidationErrorKind::Malformed,
            )));
        }

        for target in &targets {
            let _validated_destination = layout.file_path(target.path())?;
        }
        let coordination_lock_relative = layout.coordination_lock_relative()?;

        Ok(Self {
            layout,
            commit,
            selection_id: *selection.selection_id(),
            targets: targets.into_boxed_slice(),
            coordination_lock_relative,
        })
    }

    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) const fn selection_id(&self) -> &SelectionId {
        &self.selection_id
    }

    pub(super) const fn targets(&self) -> &[LocalDirFileTarget] {
        &self.targets
    }

    pub(super) const fn layout(&self) -> &HubLocalDirLayout {
        &self.layout
    }
}

impl Debug for LocalDirReconciliationPlan {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirReconciliationPlan")
            .field("target_count", &self.targets.len())
            .finish_non_exhaustive()
    }
}

/// An injected, cancellation-aware pause between nonblocking lock attempts.
pub(super) trait LockWait: Debug + Send + Sync {
    fn wait(&self, cancellation: &dyn Cancellation) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ThreadYieldWait;

impl LockWait for ThreadYieldWait {
    fn wait(&self, _cancellation: &dyn Cancellation) -> io::Result<()> {
        std::thread::yield_now();
        Ok(())
    }
}

/// Supplies already-local validated bytes without owning a network transport.
pub(super) trait LocalDirCandidateSet: Debug + Send {
    fn prepare_local(
        &mut self,
        target: &LocalDirFileTarget,
        cancellation: &dyn Cancellation,
    ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError>;
}

/// Supplies immutable owned-cache blobs without constructing a transport.
#[derive(Clone, Debug)]
pub(super) struct OwnedBlobCandidates {
    cache: CacheKernel,
}

impl OwnedBlobCandidates {
    pub(super) const fn new(cache: CacheKernel) -> Self {
        Self { cache }
    }
}

impl LocalDirCandidateSet for OwnedBlobCandidates {
    fn prepare_local(
        &mut self,
        target: &LocalDirFileTarget,
        _cancellation: &dyn Cancellation,
    ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
        self.cache
            .open_blob(&target.digest(), target.entry().size())
            .map(|reader| reader.map(PreparedLocalDirSource::owned_cache))
            .map_err(LocalDirSourceError::cache)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedSourceProvenance {
    Owned,
    Compatible,
    NewlyAcquired { downloaded_body_bytes: u64 },
}

/// An owned reader that keeps any cache lease alive until reconciliation ends.
pub(super) struct PreparedLocalDirSource {
    reader: Box<dyn Read + Send>,
    provenance: PreparedSourceProvenance,
}

impl PreparedLocalDirSource {
    pub(super) fn owned_cache(reader: Box<dyn Read + Send>) -> Self {
        Self {
            reader,
            provenance: PreparedSourceProvenance::Owned,
        }
    }

    pub(super) fn compatible_cache(reader: Box<dyn Read + Send>) -> Self {
        Self {
            reader,
            provenance: PreparedSourceProvenance::Compatible,
        }
    }

    pub(super) fn newly_acquired_cache(
        reader: Box<dyn Read + Send>,
        downloaded_body_bytes: u64,
    ) -> Self {
        Self {
            reader,
            provenance: PreparedSourceProvenance::NewlyAcquired {
                downloaded_body_bytes,
            },
        }
    }

    fn into_parts(self) -> (Box<dyn Read + Send>, PreparedSourceProvenance) {
        (self.reader, self.provenance)
    }

    #[cfg(test)]
    pub(super) fn into_reader(self) -> Box<dyn Read + Send> {
        self.reader
    }
}

impl Debug for PreparedLocalDirSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let source = match self.provenance {
            PreparedSourceProvenance::Owned => "owned-cache",
            PreparedSourceProvenance::Compatible => "compatible-cache",
            PreparedSourceProvenance::NewlyAcquired { .. } => "newly-acquired-cache",
        };
        formatter
            .debug_struct("PreparedLocalDirSource")
            .field("source", &source)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LocalDirFileProvenance {
    ExistingDestination,
    OwnedCache,
    CompatibleCache,
    NewlyAcquiredCache,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirReconciledFile {
    path: RepoPath,
    size: u64,
    digest: BlobDigest,
    disposition: LocalDirFileDisposition,
    provenance: LocalDirFileProvenance,
    destination_bytes_written: u64,
    downloaded_body_bytes: u64,
}

impl LocalDirReconciledFile {
    pub(super) const fn path(&self) -> &RepoPath {
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

    pub(super) const fn provenance(&self) -> LocalDirFileProvenance {
        self.provenance
    }

    pub(super) const fn destination_bytes_written(&self) -> u64 {
        self.destination_bytes_written
    }

    pub(super) const fn downloaded_body_bytes(&self) -> u64 {
        self.downloaded_body_bytes
    }
}

impl Debug for LocalDirReconciledFile {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirReconciledFile")
            .field("size", &self.size)
            .field("disposition", &self.disposition)
            .field("provenance", &self.provenance)
            .field("destination_bytes_written", &self.destination_bytes_written)
            .field("downloaded_body_bytes", &self.downloaded_body_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirReconciliationReport {
    commit: CommitId,
    selection_id: SelectionId,
    files: Box<[LocalDirReconciledFile]>,
    destination_bytes_written: u64,
    downloaded_body_bytes: u64,
}

impl LocalDirReconciliationReport {
    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) const fn selection_id(&self) -> &SelectionId {
        &self.selection_id
    }

    pub(super) const fn files(&self) -> &[LocalDirReconciledFile] {
        &self.files
    }

    pub(super) const fn destination_bytes_written(&self) -> u64 {
        self.destination_bytes_written
    }

    pub(super) const fn downloaded_body_bytes(&self) -> u64 {
        self.downloaded_body_bytes
    }
}

impl Debug for LocalDirReconciliationReport {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirReconciliationReport")
            .field("file_count", &self.files.len())
            .field("destination_bytes_written", &self.destination_bytes_written)
            .field("downloaded_body_bytes", &self.downloaded_body_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct LocalDirTransportDemand {
    commit: CommitId,
    selection_id: SelectionId,
    targets: Box<[LocalDirFileTarget]>,
}

impl LocalDirTransportDemand {
    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) const fn selection_id(&self) -> &SelectionId {
        &self.selection_id
    }

    pub(super) const fn targets(&self) -> &[LocalDirFileTarget] {
        &self.targets
    }
}

impl Debug for LocalDirTransportDemand {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirTransportDemand")
            .field("target_count", &self.targets.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LocalDirReconciliationOutcome {
    Reconciled(LocalDirReconciliationReport),
    NeedsTransport(LocalDirTransportDemand),
}

#[derive(Clone)]
pub(super) struct LocalDirReconciler {
    root: Arc<dyn RootedFileSystem>,
    effects: Effects,
    lock_wait: Arc<dyn LockWait>,
}

impl LocalDirReconciler {
    pub(super) const fn new(
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
        lock_wait: Arc<dyn LockWait>,
    ) -> Self {
        Self {
            root,
            effects,
            lock_wait,
        }
    }

    pub(super) fn reconcile(
        &self,
        plan: &LocalDirReconciliationPlan,
        candidates: &mut dyn LocalDirCandidateSet,
        policy: ExistingFilePolicy,
        cancellation: &dyn Cancellation,
    ) -> Result<LocalDirReconciliationOutcome, LocalDirReconciliationError> {
        check_cancellation(cancellation, false)?;
        let _guard = self.acquire_lock(&plan.coordination_lock_relative, cancellation)?;
        check_cancellation(cancellation, false)?;

        let materializer = LocalDirFileMaterializer::from_layout(
            plan.layout.clone(),
            Arc::clone(&self.root),
            self.effects.clone(),
        );
        let prepared = match prepare_sources(&materializer, plan, candidates, policy, cancellation)?
        {
            SourcePreparation::Ready(prepared) => prepared,
            SourcePreparation::NeedsTransport(demand) => {
                return Ok(LocalDirReconciliationOutcome::NeedsTransport(demand));
            }
        };
        let completion = LocalDirCompletionWriter::new(
            plan.layout.clone(),
            Arc::clone(&self.root),
            self.effects.clone(),
        )
        .map_err(LocalDirReconciliationError::completion)?;
        completion
            .publish_in_progress(plan.commit(), plan.selection_id())
            .map_err(LocalDirReconciliationError::completion)?;
        let report = materialize_files(&materializer, plan, prepared, policy, cancellation)
            .map_err(LocalDirReconciliationError::with_change)?;
        validate_all(
            &materializer,
            plan,
            cancellation,
            report.destination_bytes_written != 0,
        )
        .map_err(LocalDirReconciliationError::with_change)?;
        let tree = HubTree::new(
            plan.targets()
                .iter()
                .map(|target| (target.path().clone(), target.entry().clone())),
        )
        .map_err(LocalDirReconciliationError::metadata)
        .map_err(LocalDirReconciliationError::with_change)?;
        let bookkeeping = LocalDirBookkeepingWriter::from_layout(
            plan.layout.clone(),
            Arc::clone(&self.root),
            self.effects.clone(),
        );
        for target in plan.targets() {
            let etag = target
                .entry()
                .lfs_sha256()
                .unwrap_or_else(|| target.entry().blob_id());
            bookkeeping
                .write_file_hint(target.path(), plan.commit(), etag)
                .map_err(LocalDirReconciliationError::bookkeeping)
                .map_err(LocalDirReconciliationError::with_change)?;
        }
        bookkeeping
            .write_tree_hint(plan.commit(), &tree)
            .map_err(LocalDirReconciliationError::bookkeeping)
            .map_err(LocalDirReconciliationError::with_change)?;
        let completed_files = report
            .files()
            .iter()
            .map(|file| {
                LocalDirCompletionFile::new(file.path().clone(), file.size(), file.digest())
            })
            .collect::<Vec<_>>();
        completion
            .publish_complete(plan.commit(), plan.selection_id(), &completed_files)
            .map_err(|source| LocalDirReconciliationError::completion(source).with_change())?;
        Ok(LocalDirReconciliationOutcome::Reconciled(report))
    }

    fn acquire_lock(
        &self,
        relative: &Path,
        cancellation: &dyn Cancellation,
    ) -> Result<Box<dyn super::rooted_fs::RootedLockGuard>, LocalDirReconciliationError> {
        loop {
            check_cancellation(cancellation, false)?;
            match self
                .root
                .try_lock_exclusive(relative)
                .map_err(|source| LocalDirReconciliationError::lock(&source))?
            {
                RootedLockAttempt::Acquired(guard) => return Ok(guard),
                RootedLockAttempt::Contended => {
                    let waited = self.lock_wait.wait(cancellation);
                    if cancellation.is_cancelled() {
                        return Err(LocalDirReconciliationError::cancelled(false));
                    }
                    waited.map_err(|source| LocalDirReconciliationError::lock_wait(&source))?;
                }
            }
        }
    }
}

enum SourcePreparation {
    Ready(Vec<Option<PreparedLocalDirSource>>),
    NeedsTransport(LocalDirTransportDemand),
}

fn prepare_sources(
    materializer: &LocalDirFileMaterializer,
    plan: &LocalDirReconciliationPlan,
    candidates: &mut dyn LocalDirCandidateSet,
    policy: ExistingFilePolicy,
    cancellation: &dyn Cancellation,
) -> Result<SourcePreparation, LocalDirReconciliationError> {
    let initial = inspect_all(materializer, plan, cancellation, false)?;
    validate_policy(&initial, policy, false)?;
    let mut prepared = (0..plan.targets.len())
        .map(|_| None)
        .collect::<Vec<Option<PreparedLocalDirSource>>>();
    let missing = prepare_needed(plan, &initial, &mut prepared, candidates, cancellation)?;
    if !missing.is_empty() {
        return Ok(SourcePreparation::NeedsTransport(demand(plan, &missing)));
    }

    // Candidate preparation can hash cache files. Revalidate the complete
    // destination set under the lock before changing the first selected path.
    let rechecked = inspect_all(materializer, plan, cancellation, false)?;
    validate_policy(&rechecked, policy, false)?;
    let missing = prepare_needed(plan, &rechecked, &mut prepared, candidates, cancellation)?;
    if missing.is_empty() {
        Ok(SourcePreparation::Ready(prepared))
    } else {
        Ok(SourcePreparation::NeedsTransport(demand(plan, &missing)))
    }
}

fn inspect_all(
    materializer: &LocalDirFileMaterializer,
    plan: &LocalDirReconciliationPlan,
    cancellation: &dyn Cancellation,
    may_have_changed: bool,
) -> Result<Vec<LocalDirDestinationInspection>, LocalDirReconciliationError> {
    plan.targets
        .iter()
        .map(|target| {
            materializer
                .inspect(target, cancellation)
                .map_err(|source| {
                    LocalDirReconciliationError::materialization(source, may_have_changed)
                })
        })
        .collect()
}

fn validate_policy(
    inspections: &[LocalDirDestinationInspection],
    policy: ExistingFilePolicy,
    may_have_changed: bool,
) -> Result<(), LocalDirReconciliationError> {
    let conflicts = inspections.iter().any(|inspection| {
        *inspection == LocalDirDestinationInspection::Conflict
            || (*inspection == LocalDirDestinationInspection::DifferentRegular
                && policy == ExistingFilePolicy::Reject)
    });
    if conflicts {
        Err(LocalDirReconciliationError::conflict(may_have_changed))
    } else {
        Ok(())
    }
}

fn prepare_needed(
    plan: &LocalDirReconciliationPlan,
    inspections: &[LocalDirDestinationInspection],
    prepared: &mut [Option<PreparedLocalDirSource>],
    candidates: &mut dyn LocalDirCandidateSet,
    cancellation: &dyn Cancellation,
) -> Result<Vec<usize>, LocalDirReconciliationError> {
    let mut missing = Vec::new();
    for (index, ((target, inspection), slot)) in plan
        .targets
        .iter()
        .zip(inspections)
        .zip(prepared)
        .enumerate()
    {
        if *inspection == LocalDirDestinationInspection::Exact || slot.is_some() {
            continue;
        }
        check_cancellation(cancellation, false)?;
        let candidate = candidates.prepare_local(target, cancellation);
        if cancellation.is_cancelled() {
            return Err(LocalDirReconciliationError::cancelled(false));
        }
        *slot = candidate.map_err(LocalDirReconciliationError::source)?;
        if slot.is_none() {
            missing.push(index);
        }
    }
    Ok(missing)
}

fn demand(plan: &LocalDirReconciliationPlan, missing: &[usize]) -> LocalDirTransportDemand {
    let targets = missing
        .iter()
        .filter_map(|index| plan.targets.get(*index).cloned())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    LocalDirTransportDemand {
        commit: plan.commit.clone(),
        selection_id: plan.selection_id,
        targets,
    }
}

fn materialize_files(
    materializer: &LocalDirFileMaterializer,
    plan: &LocalDirReconciliationPlan,
    prepared: Vec<Option<PreparedLocalDirSource>>,
    policy: ExistingFilePolicy,
    cancellation: &dyn Cancellation,
) -> Result<LocalDirReconciliationReport, LocalDirReconciliationError> {
    let mut files = Vec::with_capacity(plan.targets.len());
    let mut destination_bytes_written = 0_u64;
    let mut downloaded_body_bytes = 0_u64;

    for (target, source) in plan.targets.iter().zip(prepared) {
        let prefix_changed = destination_bytes_written != 0;
        check_cancellation(cancellation, prefix_changed)?;
        let file = materialize_file(
            materializer,
            target,
            source,
            policy,
            cancellation,
            prefix_changed,
        )?;
        destination_bytes_written = checked_add(
            destination_bytes_written,
            file.destination_bytes_written,
            prefix_changed || file.destination_bytes_written != 0,
        )?;
        downloaded_body_bytes = checked_add(
            downloaded_body_bytes,
            file.downloaded_body_bytes,
            destination_bytes_written != 0,
        )?;
        files.push(file);
    }

    Ok(LocalDirReconciliationReport {
        commit: plan.commit.clone(),
        selection_id: plan.selection_id,
        files: files.into_boxed_slice(),
        destination_bytes_written,
        downloaded_body_bytes,
    })
}

fn materialize_file(
    materializer: &LocalDirFileMaterializer,
    target: &LocalDirFileTarget,
    source: Option<PreparedLocalDirSource>,
    policy: ExistingFilePolicy,
    cancellation: &dyn Cancellation,
    prefix_changed: bool,
) -> Result<LocalDirReconciledFile, LocalDirReconciliationError> {
    let (disposition, provenance, downloaded_body_bytes) = if let Some(source) = source {
        let (mut reader, source_provenance) = source.into_parts();
        let file_result = materializer
            .materialize_with_cancellation(target, reader.as_mut(), policy, cancellation)
            .map_err(|source| {
                LocalDirReconciliationError::materialization(source, prefix_changed)
            })?;
        let (prepared_provenance, downloaded) = report_provenance(source_provenance);
        let provenance = if file_result.disposition() == LocalDirFileDisposition::Reused {
            LocalDirFileProvenance::ExistingDestination
        } else {
            prepared_provenance
        };
        (file_result.disposition(), provenance, downloaded)
    } else {
        let inspection = materializer
            .inspect(target, cancellation)
            .map_err(|source| {
                LocalDirReconciliationError::materialization(source, prefix_changed)
            })?;
        if inspection != LocalDirDestinationInspection::Exact {
            return Err(LocalDirReconciliationError::final_validation(
                prefix_changed,
            ));
        }
        (
            LocalDirFileDisposition::Reused,
            LocalDirFileProvenance::ExistingDestination,
            0,
        )
    };
    let destination_bytes_written = if disposition == LocalDirFileDisposition::Copied {
        target.entry().size()
    } else {
        0
    };
    Ok(LocalDirReconciledFile {
        path: target.path().clone(),
        size: target.entry().size(),
        digest: target.digest(),
        disposition,
        provenance,
        destination_bytes_written,
        downloaded_body_bytes,
    })
}

fn validate_all(
    materializer: &LocalDirFileMaterializer,
    plan: &LocalDirReconciliationPlan,
    cancellation: &dyn Cancellation,
    may_have_changed: bool,
) -> Result<(), LocalDirReconciliationError> {
    for target in &plan.targets {
        let inspection = materializer
            .inspect(target, cancellation)
            .map_err(|source| {
                LocalDirReconciliationError::materialization(source, may_have_changed)
            })?;
        if inspection != LocalDirDestinationInspection::Exact {
            return Err(LocalDirReconciliationError::final_validation(
                may_have_changed,
            ));
        }
    }
    check_cancellation(cancellation, may_have_changed)
}

fn check_cancellation(
    cancellation: &dyn Cancellation,
    may_have_changed: bool,
) -> Result<(), LocalDirReconciliationError> {
    if cancellation.is_cancelled() {
        Err(LocalDirReconciliationError::cancelled(may_have_changed))
    } else {
        Ok(())
    }
}

impl Debug for LocalDirReconciler {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirReconciler")
            .finish_non_exhaustive()
    }
}

fn report_provenance(provenance: PreparedSourceProvenance) -> (LocalDirFileProvenance, u64) {
    match provenance {
        PreparedSourceProvenance::Owned => (LocalDirFileProvenance::OwnedCache, 0),
        PreparedSourceProvenance::Compatible => (LocalDirFileProvenance::CompatibleCache, 0),
        PreparedSourceProvenance::NewlyAcquired {
            downloaded_body_bytes,
        } => (
            LocalDirFileProvenance::NewlyAcquiredCache,
            downloaded_body_bytes,
        ),
    }
}

fn checked_add(
    total: u64,
    addition: u64,
    may_have_changed: bool,
) -> Result<u64, LocalDirReconciliationError> {
    total
        .checked_add(addition)
        .ok_or_else(|| LocalDirReconciliationError::accounting_overflow(may_have_changed))
}

#[derive(Debug)]
pub(super) struct LocalDirSourceError {
    kind: LocalDirSourceErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum LocalDirSourceErrorKind {
    Io(SanitizedIo),
    Cache(CacheError),
    Invalid,
}

impl LocalDirSourceError {
    pub(super) fn io(source: &io::Error) -> Self {
        Self {
            kind: LocalDirSourceErrorKind::Io(SanitizedIo::new(source)),
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) fn invalid() -> Self {
        Self {
            kind: LocalDirSourceErrorKind::Invalid,
            backtrace: Backtrace::capture(),
        }
    }

    fn cache(source: CacheError) -> Self {
        Self {
            kind: LocalDirSourceErrorKind::Cache(source),
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) const fn is_io(&self) -> bool {
        matches!(self.kind, LocalDirSourceErrorKind::Io(_))
    }

    pub(super) const fn is_invalid(&self) -> bool {
        matches!(self.kind, LocalDirSourceErrorKind::Invalid)
    }

    pub(super) const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for LocalDirSourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.kind {
            LocalDirSourceErrorKind::Io(_) => {
                formatter.write_str("local cache candidate filesystem operation failed")
            }
            LocalDirSourceErrorKind::Cache(_) => {
                formatter.write_str("owned cache candidate could not be opened")
            }
            LocalDirSourceErrorKind::Invalid => {
                formatter.write_str("local cache candidate failed validation")
            }
        }
    }
}

impl Error for LocalDirSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            LocalDirSourceErrorKind::Cache(source) => Some(source),
            LocalDirSourceErrorKind::Io(_) | LocalDirSourceErrorKind::Invalid => None,
        }
    }
}

impl From<io::Error> for LocalDirSourceError {
    fn from(source: io::Error) -> Self {
        Self::io(&source)
    }
}

#[derive(Debug)]
pub(super) struct LocalDirReconciliationError {
    kind: Box<LocalDirReconciliationErrorKind>,
    may_have_changed: bool,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum LocalDirReconciliationErrorKind {
    Plan(ValidationError),
    Lock(SanitizedIo),
    LockWait(SanitizedIo),
    Source(LocalDirSourceError),
    Materialization(LocalDirMaterializationError),
    Metadata(HubMetadataError),
    Bookkeeping(LocalDirBookkeepingWriteError),
    Completion(LocalDirCompletionError),
    Conflict,
    Cancelled,
    FinalValidation,
    AccountingOverflow,
}

impl LocalDirReconciliationError {
    fn new(kind: LocalDirReconciliationErrorKind, may_have_changed: bool) -> Self {
        Self {
            kind: Box::new(kind),
            may_have_changed,
            backtrace: Backtrace::capture(),
        }
    }

    fn plan(source: ValidationError) -> Self {
        Self::new(LocalDirReconciliationErrorKind::Plan(source), false)
    }

    fn lock(source: &io::Error) -> Self {
        Self::new(
            LocalDirReconciliationErrorKind::Lock(SanitizedIo::new(source)),
            false,
        )
    }

    fn lock_wait(source: &io::Error) -> Self {
        Self::new(
            LocalDirReconciliationErrorKind::LockWait(SanitizedIo::new(source)),
            false,
        )
    }

    fn source(source: LocalDirSourceError) -> Self {
        Self::new(LocalDirReconciliationErrorKind::Source(source), false)
    }

    fn materialization(source: LocalDirMaterializationError, prefix_changed: bool) -> Self {
        let may_have_changed = prefix_changed || source.may_have_published();
        Self::new(
            LocalDirReconciliationErrorKind::Materialization(source),
            may_have_changed,
        )
    }

    fn completion(source: LocalDirCompletionError) -> Self {
        let may_have_changed = source.may_have_published();
        Self::new(
            LocalDirReconciliationErrorKind::Completion(source),
            may_have_changed,
        )
    }

    fn metadata(source: HubMetadataError) -> Self {
        Self::new(LocalDirReconciliationErrorKind::Metadata(source), false)
    }

    fn bookkeeping(source: LocalDirBookkeepingWriteError) -> Self {
        let may_have_changed = source.may_have_published();
        Self::new(
            LocalDirReconciliationErrorKind::Bookkeeping(source),
            may_have_changed,
        )
    }

    fn with_change(mut self) -> Self {
        self.may_have_changed = true;
        self
    }

    fn conflict(may_have_changed: bool) -> Self {
        Self::new(LocalDirReconciliationErrorKind::Conflict, may_have_changed)
    }

    fn cancelled(may_have_changed: bool) -> Self {
        Self::new(LocalDirReconciliationErrorKind::Cancelled, may_have_changed)
    }

    fn final_validation(may_have_changed: bool) -> Self {
        Self::new(
            LocalDirReconciliationErrorKind::FinalValidation,
            may_have_changed,
        )
    }

    fn accounting_overflow(may_have_changed: bool) -> Self {
        Self::new(
            LocalDirReconciliationErrorKind::AccountingOverflow,
            may_have_changed,
        )
    }

    pub(super) fn is_plan_invalid(&self) -> bool {
        matches!(self.kind.as_ref(), LocalDirReconciliationErrorKind::Plan(_))
    }

    pub(super) fn is_conflict(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::Conflict
        ) || matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::Materialization(source) if source.is_conflict()
        )
    }

    pub(super) fn is_cancelled(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::Cancelled
        ) || matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::Materialization(source) if source.is_cancelled()
        )
    }

    pub(super) fn is_source(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::Source(_)
        )
    }

    pub(super) fn is_final_validation(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            LocalDirReconciliationErrorKind::FinalValidation
        )
    }

    pub(super) const fn may_have_changed(&self) -> bool {
        self.may_have_changed
    }

    pub(super) const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for LocalDirReconciliationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind.as_ref() {
            LocalDirReconciliationErrorKind::Plan(_) => {
                "local-dir reconciliation plan validation failed"
            }
            LocalDirReconciliationErrorKind::Lock(_) => {
                "local-dir reconciliation lock operation failed"
            }
            LocalDirReconciliationErrorKind::LockWait(_) => {
                "local-dir reconciliation lock wait failed"
            }
            LocalDirReconciliationErrorKind::Source(_) => {
                "local-dir cache candidate preparation failed"
            }
            LocalDirReconciliationErrorKind::Materialization(_) => {
                "local-dir selected file reconciliation failed"
            }
            LocalDirReconciliationErrorKind::Metadata(_) => {
                "local-dir compatible tree construction failed"
            }
            LocalDirReconciliationErrorKind::Bookkeeping(_) => {
                "local-dir compatible bookkeeping publication failed"
            }
            LocalDirReconciliationErrorKind::Completion(_) => {
                "local-dir completion state publication failed"
            }
            LocalDirReconciliationErrorKind::Conflict => {
                "local-dir destination conflicts with the selected files"
            }
            LocalDirReconciliationErrorKind::Cancelled => "local-dir reconciliation cancelled",
            LocalDirReconciliationErrorKind::FinalValidation => {
                "local-dir final selected-file validation failed"
            }
            LocalDirReconciliationErrorKind::AccountingOverflow => {
                "local-dir reconciliation byte accounting overflowed"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for LocalDirReconciliationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            LocalDirReconciliationErrorKind::Plan(source) => Some(source),
            LocalDirReconciliationErrorKind::Source(source) => Some(source),
            LocalDirReconciliationErrorKind::Materialization(source) => Some(source),
            LocalDirReconciliationErrorKind::Metadata(source) => Some(source),
            LocalDirReconciliationErrorKind::Bookkeeping(source) => Some(source),
            LocalDirReconciliationErrorKind::Completion(source) => Some(source),
            LocalDirReconciliationErrorKind::Lock(_)
            | LocalDirReconciliationErrorKind::LockWait(_)
            | LocalDirReconciliationErrorKind::Conflict
            | LocalDirReconciliationErrorKind::Cancelled
            | LocalDirReconciliationErrorKind::FinalValidation
            | LocalDirReconciliationErrorKind::AccountingOverflow => None,
        }
    }
}

impl From<ValidationError> for LocalDirReconciliationError {
    fn from(source: ValidationError) -> Self {
        Self::plan(source)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::{self, File, FileTimes};
    use std::io::Cursor;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread;
    use std::time::{Duration, SystemTime};

    use serde_json::json;
    use sha1::{Digest as _, Sha1};
    use tempfile::TempDir;

    use crate::cache::filter::RepositoryFilter;
    use crate::cache::hub_metadata::{HubTreeEntry, decode_local_download, decode_tree};
    use crate::cache::metadata::{LocalDirStateRecord, decode_record};
    use crate::cache::publication::{
        NoPublicationFaults, OsFileSystem, PublicationFaults, PublicationPoint,
        SequenceOperationIds, SystemClock,
    };
    use crate::cache::rooted_fs::CacheRoot;
    use crate::{Endpoint, RepositoryId, RepositorySpec};

    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const FIRST: &[u8] = b"first validated file";
    const SECOND: &[u8] = b"second validated file";
    const THIRD: &[u8] = b"third validated file";

    #[test]
    fn rust_local_dir_conformance_emitter_replaces_user_bytes_and_reopens_offline()
    -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let inventory = emit_rust_local_dir_conformance(directory.path())?;
        let value: serde_json::Value = serde_json::from_slice(&fs::read(inventory)?)?;
        let fixture = &value["local_directories"][0];
        assert_eq!(fixture["repo_id"], "fixture-org/fixture-rust-local-dir");
        assert_eq!(fixture["files"].as_array().map(Vec::len), Some(2));
        assert_eq!(
            fs::read(directory.path().join("local-dir/config.json"))?,
            FIRST
        );
        Ok(())
    }

    #[test]
    #[ignore = "invoked by the pinned-Python conformance job with an explicit output path"]
    fn emit_python_local_dir_conformance_fixture() -> Result<(), Box<dyn Error>> {
        let output = std::env::var_os("HF_STORE_CONFORMANCE_OUTPUT")
            .map(PathBuf::from)
            .ok_or("HF_STORE_CONFORMANCE_OUTPUT is required")?;
        let inventory = emit_rust_local_dir_conformance(&output)?;
        println!("{}", inventory.display());
        Ok(())
    }

    #[test]
    #[ignore = "invoked by the pinned-Python conformance job with a generated local_dir"]
    fn reuse_python_written_local_dir_and_reopen_offline() -> Result<(), Box<dyn Error>> {
        let inventory = std::env::var_os("HF_STORE_PYTHON_LOCAL_DIR_INVENTORY")
            .map(PathBuf::from)
            .ok_or("HF_STORE_PYTHON_LOCAL_DIR_INVENTORY is required")?;
        let corpus = inventory.parent().ok_or("Python inventory has no parent")?;
        let local_dir = corpus.join("local-dir");
        let endpoint = Endpoint::hugging_face();
        let repository =
            RepositorySpec::model(RepositoryId::parse("fixture-org/fixture-local-dir")?);
        let commit = CommitId::parse("4444444444444444444444444444444444444444")?;
        let layout = HubLocalDirLayout::new(&local_dir, &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(&local_dir)?);
        let tree = decode_tree(&fs::read(layout.tree_path(&commit))?)?;
        let mut original = BTreeMap::new();
        let mut targets = Vec::with_capacity(tree.files().len());
        for (path, entry) in tree.files() {
            let bytes = fs::read(layout.file_path(path)?)?;
            original.insert(path.clone(), bytes.clone());
            targets.push(LocalDirFileTarget::new(
                path.clone(),
                entry.clone(),
                BlobDigest::for_bytes(&bytes),
            ));
        }
        let selection = RepositoryFilter::new(None, &[])
            .select(targets.iter().map(|target| target.path().clone()))?;
        let plan =
            LocalDirReconciliationPlan::new(layout.clone(), commit.clone(), &selection, targets)?;
        let reconciler = LocalDirReconciler::new(
            Arc::clone(&root),
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(SystemClock),
                Arc::new(NoPublicationFaults),
            ),
            Arc::new(YieldWait::default()),
        );
        let initial = report(reconciler.reconcile(
            &plan,
            &mut CandidateMap::default(),
            ExistingFilePolicy::Reject,
            &super::super::local_dir_materialization::NeverCancelled,
        )?)?;
        assert!(
            initial
                .files()
                .iter()
                .all(|file| file.provenance() == LocalDirFileProvenance::ExistingDestination)
        );
        let config = plan
            .targets()
            .iter()
            .find(|target| target.path().as_str() == "config/fixture.json")
            .ok_or("Python fixture config target is missing")?;
        fs::write(layout.file_path(config.path())?, b"user edit")?;
        let config_bytes = original
            .get(config.path())
            .ok_or("Python fixture config bytes are missing")?;
        let mut replacement = CandidateMap::default().with_compatible(config, config_bytes);
        let replaced = report(reconciler.reconcile(
            &plan,
            &mut replacement,
            ExistingFilePolicy::ReplaceRegularFile,
            &super::super::local_dir_materialization::NeverCancelled,
        )?)?;
        assert_eq!(
            replaced.files()[0].disposition(),
            LocalDirFileDisposition::Copied
        );
        let _offline =
            super::super::local_dir_completion::LocalDirOfflineReader::new(layout, root)?
                .open(&commit, selection.selection_id())?;
        Ok(())
    }

    fn emit_rust_local_dir_conformance(output: &Path) -> Result<PathBuf, Box<dyn Error>> {
        fs::create_dir_all(output)?;
        let local_dir = output.join("local-dir");
        fs::create_dir(&local_dir)?;
        let endpoint = Endpoint::hugging_face();
        let repository =
            RepositorySpec::model(RepositoryId::parse("fixture-org/fixture-rust-local-dir")?);
        let commit = CommitId::parse(COMMIT)?;
        let layout = HubLocalDirLayout::new(&local_dir, &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(&local_dir)?);
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(SequenceOperationIds::new(1)),
            Arc::new(SystemClock),
            Arc::new(NoPublicationFaults),
        );
        let config = LocalDirFileTarget::new(
            RepoPath::parse("config.json")?,
            git_entry(FIRST)?,
            BlobDigest::for_bytes(FIRST),
        );
        let weights_digest = BlobDigest::for_bytes(SECOND);
        let weights = LocalDirFileTarget::new(
            RepoPath::parse("weights/model.safetensors")?,
            HubTreeEntry::new(
                u64::try_from(SECOND.len())?,
                "2222222222222222222222222222222222222222",
            )?
            .with_lfs(weights_digest.to_string(), u64::try_from(SECOND.len())?)?,
            weights_digest,
        );
        let targets = vec![config.clone(), weights.clone()];
        let selection = RepositoryFilter::new(None, &[])
            .select(targets.iter().map(|target| target.path().clone()))?;
        let plan =
            LocalDirReconciliationPlan::new(layout.clone(), commit.clone(), &selection, targets)?;
        let reconciler =
            LocalDirReconciler::new(Arc::clone(&root), effects, Arc::new(YieldWait::default()));
        let mut initial = CandidateMap::default()
            .with_new(&config, FIRST, u64::try_from(FIRST.len())?)
            .with_new(&weights, SECOND, u64::try_from(SECOND.len())?);
        let _initial_report = report(reconciler.reconcile(
            &plan,
            &mut initial,
            ExistingFilePolicy::Reject,
            &super::super::local_dir_materialization::NeverCancelled,
        )?)?;

        fs::write(layout.file_path(config.path())?, b"user-modified bytes")?;
        let mut replacement =
            CandidateMap::default().with_new(&config, FIRST, u64::try_from(FIRST.len())?);
        let _replacement_report = report(reconciler.reconcile(
            &plan,
            &mut replacement,
            ExistingFilePolicy::ReplaceRegularFile,
            &super::super::local_dir_materialization::NeverCancelled,
        )?)?;
        let _offline =
            super::super::local_dir_completion::LocalDirOfflineReader::new(layout.clone(), root)?
                .open(&commit, selection.selection_id())?;

        let files = [config, weights]
            .into_iter()
            .map(|target| {
                let metadata_path = layout.download_metadata_path(target.path())?;
                let metadata = decode_local_download(&fs::read(&metadata_path)?)?;
                let entry = target.entry();
                Ok(json!({
                    "path": target.path().as_str(),
                    "metadata_path": relative_posix(&local_dir, &metadata_path)?,
                    "etag": metadata.etag(),
                    "metadata_timestamp": metadata.timestamp(),
                    "size": entry.size(),
                    "content_sha256": target.digest().to_string(),
                    "blob_id": entry.blob_id(),
                    "lfs_sha256": entry.lfs_sha256(),
                    "lfs_size": entry.lfs_size(),
                }))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        let inventory = json!({
            "format_version": 1,
            "local_directories": [{
                "path": "local-dir",
                "repo_type": "model",
                "repo_id": repository.id().as_str(),
                "commit": commit.as_str(),
                "tree_path": relative_posix(&local_dir, &layout.tree_path(&commit))?,
                "gitignore_path": relative_posix(&local_dir, &layout.gitignore_path())?,
                "cachedir_tag_path": relative_posix(&local_dir, &layout.cachedir_tag_path())?,
                "files": files,
            }],
        });
        let inventory_path = output.join("local-dir-inventory.json");
        let mut bytes = serde_json::to_vec_pretty(&inventory)?;
        bytes.push(b'\n');
        fs::write(&inventory_path, bytes)?;
        Ok(inventory_path)
    }

    fn relative_posix(base: &Path, path: &Path) -> Result<String, io::Error> {
        let relative = path
            .strip_prefix(base)
            .map_err(|_outside| io::Error::other("conformance path escaped its base"))?;
        let rendered = relative.to_string_lossy().replace('\\', "/");
        if rendered.is_empty() || rendered.starts_with('/') || rendered.contains(':') {
            Err(io::Error::other("conformance path is not portable"))
        } else {
            Ok(rendered)
        }
    }

    fn git_entry(bytes: &[u8]) -> Result<HubTreeEntry, Box<dyn Error>> {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
        hasher.update(bytes);
        Ok(HubTreeEntry::new(
            u64::try_from(bytes.len())?,
            format!("{:x}", hasher.finalize()),
        )?)
    }

    struct Fixture {
        directory: TempDir,
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            Self::with_faults(Arc::new(NoPublicationFaults))
        }

        fn with_faults(faults: Arc<dyn PublicationFaults>) -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
            let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
            let effects = Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(SequenceOperationIds::new(1)),
                Arc::new(SystemClock),
                faults,
            );
            Ok(Self {
                directory,
                layout,
                root,
                effects,
            })
        }

        fn target(path: &str, bytes: &[u8]) -> Result<LocalDirFileTarget, Box<dyn Error>> {
            Ok(LocalDirFileTarget::new(
                RepoPath::parse(path)?,
                HubTreeEntry::new(u64::try_from(bytes.len())?, "opaque-validator")?,
                BlobDigest::for_bytes(bytes),
            ))
        }

        fn plan(
            &self,
            targets: Vec<LocalDirFileTarget>,
        ) -> Result<LocalDirReconciliationPlan, Box<dyn Error>> {
            let selection = RepositoryFilter::new(None, &[])
                .select(targets.iter().map(|target| target.path().clone()))?;
            Ok(LocalDirReconciliationPlan::new(
                self.layout.clone(),
                CommitId::parse(COMMIT)?,
                &selection,
                targets,
            )?)
        }

        fn reconciler(&self, wait: Arc<dyn LockWait>) -> LocalDirReconciler {
            LocalDirReconciler::new(Arc::clone(&self.root), self.effects.clone(), wait)
        }

        fn destination(&self, path: &RepoPath) -> PathBuf {
            self.directory.path().join(path.as_str())
        }

        fn write(&self, target: &LocalDirFileTarget, bytes: &[u8]) -> io::Result<()> {
            let destination = self.destination(target.path());
            let parent = destination
                .parent()
                .ok_or_else(|| io::Error::other("test destination has no parent"))?;
            fs::create_dir_all(parent)?;
            fs::write(destination, bytes)
        }

        fn completion(&self) -> Result<Option<LocalDirStateRecord>, Box<dyn Error>> {
            let path = self.layout.coordination_state_path();
            if !path.try_exists()? {
                return Ok(None);
            }
            Ok(Some(decode_record(&fs::read(path)?)?))
        }
    }

    #[derive(Debug, Default)]
    struct YieldWait {
        calls: AtomicUsize,
    }

    impl LockWait for YieldWait {
        fn wait(&self, _cancellation: &dyn Cancellation) -> io::Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            thread::yield_now();
            Ok(())
        }
    }

    enum CandidateValue {
        Owned(Vec<u8>),
        Compatible(Vec<u8>),
        NewlyAcquired(Vec<u8>, u64),
        Reader(Box<dyn Read + Send>, PreparedSourceProvenance),
    }

    impl Debug for CandidateValue {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str("CandidateValue(..)")
        }
    }

    #[derive(Debug, Default)]
    struct CandidateMap {
        values: BTreeMap<RepoPath, CandidateValue>,
        calls: Arc<Mutex<Vec<RepoPath>>>,
    }

    impl CandidateMap {
        fn with_owned(mut self, target: &LocalDirFileTarget, bytes: &[u8]) -> Self {
            self.values
                .insert(target.path().clone(), CandidateValue::Owned(bytes.to_vec()));
            self
        }

        fn with_compatible(mut self, target: &LocalDirFileTarget, bytes: &[u8]) -> Self {
            self.values.insert(
                target.path().clone(),
                CandidateValue::Compatible(bytes.to_vec()),
            );
            self
        }

        fn with_new(mut self, target: &LocalDirFileTarget, bytes: &[u8], downloaded: u64) -> Self {
            self.values.insert(
                target.path().clone(),
                CandidateValue::NewlyAcquired(bytes.to_vec(), downloaded),
            );
            self
        }

        fn with_reader(
            mut self,
            target: &LocalDirFileTarget,
            reader: Box<dyn Read + Send>,
            provenance: PreparedSourceProvenance,
        ) -> Self {
            self.values.insert(
                target.path().clone(),
                CandidateValue::Reader(reader, provenance),
            );
            self
        }

        fn calls(&self) -> Result<Vec<RepoPath>, Box<dyn Error>> {
            Ok(self
                .calls
                .lock()
                .map_err(|_poisoned| "candidate call log lock poisoned")?
                .clone())
        }
    }

    impl LocalDirCandidateSet for CandidateMap {
        fn prepare_local(
            &mut self,
            target: &LocalDirFileTarget,
            _cancellation: &dyn Cancellation,
        ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
            self.calls
                .lock()
                .map_err(|_poisoned| LocalDirSourceError::invalid())?
                .push(target.path().clone());
            Ok(self.values.remove(target.path()).map(|value| match value {
                CandidateValue::Owned(bytes) => {
                    PreparedLocalDirSource::owned_cache(Box::new(Cursor::new(bytes)))
                }
                CandidateValue::Compatible(bytes) => {
                    PreparedLocalDirSource::compatible_cache(Box::new(Cursor::new(bytes)))
                }
                CandidateValue::NewlyAcquired(bytes, downloaded) => {
                    PreparedLocalDirSource::newly_acquired_cache(
                        Box::new(Cursor::new(bytes)),
                        downloaded,
                    )
                }
                CandidateValue::Reader(reader, provenance) => {
                    PreparedLocalDirSource { reader, provenance }
                }
            }))
        }
    }

    #[derive(Debug, Default)]
    struct AtomicCancellation(AtomicBool);

    impl Cancellation for AtomicCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    fn report(
        outcome: LocalDirReconciliationOutcome,
    ) -> Result<LocalDirReconciliationReport, Box<dyn Error>> {
        match outcome {
            LocalDirReconciliationOutcome::Reconciled(report) => Ok(report),
            LocalDirReconciliationOutcome::NeedsTransport(_) => {
                Err("reconciliation unexpectedly needed transport".into())
            }
        }
    }

    #[test]
    fn all_exact_destinations_need_no_candidate_source_and_keep_mtime() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a/config.json", FIRST)?;
        let second = Fixture::target("b/model.bin", SECOND)?;
        fixture.write(&first, FIRST)?;
        fixture.write(&second, SECOND)?;
        let fixed_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let first_path = fixture.destination(first.path());
        File::options()
            .write(true)
            .open(&first_path)?
            .set_times(FileTimes::new().set_modified(fixed_time))?;
        let before = fs::metadata(&first_path)?.modified()?;
        let plan = fixture.plan(vec![second, first])?;
        let mut candidates = CandidateMap::default();

        let result = report(
            fixture
                .reconciler(Arc::new(YieldWait::default()))
                .reconcile(
                    &plan,
                    &mut candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )?,
        )?;

        assert!(candidates.calls()?.is_empty());
        assert_eq!(result.files().len(), 2);
        assert!(
            result
                .files()
                .iter()
                .all(|file| file.disposition() == LocalDirFileDisposition::Reused)
        );
        assert!(
            result
                .files()
                .iter()
                .all(|file| file.provenance() == LocalDirFileProvenance::ExistingDestination)
        );
        assert_eq!(result.destination_bytes_written(), 0);
        assert_eq!(result.downloaded_body_bytes(), 0);
        assert_eq!(fs::metadata(first_path)?.modified()?, before);
        assert_eq!(result.commit(), plan.commit());
        assert_eq!(result.selection_id(), plan.selection_id());
        let completion = fixture
            .completion()?
            .ok_or("successful reconciliation did not publish completion")?;
        assert!(completion.is_complete());
        assert_eq!(completion.commit(), plan.commit().as_str());
        assert_eq!(completion.selection_id(), plan.selection_id().to_string());
        assert_eq!(completion.files().len(), 2);
        Ok(())
    }

    #[test]
    fn mixed_exact_and_missing_calls_candidate_only_for_missing_file() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let exact = Fixture::target("a.json", FIRST)?;
        let missing = Fixture::target("z.bin", SECOND)?;
        fixture.write(&exact, FIRST)?;
        let plan = fixture.plan(vec![missing.clone(), exact.clone()])?;
        let mut candidates = CandidateMap::default().with_owned(&missing, SECOND);

        let result = report(
            fixture
                .reconciler(Arc::new(YieldWait::default()))
                .reconcile(
                    &plan,
                    &mut candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )?,
        )?;

        assert_eq!(candidates.calls()?, vec![missing.path().clone()]);
        assert_eq!(fs::read(fixture.destination(missing.path()))?, SECOND);
        assert_eq!(
            result.files()[0].disposition(),
            LocalDirFileDisposition::Reused
        );
        assert_eq!(
            result.files()[1].disposition(),
            LocalDirFileDisposition::Copied
        );
        assert_eq!(
            result.destination_bytes_written(),
            u64::try_from(SECOND.len())?
        );
        assert_eq!(result.downloaded_body_bytes(), 0);
        Ok(())
    }

    #[test]
    fn initial_default_conflict_prevents_candidate_calls_and_earlier_writes()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a-first.bin", FIRST)?;
        let conflicting = Fixture::target("z-conflict.bin", SECOND)?;
        fixture.write(&conflicting, b"user bytes")?;
        let plan = fixture.plan(vec![conflicting.clone(), first.clone()])?;
        let mut candidates = CandidateMap::default()
            .with_owned(&first, FIRST)
            .with_owned(&conflicting, SECOND);

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )
            .expect_err("default mismatch must be a conflict");

        assert!(error.is_conflict());
        assert!(!error.may_have_changed());
        assert!(candidates.calls()?.is_empty());
        assert!(!fixture.destination(first.path()).try_exists()?);
        assert_eq!(
            fs::read(fixture.destination(conflicting.path()))?,
            b"user bytes"
        );
        Ok(())
    }

    #[test]
    fn absent_candidates_return_sorted_transport_demand_without_writes()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a.bin", FIRST)?;
        let second = Fixture::target("z.bin", SECOND)?;
        let plan = fixture.plan(vec![second.clone(), first.clone()])?;
        let mut candidates = CandidateMap::default();

        let outcome = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )?;
        let demand = match outcome {
            LocalDirReconciliationOutcome::NeedsTransport(demand) => demand,
            LocalDirReconciliationOutcome::Reconciled(_) => {
                return Err("missing candidates unexpectedly reconciled".into());
            }
        };

        assert_eq!(
            demand
                .targets()
                .iter()
                .map(LocalDirFileTarget::path)
                .collect::<Vec<_>>(),
            vec![first.path(), second.path()]
        );
        assert_eq!(demand.commit(), plan.commit());
        assert_eq!(demand.selection_id(), plan.selection_id());
        assert!(!fixture.destination(first.path()).try_exists()?);
        assert!(!fixture.destination(second.path()).try_exists()?);
        assert!(fixture.completion()?.is_none());
        Ok(())
    }

    #[test]
    fn failed_file_copy_leaves_in_progress_state_instead_of_false_completion()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let mut candidates = CandidateMap::default().with_owned(&target, b"wrong bytes");

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )
            .expect_err("invalid candidate unexpectedly completed reconciliation");

        assert!(error.may_have_changed());
        let state = fixture
            .completion()?
            .ok_or("reconciliation did not invalidate completion before copying")?;
        assert!(state.is_in_progress());
        assert_eq!(state.commit(), plan.commit().as_str());
        assert_eq!(state.selection_id(), plan.selection_id().to_string());
        assert!(state.files().is_empty());
        assert!(!fixture.destination(target.path()).try_exists()?);
        Ok(())
    }

    #[test]
    fn canonical_report_keeps_downloaded_and_destination_bytes_separate()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a.bin", FIRST)?;
        let second = Fixture::target("z.bin", SECOND)?;
        let plan = fixture.plan(vec![second.clone(), first.clone()])?;
        let downloaded = 9_999;
        let mut candidates = CandidateMap::default()
            .with_compatible(&first, FIRST)
            .with_new(&second, SECOND, downloaded);

        let result = report(
            fixture
                .reconciler(Arc::new(YieldWait::default()))
                .reconcile(
                    &plan,
                    &mut candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )?,
        )?;

        assert_eq!(result.files()[0].path(), first.path());
        assert_eq!(result.files()[1].path(), second.path());
        assert_eq!(
            result.files()[0].provenance(),
            LocalDirFileProvenance::CompatibleCache
        );
        assert_eq!(
            result.files()[1].provenance(),
            LocalDirFileProvenance::NewlyAcquiredCache
        );
        let expected_written = u64::try_from(FIRST.len() + SECOND.len())?;
        assert_eq!(result.destination_bytes_written(), expected_written);
        assert_eq!(result.downloaded_body_bytes(), downloaded);
        assert_eq!(result.files()[0].downloaded_body_bytes(), 0);
        assert_eq!(result.files()[1].downloaded_body_bytes(), downloaded);
        assert_eq!(
            result.files()[0].destination_bytes_written(),
            u64::try_from(FIRST.len())?
        );
        assert_eq!(result.files()[0].size(), u64::try_from(FIRST.len())?);
        assert_eq!(result.files()[0].digest(), first.digest());
        Ok(())
    }

    #[test]
    fn owned_blob_cache_hit_materializes_without_network_bytes() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let endpoint = Endpoint::hugging_face();
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let owned_root = fixture.directory.path().join("owned-cache");
        fs::create_dir_all(&owned_root)?;
        let kernel =
            CacheKernel::new(&owned_root, &endpoint, &repository, fixture.effects.clone())?;
        kernel.initialize()?;
        kernel.publish_blob(
            Cursor::new(FIRST),
            u64::try_from(FIRST.len())?,
            target.digest(),
        )?;
        let retained_cache = kernel.clone();
        let mut candidates = OwnedBlobCandidates::new(kernel);

        let result = report(
            fixture
                .reconciler(Arc::new(YieldWait::default()))
                .reconcile(
                    &plan,
                    &mut candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )?,
        )?;

        assert_eq!(fs::read(fixture.destination(target.path()))?, FIRST);
        assert_eq!(result.downloaded_body_bytes(), 0);
        assert_eq!(
            result.files()[0].provenance(),
            LocalDirFileProvenance::OwnedCache
        );
        assert_eq!(
            result.destination_bytes_written(),
            u64::try_from(FIRST.len())?
        );
        fs::write(fixture.destination(target.path()), b"user edit")?;
        let mut retained_blob = retained_cache
            .open_blob(&target.digest(), u64::try_from(FIRST.len())?)?
            .ok_or("owned blob disappeared after local directory edit")?;
        let mut retained_bytes = Vec::new();
        retained_blob.read_to_end(&mut retained_bytes)?;
        assert_eq!(retained_bytes, FIRST);
        Ok(())
    }

    #[derive(Debug)]
    struct ConcurrentExactCandidates {
        destination: PathBuf,
        bytes: Vec<u8>,
        downloaded: u64,
    }

    impl LocalDirCandidateSet for ConcurrentExactCandidates {
        fn prepare_local(
            &mut self,
            _target: &LocalDirFileTarget,
            _cancellation: &dyn Cancellation,
        ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
            fs::write(&self.destination, &self.bytes)
                .map_err(|source| LocalDirSourceError::io(&source))?;
            Ok(Some(PreparedLocalDirSource::newly_acquired_cache(
                Box::new(Cursor::new(self.bytes.clone())),
                self.downloaded,
            )))
        }
    }

    #[test]
    fn prepared_source_that_rechecks_as_exact_reports_existing_destination()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let downloaded = 321;
        let mut candidates = ConcurrentExactCandidates {
            destination: fixture.destination(target.path()),
            bytes: FIRST.to_vec(),
            downloaded,
        };

        let result = report(
            fixture
                .reconciler(Arc::new(YieldWait::default()))
                .reconcile(
                    &plan,
                    &mut candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )?,
        )?;

        assert_eq!(
            result.files()[0].disposition(),
            LocalDirFileDisposition::Reused
        );
        assert_eq!(
            result.files()[0].provenance(),
            LocalDirFileProvenance::ExistingDestination
        );
        assert_eq!(result.destination_bytes_written(), 0);
        assert_eq!(result.downloaded_body_bytes(), downloaded);
        Ok(())
    }

    #[derive(Debug)]
    struct LateConflictCandidates {
        conflict_destination: PathBuf,
        values: BTreeMap<RepoPath, Vec<u8>>,
        mutated: bool,
    }

    impl LocalDirCandidateSet for LateConflictCandidates {
        fn prepare_local(
            &mut self,
            target: &LocalDirFileTarget,
            _cancellation: &dyn Cancellation,
        ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
            if !self.mutated {
                fs::write(&self.conflict_destination, b"late user edit")
                    .map_err(|source| LocalDirSourceError::io(&source))?;
                self.mutated = true;
            }
            Ok(self
                .values
                .remove(target.path())
                .map(|bytes| PreparedLocalDirSource::owned_cache(Box::new(Cursor::new(bytes)))))
        }
    }

    #[test]
    fn conflict_discovered_after_source_preparation_prevents_earlier_selected_writes()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a-first.bin", FIRST)?;
        let conflict = Fixture::target("z-conflict.bin", SECOND)?;
        let plan = fixture.plan(vec![first.clone(), conflict.clone()])?;
        let values = BTreeMap::from([
            (first.path().clone(), FIRST.to_vec()),
            (conflict.path().clone(), SECOND.to_vec()),
        ]);
        let mut candidates = LateConflictCandidates {
            conflict_destination: fixture.destination(conflict.path()),
            values,
            mutated: false,
        };

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )
            .expect_err("late mismatch must stop execution before the first target");

        assert!(error.is_conflict());
        assert!(!error.may_have_changed());
        assert!(!fixture.destination(first.path()).try_exists()?);
        assert_eq!(
            fs::read(fixture.destination(conflict.path()))?,
            b"late user edit"
        );
        Ok(())
    }

    #[derive(Debug)]
    struct CancelWait {
        cancellation: Arc<AtomicCancellation>,
        calls: AtomicUsize,
    }

    impl LockWait for CancelWait {
        fn wait(&self, _cancellation: &dyn Cancellation) -> io::Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.cancellation.0.store(true, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn lock_contention_wait_is_cancellable_before_sources_or_writes() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let held = match fixture
            .root
            .try_lock_exclusive(&plan.coordination_lock_relative)?
        {
            RootedLockAttempt::Acquired(guard) => guard,
            RootedLockAttempt::Contended => return Err("test lock was already contended".into()),
        };
        let cancellation = Arc::new(AtomicCancellation::default());
        let wait = Arc::new(CancelWait {
            cancellation: Arc::clone(&cancellation),
            calls: AtomicUsize::new(0),
        });
        let wait_effect: Arc<dyn LockWait> = Arc::<CancelWait>::clone(&wait);
        let mut candidates = CandidateMap::default().with_owned(&target, FIRST);

        let error = fixture
            .reconciler(wait_effect)
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                cancellation.as_ref(),
            )
            .expect_err("contended reconciliation must observe cancellation");
        drop(held);

        assert!(error.is_cancelled());
        assert!(!error.may_have_changed());
        assert_eq!(wait.calls.load(Ordering::Relaxed), 1);
        assert!(candidates.calls()?.is_empty());
        assert!(!fixture.destination(target.path()).try_exists()?);
        Ok(())
    }

    #[derive(Debug)]
    struct CancelInterruptedWait {
        cancellation: Arc<AtomicCancellation>,
    }

    impl LockWait for CancelInterruptedWait {
        fn wait(&self, _cancellation: &dyn Cancellation) -> io::Result<()> {
            self.cancellation.0.store(true, Ordering::Release);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "wait interrupted with secret context",
            ))
        }
    }

    #[test]
    fn cancelled_interrupted_lock_wait_is_classified_as_cancellation() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let held = match fixture
            .root
            .try_lock_exclusive(&plan.coordination_lock_relative)?
        {
            RootedLockAttempt::Acquired(guard) => guard,
            RootedLockAttempt::Contended => return Err("test lock was already contended".into()),
        };
        let cancellation = Arc::new(AtomicCancellation::default());
        let mut candidates = CandidateMap::default().with_owned(&target, FIRST);

        let error = fixture
            .reconciler(Arc::new(CancelInterruptedWait {
                cancellation: Arc::clone(&cancellation),
            }))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                cancellation.as_ref(),
            )
            .expect_err("cancelled interrupted wait must fail");
        drop(held);

        assert!(error.is_cancelled());
        assert!(!error.may_have_changed());
        assert!(!format!("{error:?} {error}").contains("secret context"));
        Ok(())
    }

    #[derive(Debug)]
    struct CancellingCandidates {
        cancellation: Arc<AtomicCancellation>,
    }

    impl LocalDirCandidateSet for CancellingCandidates {
        fn prepare_local(
            &mut self,
            _target: &LocalDirFileTarget,
            cancellation: &dyn Cancellation,
        ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
            if cancellation.is_cancelled() {
                return Err(LocalDirSourceError::invalid());
            }
            self.cancellation.0.store(true, Ordering::Release);
            Err(LocalDirSourceError::io(&io::Error::new(
                io::ErrorKind::Interrupted,
                "candidate interrupted with secret context",
            )))
        }
    }

    #[test]
    fn cancellation_observed_after_candidate_hashing_wins_over_candidate_error()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let cancellation = Arc::new(AtomicCancellation::default());
        let mut candidates = CancellingCandidates {
            cancellation: Arc::clone(&cancellation),
        };

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                cancellation.as_ref(),
            )
            .expect_err("candidate cancellation must stop reconciliation");

        assert!(error.is_cancelled());
        assert!(!error.is_source());
        assert!(!error.may_have_changed());
        assert!(!fixture.destination(target.path()).try_exists()?);
        assert!(!format!("{error:?} {error}").contains("secret context"));
        Ok(())
    }

    struct CancelOnRead {
        inner: Cursor<Vec<u8>>,
        cancellation: Arc<AtomicCancellation>,
    }

    impl Read for CancelOnRead {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let read = self.inner.read(buffer)?;
            self.cancellation.0.store(true, Ordering::Release);
            Ok(read)
        }
    }

    #[test]
    fn cancellation_after_a_copied_prefix_reports_possible_change() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a.bin", FIRST)?;
        let second = Fixture::target("z.bin", SECOND)?;
        let plan = fixture.plan(vec![first.clone(), second.clone()])?;
        let cancellation = Arc::new(AtomicCancellation::default());
        let cancelling_reader = CancelOnRead {
            inner: Cursor::new(SECOND.to_vec()),
            cancellation: Arc::clone(&cancellation),
        };
        let mut candidates = CandidateMap::default()
            .with_owned(&first, FIRST)
            .with_reader(
                &second,
                Box::new(cancelling_reader),
                PreparedSourceProvenance::Owned,
            );

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                cancellation.as_ref(),
            )
            .expect_err("cancellation in the second file must fail");

        assert!(error.is_cancelled());
        assert!(error.may_have_changed());
        assert_eq!(fs::read(fixture.destination(first.path()))?, FIRST);
        assert!(!fixture.destination(second.path()).try_exists()?);
        Ok(())
    }

    #[derive(Debug)]
    struct NthPublicationFault {
        point: PublicationPoint,
        occurrence: usize,
        seen: AtomicUsize,
    }

    impl PublicationFaults for NthPublicationFault {
        fn check(&self, point: PublicationPoint) -> io::Result<()> {
            if point != self.point {
                return Ok(());
            }
            let occurrence = self.seen.fetch_add(1, Ordering::Relaxed) + 1;
            if occurrence == self.occurrence {
                Err(io::Error::other("injected replacement boundary failure"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn injected_first_middle_and_last_replacement_boundaries_preserve_prefix_semantics()
    -> Result<(), Box<dyn Error>> {
        for point in [
            PublicationPoint::BeforeAtomicReplace,
            PublicationPoint::AfterAtomicReplace,
        ] {
            for occurrence in 1..=3 {
                let faults = Arc::new(NthPublicationFault {
                    point,
                    occurrence,
                    seen: AtomicUsize::new(0),
                });
                let fixture = Fixture::with_faults(faults)?;
                let targets = vec![
                    Fixture::target("a.bin", FIRST)?,
                    Fixture::target("b.bin", SECOND)?,
                    Fixture::target("c.bin", THIRD)?,
                ];
                let plan = fixture.plan(targets.clone())?;
                let mut candidates = CandidateMap::default()
                    .with_owned(&targets[0], FIRST)
                    .with_owned(&targets[1], SECOND)
                    .with_owned(&targets[2], THIRD);

                let error = fixture
                    .reconciler(Arc::new(YieldWait::default()))
                    .reconcile(
                        &plan,
                        &mut candidates,
                        ExistingFilePolicy::Reject,
                        &super::super::local_dir_materialization::NeverCancelled,
                    )
                    .expect_err("injected boundary must fail reconciliation");

                let visible = targets
                    .iter()
                    .filter(|target| {
                        fixture
                            .destination(target.path())
                            .try_exists()
                            .unwrap_or(false)
                    })
                    .count();
                let expected_visible = if point == PublicationPoint::BeforeAtomicReplace {
                    occurrence - 1
                } else {
                    occurrence
                };
                assert_eq!(visible, expected_visible, "point={point:?}, n={occurrence}");
                assert!(error.may_have_changed(), "point={point:?}, n={occurrence}");
                let state = fixture
                    .completion()?
                    .ok_or("replacement failure lost the in-progress state")?;
                assert!(state.is_in_progress(), "point={point:?}, n={occurrence}");
            }
        }
        Ok(())
    }

    struct MutatingReader {
        inner: Cursor<Vec<u8>>,
        destination: PathBuf,
        mutated: bool,
    }

    impl Read for MutatingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if !self.mutated {
                fs::write(&self.destination, b"non-cooperating edit")?;
                self.mutated = true;
            }
            self.inner.read(buffer)
        }
    }

    #[test]
    fn final_reinspection_detects_noncooperating_edit_to_an_earlier_file()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = Fixture::target("a.bin", FIRST)?;
        let second = Fixture::target("z.bin", SECOND)?;
        let plan = fixture.plan(vec![first.clone(), second.clone()])?;
        let mut candidates = CandidateMap::default()
            .with_owned(&first, FIRST)
            .with_reader(
                &second,
                Box::new(MutatingReader {
                    inner: Cursor::new(SECOND.to_vec()),
                    destination: fixture.destination(first.path()),
                    mutated: false,
                }),
                PreparedSourceProvenance::Compatible,
            );

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )
            .expect_err("final reinspection must detect the external edit");

        assert!(error.is_final_validation());
        assert!(error.may_have_changed());
        assert_eq!(
            fs::read(fixture.destination(first.path()))?,
            b"non-cooperating edit"
        );
        assert_eq!(fs::read(fixture.destination(second.path()))?, SECOND);
        Ok(())
    }

    #[derive(Debug)]
    struct SignallingWait {
        signal: Mutex<Option<Sender<()>>>,
    }

    impl LockWait for SignallingWait {
        fn wait(&self, _cancellation: &dyn Cancellation) -> io::Result<()> {
            if let Some(signal) = self
                .signal
                .lock()
                .map_err(|_poisoned| io::Error::other("wait signal lock poisoned"))?
                .take()
            {
                signal
                    .send(())
                    .map_err(|_closed| io::Error::other("wait signal receiver closed"))?;
            }
            thread::yield_now();
            Ok(())
        }
    }

    struct BlockingReader {
        inner: Cursor<Vec<u8>>,
        started: Option<Sender<()>>,
        release: Receiver<()>,
    }

    impl Read for BlockingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if let Some(started) = self.started.take() {
                started
                    .send(())
                    .map_err(|_closed| io::Error::other("start receiver closed"))?;
                self.release
                    .recv()
                    .map_err(|_closed| io::Error::other("release sender closed"))?;
            }
            self.inner.read(buffer)
        }
    }

    #[test]
    fn same_root_concurrent_reconciliation_revalidates_and_skips_duplicate_source()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("model.bin", FIRST)?;
        let plan = fixture.plan(vec![target.clone()])?;
        let reconciler = fixture.reconciler(Arc::new(YieldWait::default()));
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let blocking_reader = BlockingReader {
            inner: Cursor::new(FIRST.to_vec()),
            started: Some(started_sender),
            release: release_receiver,
        };
        let mut first_candidates = CandidateMap::default().with_reader(
            &target,
            Box::new(blocking_reader),
            PreparedSourceProvenance::Owned,
        );
        let first_plan = plan.clone();
        let first_reconciler = reconciler.clone();
        let first = thread::spawn(move || {
            first_reconciler
                .reconcile(
                    &first_plan,
                    &mut first_candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )
                .map_err(|error| error.to_string())
        });
        started_receiver.recv()?;

        let (contended_sender, contended_receiver) = mpsc::channel();
        let second_wait = Arc::new(SignallingWait {
            signal: Mutex::new(Some(contended_sender)),
        });
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let mut second_candidates = CandidateMap {
            values: BTreeMap::new(),
            calls: Arc::clone(&second_calls),
        };
        let second_plan = plan.clone();
        let second_reconciler = fixture.reconciler(second_wait);
        let second = thread::spawn(move || {
            second_reconciler
                .reconcile(
                    &second_plan,
                    &mut second_candidates,
                    ExistingFilePolicy::Reject,
                    &super::super::local_dir_materialization::NeverCancelled,
                )
                .map_err(|error| error.to_string())
        });
        contended_receiver.recv()?;
        release_sender.send(())?;

        let first_outcome = first.join().map_err(|_panic| "first worker panicked")??;
        let second_outcome = second.join().map_err(|_panic| "second worker panicked")??;
        let first_report = report(first_outcome)?;
        let second_report = report(second_outcome)?;
        assert_eq!(
            first_report.files()[0].disposition(),
            LocalDirFileDisposition::Copied
        );
        assert_eq!(
            second_report.files()[0].disposition(),
            LocalDirFileDisposition::Reused
        );
        assert!(
            second_calls
                .lock()
                .map_err(|_poisoned| "second call log lock poisoned")?
                .is_empty()
        );
        assert_eq!(fs::read(fixture.destination(target.path()))?, FIRST);
        Ok(())
    }

    #[derive(Debug)]
    struct SecretErrorCandidates;

    impl LocalDirCandidateSet for SecretErrorCandidates {
        fn prepare_local(
            &mut self,
            _target: &LocalDirFileTarget,
            _cancellation: &dyn Cancellation,
        ) -> Result<Option<PreparedLocalDirSource>, LocalDirSourceError> {
            Err(LocalDirSourceError::io(&io::Error::other(
                "token=hf_secret signed=https://secret.example/path",
            )))
        }
    }

    #[test]
    fn debug_display_and_error_sources_redact_roots_paths_and_source_context()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let target = Fixture::target("private/token=hf_secret.bin", FIRST)?;
        let plan = fixture.plan(vec![target])?;
        let plan_rendered = format!("{plan:?}");
        assert!(!plan_rendered.contains("token=hf_secret"));
        assert!(!plan_rendered.contains(&fixture.directory.path().display().to_string()));
        assert!(!plan_rendered.contains(COMMIT));
        let mut candidates = SecretErrorCandidates;

        let error = fixture
            .reconciler(Arc::new(YieldWait::default()))
            .reconcile(
                &plan,
                &mut candidates,
                ExistingFilePolicy::Reject,
                &super::super::local_dir_materialization::NeverCancelled,
            )
            .expect_err("candidate error must fail reconciliation");
        let rendered = format!("{error:?} {error}");

        assert!(error.is_source());
        assert!(!rendered.contains("hf_secret"));
        assert!(!rendered.contains("secret.example"));
        assert!(!rendered.contains("private/"));
        assert!(!rendered.contains(&fixture.directory.path().display().to_string()));
        Ok(())
    }

    #[test]
    fn plan_rejects_selection_mismatch_and_reserved_destination_before_reconciliation()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let selected = Fixture::target("selected.bin", FIRST)?;
        let other = Fixture::target("other.bin", SECOND)?;
        let selection = RepositoryFilter::new(None, &[]).select([selected.path().clone()])?;
        let mismatch = LocalDirReconciliationPlan::new(
            fixture.layout.clone(),
            CommitId::parse(COMMIT)?,
            &selection,
            [other],
        )
        .expect_err("target set must exactly match selection");
        assert!(mismatch.is_plan_invalid());

        let reserved = Fixture::target(".cache/huggingface/download/x", FIRST)?;
        let reserved_selection =
            RepositoryFilter::new(None, &[]).select([reserved.path().clone()])?;
        let reserved_error = LocalDirReconciliationPlan::new(
            fixture.layout.clone(),
            CommitId::parse(COMMIT)?,
            &reserved_selection,
            [reserved],
        )
        .expect_err("reserved destination must fail during plan construction");
        assert!(reserved_error.is_plan_invalid());
        assert!(!fixture.directory.path().join(".cache").try_exists()?);
        Ok(())
    }
}
