use std::fmt::{self, Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "network")]
use std::sync::Mutex;

use crate::LocalDirectory;
#[cfg(feature = "network")]
use crate::cache::AcquisitionCache;
use crate::cache::{CacheView, OfflineCache, RepositoryFilter};
#[cfg(feature = "network")]
use crate::error::CacheFailure;
use crate::error::HubOperationError;
use crate::hub_protocol::HubProtocol;
#[cfg(feature = "network")]
use crate::progress::ProgressObserver;
use crate::transport::Transport;
use crate::{
    AuthToken, CancellationToken, Endpoint, FetchPlan, InspectionReport, RepoPath, RepositorySpec,
    Revision, Snapshot, VerificationReport,
};

/// A typed request to resolve and plan one repository revision.
#[derive(Clone, Debug)]
pub struct FetchRequest {
    repository: RepositorySpec,
    revision: Revision,
    allow_patterns: Option<Vec<Box<str>>>,
    ignore_patterns: Vec<Box<str>>,
    authorization: Option<AuthToken>,
}

impl FetchRequest {
    /// Creates a request for a repository and revision.
    #[must_use]
    pub const fn new(repository: RepositorySpec, revision: Revision) -> Self {
        Self {
            repository,
            revision,
            allow_patterns: None,
            ignore_patterns: Vec::new(),
            authorization: None,
        }
    }

    /// Replaces the allow-pattern list.
    ///
    /// An empty list deliberately selects no files. Omitting this method allows
    /// every path not excluded by an ignore pattern.
    #[must_use]
    pub fn allow_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<Box<str>>>,
    ) -> Self {
        self.allow_patterns = Some(patterns.into_iter().map(Into::into).collect());
        self
    }

    /// Replaces the ignore-pattern list.
    #[must_use]
    pub fn ignore_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<Box<str>>>,
    ) -> Self {
        self.ignore_patterns = patterns.into_iter().map(Into::into).collect();
        self
    }

    /// Supplies a redacted bearer token for this request only.
    #[must_use]
    pub fn authorization(mut self, token: AuthToken) -> Self {
        self.authorization = Some(token);
        self
    }

    /// Returns the requested repository.
    #[must_use]
    pub const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    /// Returns the requested revision.
    #[must_use]
    pub const fn revision(&self) -> &Revision {
        &self.revision
    }

    fn filter(&self) -> RepositoryFilter {
        let allow = self
            .allow_patterns
            .as_deref()
            .map(|patterns| patterns.iter().map(AsRef::as_ref).collect::<Vec<&str>>());
        let ignore = self
            .ignore_patterns
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>();
        RepositoryFilter::new(allow.as_deref(), &ignore)
    }
}

/// Per-operation policy for online snapshot acquisition.
#[derive(Clone)]
pub struct FetchOptions {
    cancellation: CancellationToken,
    progress: Option<Arc<dyn crate::ProgressObserver>>,
    max_attempts: u32,
}

/// Selects which cache layout owns the returned snapshot.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheMode {
    /// Use hf-store's endpoint-namespaced owned cache layout.
    Owned,
    /// Share the canonical Hugging Face cache layout with `huggingface_hub`.
    Compatible,
}

impl CacheMode {
    const fn view(self) -> CacheView {
        match self {
            Self::Owned => CacheView::Owned,
            Self::Compatible => CacheView::Compatible,
        }
    }
}

impl FetchOptions {
    /// Replaces the cooperative cancellation token.
    #[must_use]
    pub fn cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Installs a synchronous structured progress observer.
    #[must_use]
    pub fn progress(mut self, observer: Arc<dyn crate::ProgressObserver>) -> Self {
        self.progress = Some(observer);
        self
    }

    /// Sets the total number of attempts for retryable file failures.
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            progress: None,
            max_attempts: 4,
        }
    }
}

impl Debug for FetchOptions {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FetchOptions")
            .field("cancellation", &self.cancellation)
            .field("has_progress", &self.progress.is_some())
            .field("max_attempts", &self.max_attempts)
            .finish()
    }
}

/// Builder for an online Hub planning service.
#[derive(Clone, Debug)]
pub struct HubStoreBuilder {
    endpoint: Endpoint,
    cache_root: Option<PathBuf>,
    cache_mode: CacheMode,
    max_concurrent_downloads: usize,
}

impl HubStoreBuilder {
    /// Selects a validated Hub-compatible endpoint.
    #[must_use]
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Selects the explicit shared cache root used by acquisition.
    #[must_use]
    pub fn cache_root(mut self, cache_root: impl Into<PathBuf>) -> Self {
        self.cache_root = Some(cache_root.into());
        self
    }

    /// Selects the owned or Python-compatible cache view.
    #[must_use]
    pub const fn cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Sets the maximum number of file transfers polled concurrently.
    #[must_use]
    pub const fn max_concurrent_downloads(mut self, maximum: usize) -> Self {
        self.max_concurrent_downloads = maximum;
        self
    }

    /// Builds a lazy service without constructing an HTTP client.
    #[must_use]
    pub fn build(self) -> HubStore {
        HubStore {
            endpoint: self.endpoint,
            cache_root: self.cache_root,
            cache_mode: self.cache_mode,
            max_concurrent_downloads: self.max_concurrent_downloads,
            #[cfg(feature = "network")]
            transport: Mutex::new(None),
        }
    }
}

impl Default for HubStoreBuilder {
    fn default() -> Self {
        Self {
            endpoint: Endpoint::hugging_face(),
            cache_root: None,
            cache_mode: CacheMode::Compatible,
            max_concurrent_downloads: 8,
        }
    }
}

/// Lazy online service for resolving immutable Hub fetch plans.
///
/// The service never discovers credentials and does not create or enter an
/// async runtime. With the `network` feature disabled, [`HubStore::plan`]
/// returns a backend-unavailable [`crate::HubError`].
pub struct HubStore {
    endpoint: Endpoint,
    cache_root: Option<PathBuf>,
    cache_mode: CacheMode,
    max_concurrent_downloads: usize,
    #[cfg(feature = "network")]
    transport: Mutex<Option<Arc<dyn Transport>>>,
}

#[cfg(feature = "network")]
struct CompletedAcquisition {
    repository: RepositorySpec,
    requested_revision: Revision,
    plan: Arc<FetchPlan>,
    cache: Arc<AcquisitionCache>,
    acquired: crate::cache::AcquiredSnapshot,
    reused: bool,
    cancellation: CancellationToken,
}

impl HubStore {
    /// Starts a builder using the canonical Hugging Face endpoint.
    #[must_use]
    pub fn builder() -> HubStoreBuilder {
        HubStoreBuilder::default()
    }

    /// Resolves the requested revision and returns a deterministic immutable
    /// fetch plan.
    ///
    /// # Errors
    ///
    /// Returns a classified error for unavailable networking, Hub access,
    /// transport, protocol, or validation failures.
    pub async fn plan(&self, request: FetchRequest) -> Result<FetchPlan, HubOperationError> {
        let transport = self.transport()?;
        let protocol = HubProtocol::new(self.endpoint.clone(), transport)?;
        let filter = request.filter();
        protocol
            .build_plan(
                &request.repository,
                &request.revision,
                &filter,
                request.authorization.as_ref(),
            )
            .await
    }

    /// Resolves, downloads or reuses, validates, and activates one immutable snapshot.
    ///
    /// # Errors
    ///
    /// Returns a classified error when cache configuration is missing, planning
    /// or transfer fails, cancellation is observed, or activation cannot prove
    /// the complete selected snapshot.
    #[cfg(feature = "network")]
    pub async fn fetch(
        &self,
        request: FetchRequest,
        options: FetchOptions,
    ) -> Result<Snapshot, HubOperationError> {
        let completed = self.acquire(request, options).await?;
        Ok(Snapshot::from_acquired(
            self.endpoint.clone(),
            completed.repository,
            completed.requested_revision,
            &completed.acquired,
            completed.reused,
        ))
    }

    /// Acquires a snapshot and independently reconciles it into a caller-owned directory.
    ///
    /// Existing unrelated files are preserved. A differing selected regular file
    /// is rejected unless `replace_existing` is true; links, directories, and
    /// other special entries are always rejected.
    ///
    /// # Errors
    ///
    /// Returns a classified planning, transfer, cache, cancellation, or local
    /// destination conflict error.
    #[cfg(feature = "network")]
    pub async fn fetch_to_local_dir(
        &self,
        request: FetchRequest,
        options: FetchOptions,
        destination: impl AsRef<Path>,
        replace_existing: bool,
    ) -> Result<LocalDirectory, HubOperationError> {
        let completed = self.acquire(request, options).await?;
        let materialized = completed.cache.materialize_local_dir(
            completed.plan.as_ref(),
            &completed.acquired,
            destination.as_ref(),
            replace_existing,
            &completed.cancellation,
        )?;
        Ok(LocalDirectory::from_materialized(
            self.endpoint.clone(),
            completed.repository,
            completed.requested_revision,
            materialized,
        ))
    }

    #[cfg(feature = "network")]
    #[allow(
        clippy::too_many_lines,
        reason = "the acquisition orchestration keeps planning, cache reuse, bounded transfer, and activation policy visible in one boundary"
    )]
    async fn acquire(
        &self,
        request: FetchRequest,
        options: FetchOptions,
    ) -> Result<CompletedAcquisition, HubOperationError> {
        use std::collections::BTreeMap;
        use std::time::Duration;

        let cache_root = self
            .cache_root
            .as_ref()
            .ok_or_else(|| HubOperationError::cache(CacheFailure::Missing))?;
        if self.max_concurrent_downloads == 0 || options.max_attempts == 0 {
            return Err(HubOperationError::protocol());
        }
        let transport = self.transport()?;
        let protocol = Arc::new(HubProtocol::new(self.endpoint.clone(), transport)?);
        let filter = request.filter();
        let plan = Arc::new(
            protocol
                .build_plan(
                    &request.repository,
                    &request.revision,
                    &filter,
                    request.authorization.as_ref(),
                )
                .await?,
        );
        let cache = Arc::new(AcquisitionCache::shared(
            cache_root,
            &self.endpoint,
            &request.repository,
            self.cache_mode.view(),
        )?);
        match cache.open_plan(plan.as_ref()) {
            Ok(acquired) => {
                return Ok(CompletedAcquisition {
                    repository: request.repository,
                    requested_revision: request.revision,
                    plan,
                    cache,
                    acquired,
                    reused: true,
                    cancellation: options.cancellation,
                });
            }
            Err(error)
                if matches!(
                    error.cache_failure(),
                    Some(CacheFailure::Missing | CacheFailure::Incomplete)
                ) => {}
            Err(error) => return Err(error),
        }
        let retry_policy = crate::transfer::RetryPolicy::new(
            options.max_attempts,
            Duration::from_millis(200),
            Duration::from_secs(10),
        )
        .ok_or_else(HubOperationError::protocol)?;
        let progress: Arc<dyn ProgressObserver> = options
            .progress
            .unwrap_or_else(|| Arc::new(crate::progress::NoopProgress));
        let jobs = plan
            .files()
            .iter()
            .cloned()
            .map(|file| {
                let cache = Arc::clone(&cache);
                let protocol = Arc::clone(&protocol);
                let plan = Arc::clone(&plan);
                let authorization = request.authorization.clone();
                let cancellation = options.cancellation.clone();
                let progress = Arc::clone(&progress);
                Box::pin(async move {
                    let digest = cache
                        .download_file(
                            protocol,
                            plan.as_ref(),
                            &file,
                            authorization,
                            retry_policy,
                            cancellation,
                            progress,
                        )
                        .await?;
                    Ok((file.path().clone(), digest))
                }) as crate::transfer::ScheduledFuture<_>
            })
            .collect::<Vec<_>>();
        let downloaded = crate::transfer::run_bounded(
            self.max_concurrent_downloads,
            jobs,
            &options.cancellation,
        )
        .await?;
        let digests = downloaded.into_iter().collect::<BTreeMap<_, _>>();
        let acquired = cache.activate(plan.as_ref(), &digests)?;
        Ok(CompletedAcquisition {
            repository: request.repository,
            requested_revision: request.revision,
            plan,
            cache,
            acquired,
            reused: false,
            cancellation: options.cancellation,
        })
    }

    /// Reports an unavailable network backend in cache-only builds.
    ///
    /// # Errors
    ///
    /// Always returns a backend-unavailable classified error.
    #[cfg(not(feature = "network"))]
    #[allow(
        clippy::unused_async,
        reason = "the feature-independent public signature remains awaitable to downstream callers"
    )]
    pub async fn fetch(
        &self,
        _request: FetchRequest,
        _options: FetchOptions,
    ) -> Result<Snapshot, HubOperationError> {
        Err(HubOperationError::transport(
            crate::transport::TransportError::unavailable(),
        ))
    }

    #[cfg(feature = "network")]
    fn transport(&self) -> Result<Arc<dyn Transport>, HubOperationError> {
        let mut slot = self.transport.lock().map_err(|_poisoned| {
            HubOperationError::transport(crate::transport::TransportError::connection())
        })?;
        if let Some(transport) = slot.as_ref() {
            return Ok(Arc::clone(transport));
        }
        let transport: Arc<dyn Transport> = Arc::new(
            crate::reqwest_transport::ReqwestTransport::build()
                .map_err(HubOperationError::transport)?,
        );
        *slot = Some(Arc::clone(&transport));
        Ok(transport)
    }

    #[cfg(not(feature = "network"))]
    #[allow(
        clippy::unused_self,
        reason = "the same service method reports an unavailable backend in cache-only builds"
    )]
    fn transport(&self) -> Result<Arc<dyn Transport>, HubOperationError> {
        Err(HubOperationError::transport(
            crate::transport::TransportError::unavailable(),
        ))
    }
}

impl Debug for HubStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HubStore")
            .field("endpoint", &self.endpoint)
            .field("has_cache_root", &self.cache_root.is_some())
            .field("cache_mode", &self.cache_mode)
            .field("max_concurrent_downloads", &self.max_concurrent_downloads)
            .finish_non_exhaustive()
    }
}

/// A transport-free service for exact-selection cache lookup.
#[derive(Clone, Debug)]
pub struct OfflineStore {
    cache_root: PathBuf,
    endpoint: Endpoint,
    cache_mode: CacheMode,
}

impl OfflineStore {
    /// Creates an offline service from an explicit shared cache root.
    #[must_use]
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
            endpoint: Endpoint::hugging_face(),
            cache_mode: CacheMode::Compatible,
        }
    }

    /// Replaces the validated endpoint identity used to namespace cache state.
    #[must_use]
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Selects the owned or Python-compatible cache view.
    #[must_use]
    pub const fn cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Opens and revalidates an exact selected path set without any transport capability.
    ///
    /// # Errors
    ///
    /// Returns a classified cache error for missing, incomplete, corrupt,
    /// unsupported, or unsafe local state.
    pub fn open(
        &self,
        repository: &RepositorySpec,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> Result<Snapshot, HubOperationError> {
        let cache = OfflineCache::shared(
            &self.cache_root,
            &self.endpoint,
            repository,
            self.cache_mode.view(),
        )?;
        let acquired = cache.open(revision, paths)?;
        Ok(Snapshot::from_acquired(
            self.endpoint.clone(),
            repository.clone(),
            revision.clone(),
            &acquired,
            true,
        ))
    }

    /// Inspects one exact selection without networking or cache mutation.
    #[must_use]
    pub fn inspect(
        &self,
        repository: &RepositorySpec,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> InspectionReport {
        match self.open(repository, revision, paths) {
            Ok(snapshot) => InspectionReport::complete(self.cache_mode, &snapshot),
            Err(error) => InspectionReport::failed(self.cache_mode, &error),
        }
    }

    /// Revalidates one exact selection and returns stable report evidence.
    #[must_use]
    pub fn verify(
        &self,
        repository: &RepositorySpec,
        revision: &Revision,
        paths: &[RepoPath],
    ) -> VerificationReport {
        VerificationReport::from_inspection(self.inspect(repository, revision, paths))
    }

    /// Opens and fully revalidates a completed caller-owned local directory.
    ///
    /// The exact immutable commit and selected path set must match the
    /// completion record written by [`HubStore::fetch_to_local_dir`]. No cache
    /// root or transport capability is consulted.
    ///
    /// # Errors
    ///
    /// Returns a classified cache error when completion metadata is absent or
    /// stale, or when any selected file no longer matches its recorded size and
    /// digest.
    pub fn open_local_dir(
        &self,
        destination: impl AsRef<Path>,
        repository: &RepositorySpec,
        commit: &crate::CommitId,
        paths: &[RepoPath],
    ) -> Result<LocalDirectory, HubOperationError> {
        let materialized = OfflineCache::open_local_dir(
            destination.as_ref(),
            &self.endpoint,
            repository,
            commit,
            paths,
        )?;
        let requested_revision =
            Revision::parse(commit.as_str()).map_err(HubOperationError::validation)?;
        Ok(LocalDirectory::from_materialized(
            self.endpoint.clone(),
            repository.clone(),
            requested_revision,
            materialized,
        ))
    }

    /// Returns the configured cache root.
    #[must_use]
    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::RepositoryId;

    use super::*;

    #[test]
    fn request_debug_redacts_authorization_and_public_values_are_send() -> Result<(), Box<dyn Error>>
    {
        fn assert_send<T: Send>() {}
        assert_send::<FetchRequest>();
        assert_send::<HubStore>();
        assert_send::<FetchPlan>();

        let secret = "hf_secret_public_request";
        let request = FetchRequest::new(
            RepositorySpec::model(RepositoryId::parse("owner/repo")?),
            Revision::parse("main")?,
        )
        .authorization(AuthToken::new(secret)?);
        assert!(!format!("{request:?}").contains(secret));
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn public_service_plans_against_the_hermetic_hub_fixture() -> Result<(), Box<dyn Error>> {
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
        let fixture = ScriptedHub::start([
            Exchange::new(
                ExpectedRequest::get("/api/models/owner/repo/revision/main"),
                ScriptedResponse::new(200, format!(r#"{{"sha":"{COMMIT}"}}"#).into_bytes()),
            ),
            Exchange::new(
                ExpectedRequest::get(&format!(
                    "/api/models/owner/repo/tree/{COMMIT}?recursive=true&expand=true"
                )),
                ScriptedResponse::new(
                    200,
                    br#"[{"type":"file","path":"config.json","oid":"git-id","size":2}]"#.as_slice(),
                ),
            ),
        ])?;
        let store = HubStore::builder()
            .endpoint(Endpoint::parse(fixture.endpoint())?)
            .build();
        let request = FetchRequest::new(
            RepositorySpec::model(RepositoryId::parse("owner/repo")?),
            Revision::parse("main")?,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(store.plan(request));
        let observed = fixture.finish();
        let plan = result.map_err(|error| format!("{error}; fixture: {observed:?}"))?;
        assert_eq!(plan.commit().as_str(), COMMIT);
        assert_eq!(plan.files()[0].path().as_str(), "config.json");
        assert_eq!(observed?.len(), 2);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one end-to-end fixture proves online acquisition, independent local materialization, and both offline reopen paths"
    )]
    fn public_fetch_downloads_then_reuses_a_complete_snapshot_without_file_body_bytes()
    -> Result<(), Box<dyn Error>> {
        use crate::cache::BlobDigest;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
        let bytes = b"model bytes";
        let digest = BlobDigest::for_bytes(bytes).to_string();
        let info = format!(r#"{{"sha":"{COMMIT}"}}"#);
        let tree = format!(
            r#"[{{"type":"file","path":"model.bin","oid":"pointer","size":{},"lfs":{{"oid":"{digest}","size":{}}}}}]"#,
            bytes.len(),
            bytes.len()
        );
        let fixture = ScriptedHub::start([
            Exchange::new(
                ExpectedRequest::get("/api/models/owner/repo/revision/main"),
                ScriptedResponse::new(200, info.clone().into_bytes()),
            ),
            Exchange::new(
                ExpectedRequest::get(&format!(
                    "/api/models/owner/repo/tree/{COMMIT}?recursive=true&expand=true"
                )),
                ScriptedResponse::new(200, tree.clone().into_bytes()),
            ),
            Exchange::new(
                ExpectedRequest::get(&format!("/owner/repo/resolve/{COMMIT}/model.bin")),
                ScriptedResponse::new(200, bytes.as_slice()).header("etag", "stable-etag"),
            ),
            Exchange::new(
                ExpectedRequest::get("/api/models/owner/repo/revision/main"),
                ScriptedResponse::new(200, info.into_bytes()),
            ),
            Exchange::new(
                ExpectedRequest::get(&format!(
                    "/api/models/owner/repo/tree/{COMMIT}?recursive=true&expand=true"
                )),
                ScriptedResponse::new(200, tree.into_bytes()),
            ),
        ])?;
        let directory = tempfile::TempDir::new()?;
        let store = HubStore::builder()
            .endpoint(Endpoint::parse(fixture.endpoint())?)
            .cache_root(directory.path())
            .cache_mode(CacheMode::Owned)
            .max_concurrent_downloads(2)
            .build();
        let request = || {
            Ok::<_, crate::ValidationError>(FetchRequest::new(
                RepositorySpec::model(RepositoryId::parse("owner/repo")?),
                Revision::parse("main")?,
            ))
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let first = runtime.block_on(store.fetch(request()?, FetchOptions::default()))?;
        assert!(!first.was_reused());
        assert_eq!(first.cache_mode(), CacheMode::Owned);
        let path = RepoPath::parse("model.bin")?;
        let file = first.file(&path).ok_or("downloaded file missing")?;
        assert!(file.local_path().starts_with(first.directory()));
        assert_eq!(file.form(), crate::SnapshotFileForm::Owned);
        assert_eq!(std::fs::read(file.local_path())?, bytes);
        assert_eq!(file.sha256(), digest);

        let local_root = directory.path().join("local-dir");
        let local = runtime.block_on(store.fetch_to_local_dir(
            request()?,
            FetchOptions::default(),
            &local_root,
            false,
        ))?;
        assert_eq!(local.commit().as_str(), COMMIT);
        assert_eq!(local.root(), local_root);
        assert_eq!(std::fs::read(local.files()[0].local_path())?, bytes);
        assert_ne!(local.files()[0].local_path(), file.local_path());
        let offline = OfflineStore::new(directory.path())
            .endpoint(first.endpoint().clone())
            .cache_mode(CacheMode::Owned);
        let reopened_local = offline.open_local_dir(
            &local_root,
            first.repository(),
            first.commit(),
            std::slice::from_ref(&path),
        )?;
        assert_eq!(reopened_local.selection_id(), local.selection_id());
        assert_eq!(
            std::fs::read(reopened_local.files()[0].local_path())?,
            bytes
        );
        let offline_snapshot = offline.open(
            first.repository(),
            &Revision::parse("main")?,
            std::slice::from_ref(&path),
        )?;
        assert!(offline_snapshot.was_reused());
        assert_eq!(offline_snapshot.commit(), first.commit());
        let inspection = offline.inspect(
            first.repository(),
            &Revision::parse("main")?,
            std::slice::from_ref(&path),
        );
        assert_eq!(inspection.state(), crate::InspectionState::Complete);
        assert!(
            offline
                .verify(
                    first.repository(),
                    &Revision::parse("main")?,
                    std::slice::from_ref(&path),
                )
                .is_valid()
        );
        let encoded = serde_json::to_string(&inspection)?;
        assert!(encoded.contains("\"schema\":\"hf-store.inspection\""));
        assert_eq!(
            std::fs::read(
                offline_snapshot
                    .file(&path)
                    .ok_or("offline file missing")?
                    .local_path()
            )?,
            bytes
        );
        std::fs::write(local.files()[0].local_path(), b"changed")?;
        let stale = offline
            .open_local_dir(
                &local_root,
                first.repository(),
                first.commit(),
                std::slice::from_ref(&path),
            )
            .expect_err("modified local directory unexpectedly remained complete");
        assert!(stale.is_cache());
        assert_eq!(fixture.finish()?.len(), 5);
        Ok(())
    }

    #[cfg(not(feature = "network"))]
    #[test]
    fn cache_only_build_classifies_network_as_unavailable_without_a_runtime()
    -> Result<(), Box<dyn Error>> {
        let store = HubStore::builder().build();
        let request = FetchRequest::new(
            RepositorySpec::model(RepositoryId::parse("owner/repo")?),
            Revision::parse("main")?,
        );
        let error = run_ready(store.plan(request)).expect_err("network unexpectedly available");
        assert!(error.is_backend_unavailable());
        Ok(())
    }

    #[cfg(not(feature = "network"))]
    fn run_ready<F: Future>(future: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut future = std::pin::pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future unexpectedly remained pending"),
        }
    }
}
