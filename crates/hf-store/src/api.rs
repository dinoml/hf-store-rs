use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
#[cfg(feature = "network")]
use std::sync::Mutex;

use crate::cache::RepositoryFilter;
use crate::error::HubOperationError;
use crate::hub_protocol::HubProtocol;
use crate::transport::Transport;
use crate::{AuthToken, Endpoint, FetchPlan, RepositorySpec, Revision};

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
}

/// Builder for an online Hub planning service.
#[derive(Clone, Debug)]
pub struct HubStoreBuilder {
    endpoint: Endpoint,
}

impl HubStoreBuilder {
    /// Selects a validated Hub-compatible endpoint.
    #[must_use]
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Builds a lazy service without constructing an HTTP client.
    #[must_use]
    pub fn build(self) -> HubStore {
        HubStore {
            endpoint: self.endpoint,
            #[cfg(feature = "network")]
            transport: Mutex::new(None),
        }
    }
}

impl Default for HubStoreBuilder {
    fn default() -> Self {
        Self {
            endpoint: Endpoint::hugging_face(),
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
    #[cfg(feature = "network")]
    transport: Mutex<Option<Arc<dyn Transport>>>,
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
        let allow = request
            .allow_patterns
            .as_deref()
            .map(|patterns| patterns.iter().map(AsRef::as_ref).collect::<Vec<&str>>());
        let ignore = request
            .ignore_patterns
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>();
        let filter = RepositoryFilter::new(allow.as_deref(), &ignore);
        protocol
            .build_plan(
                &request.repository,
                &request.revision,
                &filter,
                request.authorization.as_ref(),
            )
            .await
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
            .finish_non_exhaustive()
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
