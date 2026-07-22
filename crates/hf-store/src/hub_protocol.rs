use std::sync::Arc;

use serde::Deserialize;
use url::Url;

use crate::error::HubOperationError;
use crate::transport::{RedirectFollower, Transport, TransportMethod, TransportRequest};
use crate::{AuthToken, CommitId, Endpoint, RepositoryKind, RepositorySpec, Revision};

const MAX_INFO_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub(crate) struct HubProtocol {
    endpoint: Endpoint,
    redirects: RedirectFollower,
}

impl HubProtocol {
    pub(crate) fn new(
        endpoint: Endpoint,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, HubOperationError> {
        let redirects =
            RedirectFollower::new(&endpoint, transport).map_err(HubOperationError::transport)?;
        Ok(Self {
            endpoint,
            redirects,
        })
    }

    pub(crate) async fn resolve_revision(
        &self,
        repository: &RepositorySpec,
        revision: &Revision,
        authorization: Option<&AuthToken>,
    ) -> Result<CommitId, HubOperationError> {
        if let Ok(commit) = CommitId::parse(revision.as_str()) {
            return Ok(commit);
        }

        let target = repository_api_url(&self.endpoint, repository, "revision", revision.as_str())?;
        let mut request = TransportRequest::new(TransportMethod::Get, target)
            .map_err(HubOperationError::transport)?;
        if let Some(token) = authorization {
            request = request.with_authorization(token.clone());
        }
        let mut response = self
            .redirects
            .send(request)
            .await
            .map_err(HubOperationError::transport)?;
        if let Some(error) = HubOperationError::from_status(response.status(), None) {
            return Err(error);
        }

        let body = collect_bounded_body(&mut response, MAX_INFO_BODY_BYTES).await?;
        let info: RepositoryInfo =
            serde_json::from_slice(&body).map_err(|_source| HubOperationError::protocol())?;
        CommitId::parse(info.sha).map_err(HubOperationError::validation)
    }
}

#[derive(Deserialize)]
struct RepositoryInfo {
    sha: Box<str>,
}

async fn collect_bounded_body(
    response: &mut crate::transport::TransportResponse,
    limit: usize,
) -> Result<Vec<u8>, HubOperationError> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .body_mut()
        .next_chunk()
        .await
        .map_err(HubOperationError::transport)?
    {
        let remaining = limit
            .checked_sub(body.len())
            .ok_or_else(HubOperationError::protocol)?;
        if chunk.len() > remaining {
            return Err(HubOperationError::protocol());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn repository_api_url(
    endpoint: &Endpoint,
    repository: &RepositorySpec,
    operation: &str,
    argument: &str,
) -> Result<Url, HubOperationError> {
    let mut target =
        Url::parse(endpoint.as_str()).map_err(|_source| HubOperationError::protocol())?;
    let plural = match repository.kind() {
        RepositoryKind::Model => "models",
        RepositoryKind::Dataset => "datasets",
        RepositoryKind::Space => "spaces",
    };
    {
        let mut segments = target
            .path_segments_mut()
            .map_err(|()| HubOperationError::protocol())?;
        segments.pop_if_empty();
        segments.push("api").push(plural);
        for component in repository.id().as_str().split('/') {
            segments.push(component);
        }
        segments.push(operation).push(argument)
    };
    Ok(target)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use crate::RepositoryId;
    use crate::transport::{
        TransportBody, TransportError, TransportFuture, TransportHeaders, TransportResponse,
    };

    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[derive(Debug)]
    struct MemoryBody(VecDeque<Box<[u8]>>);

    impl TransportBody for MemoryBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            Box::pin(std::future::ready(Ok(self.0.pop_front())))
        }
    }

    #[derive(Debug)]
    struct ScriptedTransport {
        responses: Mutex<VecDeque<TransportResponse>>,
        requests: Arc<Mutex<Vec<(String, bool)>>>,
    }

    impl Transport for ScriptedTransport {
        fn send(
            &self,
            request: TransportRequest,
        ) -> TransportFuture<'_, Result<TransportResponse, TransportError>> {
            let result = self
                .requests
                .lock()
                .map_err(|_poisoned| TransportError::connection())
                .and_then(|mut requests| {
                    requests.push((
                        request.target().as_str().to_owned(),
                        request.authorization().is_some(),
                    ));
                    self.responses
                        .lock()
                        .map_err(|_poisoned| TransportError::connection())?
                        .pop_front()
                        .ok_or_else(TransportError::connection)
                });
            Box::pin(std::future::ready(result))
        }
    }

    #[test]
    fn branches_tags_and_pull_requests_resolve_for_every_repository_kind()
    -> Result<(), Box<dyn Error>> {
        for (kind, plural) in [
            (RepositoryKind::Model, "models"),
            (RepositoryKind::Dataset, "datasets"),
            (RepositoryKind::Space, "spaces"),
        ] {
            for (revision, encoded) in [
                ("feature/runtime", "feature%2Fruntime"),
                ("v0.1.0", "v0.1.0"),
                ("refs/pr/17", "refs%2Fpr%2F17"),
            ] {
                let requests = Arc::new(Mutex::new(Vec::new()));
                let protocol =
                    protocol(&format!(r#"{{"sha":"{COMMIT}"}}"#), Arc::clone(&requests))?;
                let repository = RepositorySpec::new(kind, RepositoryId::parse("owner/repo")?);
                let token = AuthToken::new("hf_secret_revision_token")?;
                let commit = run_ready(protocol.resolve_revision(
                    &repository,
                    &Revision::parse(revision)?,
                    Some(&token),
                ))?;
                assert_eq!(commit.as_str(), COMMIT);
                let requests = requests
                    .lock()
                    .map_err(|_poisoned| "request lock poisoned")?;
                assert_eq!(requests.len(), 1);
                assert_eq!(
                    requests[0].0,
                    format!("https://hub.example/base/api/{plural}/owner/repo/revision/{encoded}")
                );
                assert!(requests[0].1);
            }
        }
        Ok(())
    }

    #[test]
    fn full_commits_need_no_transport_request() -> Result<(), Box<dyn Error>> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let protocol = protocol("not used", Arc::clone(&requests))?;
        let repository = RepositorySpec::model(RepositoryId::parse("owner/repo")?);
        let commit =
            run_ready(protocol.resolve_revision(&repository, &Revision::parse(COMMIT)?, None))?;
        assert_eq!(commit.as_str(), COMMIT);
        assert!(
            requests
                .lock()
                .map_err(|_poisoned| "request lock poisoned")?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn resolution_classifies_status_malformed_sha_and_oversized_bodies()
    -> Result<(), Box<dyn Error>> {
        let missing = protocol_with_response(response(404, [b"{}".as_slice()])?)?;
        assert!(
            run_ready(missing.resolve_revision(&repository()?, &Revision::parse("main")?, None))
                .expect_err("accepted missing revision")
                .is_missing()
        );

        let malformed =
            protocol_with_response(response(200, [br#"{"sha":"not-a-commit"}"#.as_slice()])?)?;
        assert!(
            run_ready(malformed.resolve_revision(&repository()?, &Revision::parse("main")?, None))
                .expect_err("accepted malformed commit")
                .is_validation()
        );

        let oversized = protocol_with_response(response(
            200,
            [
                vec![b'x'; MAX_INFO_BODY_BYTES].into_boxed_slice().as_ref(),
                b"x".as_slice(),
            ],
        )?)?;
        assert!(
            run_ready(oversized.resolve_revision(&repository()?, &Revision::parse("main")?, None))
                .expect_err("accepted oversized body")
                .is_protocol()
        );
        Ok(())
    }

    fn repository() -> Result<RepositorySpec, crate::ValidationError> {
        Ok(RepositorySpec::model(RepositoryId::parse("owner/repo")?))
    }

    fn protocol(
        body: &str,
        requests: Arc<Mutex<Vec<(String, bool)>>>,
    ) -> Result<HubProtocol, HubOperationError> {
        HubProtocol::new(
            Endpoint::parse("https://hub.example/base").map_err(HubOperationError::validation)?,
            Arc::new(ScriptedTransport {
                responses: Mutex::new(VecDeque::from([
                    response(200, [body.as_bytes()]).map_err(HubOperationError::transport)?
                ])),
                requests,
            }),
        )
    }

    fn protocol_with_response(
        response: TransportResponse,
    ) -> Result<HubProtocol, HubOperationError> {
        HubProtocol::new(
            Endpoint::parse("https://hub.example").map_err(HubOperationError::validation)?,
            Arc::new(ScriptedTransport {
                responses: Mutex::new(VecDeque::from([response])),
                requests: Arc::new(Mutex::new(Vec::new())),
            }),
        )
    }

    fn response<'a>(
        status: u16,
        chunks: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<TransportResponse, TransportError> {
        TransportResponse::new(
            status,
            TransportHeaders::default(),
            Box::new(MemoryBody(
                chunks.into_iter().map(Box::<[u8]>::from).collect(),
            )),
        )
    }

    fn run_ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("test future unexpectedly remained pending"),
        }
    }
}
