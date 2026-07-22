use std::sync::Arc;

use serde::Deserialize;
use url::Url;

use crate::cache::{HubTree, HubTreeEntry, RepositoryFilter};
use crate::error::HubOperationError;
use crate::fetch_plan::FetchPlan;
use crate::transport::{RedirectFollower, Transport, TransportMethod, TransportRequest};
use crate::{AuthToken, CommitId, Endpoint, RepositoryKind, RepositorySpec, Revision};

const MAX_INFO_BODY_BYTES: usize = 1024 * 1024;
const MAX_TREE_PAGE_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_TREE_PAGES: usize = 1024;
const MAX_TREE_FILES: usize = 1_000_000;

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

    pub(crate) async fn build_plan(
        &self,
        repository: &RepositorySpec,
        revision: &Revision,
        filter: &RepositoryFilter,
        authorization: Option<&AuthToken>,
    ) -> Result<FetchPlan, HubOperationError> {
        let commit = self
            .resolve_revision(repository, revision, authorization)
            .await?;
        let tree = self
            .retrieve_tree(repository, &commit, authorization)
            .await?;
        FetchPlan::build(
            self.endpoint.clone(),
            repository.clone(),
            revision.clone(),
            commit,
            &tree,
            filter,
        )
        .map_err(HubOperationError::validation)
    }

    pub(crate) async fn retrieve_tree(
        &self,
        repository: &RepositorySpec,
        commit: &CommitId,
        authorization: Option<&AuthToken>,
    ) -> Result<HubTree, HubOperationError> {
        let mut target = repository_api_url(&self.endpoint, repository, "tree", commit.as_str())?;
        target
            .query_pairs_mut()
            .append_pair("recursive", "true")
            .append_pair("expand", "true");
        let endpoint =
            Url::parse(self.endpoint.as_str()).map_err(|_source| HubOperationError::protocol())?;
        let mut visited = std::collections::BTreeSet::new();
        let mut files = Vec::new();

        for _page in 0..MAX_TREE_PAGES {
            if !visited.insert(target.as_str().to_owned()) || !same_origin(&target, &endpoint) {
                return Err(HubOperationError::protocol());
            }
            let mut request = TransportRequest::new(TransportMethod::Get, target.clone())
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
            let next = response
                .headers()
                .get("link")
                .map(|value| parse_next_link(&target, value))
                .transpose()?
                .flatten();
            let body = collect_bounded_body(&mut response, MAX_TREE_PAGE_BODY_BYTES).await?;
            let entries: Vec<RawTreeEntry> =
                serde_json::from_slice(&body).map_err(|_source| HubOperationError::protocol())?;
            for raw in entries {
                if let Some(file) = raw.into_file()? {
                    files.push(file);
                    if files.len() > MAX_TREE_FILES {
                        return Err(HubOperationError::protocol());
                    }
                }
            }
            match next {
                Some(next) => target = next,
                None => {
                    return HubTree::new(files).map_err(|_source| tree_validation_error());
                }
            }
        }
        Err(HubOperationError::protocol())
    }
}

#[derive(Deserialize)]
struct RepositoryInfo {
    sha: Box<str>,
}

#[derive(Deserialize)]
struct RawTreeEntry {
    #[serde(rename = "type")]
    kind: Box<str>,
    path: Box<str>,
    oid: Box<str>,
    size: Option<u64>,
    lfs: Option<RawLfs>,
    #[serde(rename = "xetHash")]
    xet_hash: Option<Box<str>>,
}

#[derive(Deserialize)]
struct RawLfs {
    oid: Box<str>,
    size: u64,
}

impl RawTreeEntry {
    fn into_file(self) -> Result<Option<(crate::RepoPath, HubTreeEntry)>, HubOperationError> {
        match self.kind.as_ref() {
            "directory" => Ok(None),
            "file" => {
                let path =
                    crate::RepoPath::parse(self.path).map_err(HubOperationError::validation)?;
                let size = self.size.ok_or_else(HubOperationError::protocol)?;
                let mut entry =
                    HubTreeEntry::new(size, self.oid).map_err(|_source| tree_validation_error())?;
                if let Some(lfs) = self.lfs {
                    if lfs.size != size {
                        return Err(tree_validation_error());
                    }
                    entry = entry
                        .with_lfs(lfs.oid, lfs.size)
                        .map_err(|_source| tree_validation_error())?;
                }
                if let Some(hash) = self.xet_hash {
                    entry = entry
                        .with_xet(hash)
                        .map_err(|_source| tree_validation_error())?;
                }
                Ok(Some((path, entry)))
            }
            _ => Err(HubOperationError::protocol()),
        }
    }
}

fn tree_validation_error() -> HubOperationError {
    HubOperationError::validation(crate::ValidationError::new(
        "Hub repository tree",
        crate::validation::ValidationErrorKind::Malformed,
    ))
}

fn parse_next_link(base: &Url, value: &str) -> Result<Option<Url>, HubOperationError> {
    let mut next = None;
    for item in value.split(',') {
        let Some((target, parameters)) = item.trim().split_once('>') else {
            return Err(HubOperationError::protocol());
        };
        let Some(target) = target.strip_prefix('<') else {
            return Err(HubOperationError::protocol());
        };
        let is_next = parameters.split(';').any(|parameter| {
            parameter
                .trim()
                .strip_prefix("rel=")
                .is_some_and(|relation| relation.trim_matches('"') == "next")
        });
        if is_next {
            if next.is_some() {
                return Err(HubOperationError::protocol());
            }
            next = Some(
                base.join(target)
                    .map_err(|_source| HubOperationError::protocol())?,
            );
        }
    }
    Ok(next)
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
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

    #[test]
    fn paginated_tree_is_commit_bound_complete_and_validated() -> Result<(), Box<dyn Error>> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let first = response_with_headers(
            200,
            br#"[
                {"type":"directory","path":"weights","oid":"tree-id"},
                {"type":"file","path":"weights/model.bin","oid":"pointer-id","size":5,
                 "lfs":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":5,"pointerSize":130}}
            ]"#,
            [("link", "</api/models/owner/repo/tree/0123456789abcdef0123456789abcdef01234567?cursor=last>; rel=\"next\"")],
        )?;
        let second = response_with_headers(
            200,
            br#"[{"type":"file","path":"config.json","oid":"git-id","size":2,"xetHash":"xet-id"}]"#,
            [],
        )?;
        let protocol = protocol_with_responses([first, second], Arc::clone(&requests))?;
        let tree =
            run_ready(protocol.retrieve_tree(&repository()?, &CommitId::parse(COMMIT)?, None))?;

        assert_eq!(
            tree.files()
                .keys()
                .map(crate::RepoPath::as_str)
                .collect::<Vec<_>>(),
            ["config.json", "weights/model.bin"]
        );
        let model = tree
            .files()
            .get(&crate::RepoPath::parse("weights/model.bin")?)
            .ok_or("model entry missing")?;
        assert_eq!(model.size(), 5);
        assert_eq!(
            model.lfs_sha256(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        let requests = requests
            .lock()
            .map_err(|_poisoned| "request lock poisoned")?;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].0.ends_with(&format!(
            "/api/models/owner/repo/tree/{COMMIT}?recursive=true&expand=true"
        )));
        assert!(
            requests[1]
                .0
                .ends_with(&format!("/api/models/owner/repo/tree/{COMMIT}?cursor=last"))
        );
        Ok(())
    }

    #[test]
    fn tree_rejects_duplicate_unsafe_and_untrusted_pagination() -> Result<(), Box<dyn Error>> {
        for body in [
            br#"[{"type":"file","path":"same","oid":"a","size":1},{"type":"file","path":"same","oid":"b","size":1}]"#.as_slice(),
            br#"[{"type":"file","path":"../escape","oid":"a","size":1}]"#.as_slice(),
            br#"[{"type":"file","path":"mismatch","oid":"a","size":2,"lfs":{"oid":"sha","size":1}}]"#.as_slice(),
        ] {
            let protocol = protocol_with_response(response(200, [body])?)?;
            let error = run_ready(protocol.retrieve_tree(
                &repository()?,
                &CommitId::parse(COMMIT)?,
                None,
            ))
            .expect_err("accepted malformed tree");
            assert!(error.is_validation());
        }

        let protocol = protocol_with_response(response_with_headers(
            200,
            b"[]",
            [("link", "<https://evil.example/tree?secret=value>; rel=next")],
        )?)?;
        assert!(
            run_ready(protocol.retrieve_tree(&repository()?, &CommitId::parse(COMMIT)?, None,))
                .expect_err("followed untrusted pagination")
                .is_protocol()
        );
        Ok(())
    }

    #[test]
    fn end_to_end_planning_binds_symbolic_revision_to_one_commit() -> Result<(), Box<dyn Error>> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let protocol = protocol_with_responses(
            [
                response(200, [format!(r#"{{"sha":"{COMMIT}"}}"#).as_bytes()])?,
                response(
                    200,
                    [
                        br#"[{"type":"file","path":"config.json","oid":"git-id","size":2}]"#
                            .as_slice(),
                    ],
                )?,
            ],
            Arc::clone(&requests),
        )?;
        let plan = run_ready(protocol.build_plan(
            &repository()?,
            &Revision::parse("main")?,
            &RepositoryFilter::new(None, &[]),
            None,
        ))?;
        assert_eq!(plan.commit().as_str(), COMMIT);
        assert_eq!(
            plan.files()
                .keys()
                .map(crate::RepoPath::as_str)
                .collect::<Vec<_>>(),
            ["config.json"]
        );
        let requests = requests
            .lock()
            .map_err(|_poisoned| "request lock poisoned")?;
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .0
                .ends_with("/api/models/owner/repo/revision/main")
        );
        assert!(requests[1].0.contains(&format!("/tree/{COMMIT}?")));
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
        protocol_with_responses([response], Arc::new(Mutex::new(Vec::new())))
    }

    fn protocol_with_responses(
        responses: impl IntoIterator<Item = TransportResponse>,
        requests: Arc<Mutex<Vec<(String, bool)>>>,
    ) -> Result<HubProtocol, HubOperationError> {
        HubProtocol::new(
            Endpoint::parse("https://hub.example").map_err(HubOperationError::validation)?,
            Arc::new(ScriptedTransport {
                responses: Mutex::new(responses.into_iter().collect()),
                requests,
            }),
        )
    }

    fn response_with_headers<'a>(
        status: u16,
        body: &'a [u8],
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<TransportResponse, TransportError> {
        TransportResponse::new(
            status,
            TransportHeaders::new(headers)?,
            Box::new(MemoryBody(VecDeque::from([Box::<[u8]>::from(body)]))),
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
