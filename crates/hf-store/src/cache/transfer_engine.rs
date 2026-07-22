use crate::error::{CacheFailure, HubOperationError};
use crate::progress::ProgressObserver;
use crate::transfer::{BodyDisposition, append_bounded_body, validate_file_response};
#[cfg(feature = "network")]
use crate::transfer::{RetryClock, RetryPolicy, run_with_retry};
use crate::transport::TransportBody;
use crate::validation::{ValidationError, ValidationErrorKind};
#[cfg(feature = "network")]
use crate::{AuthToken, RepositorySpec};
use crate::{CancellationToken, CommitId, RepoPath};

use super::hub_cache::copy_and_validate_content;
use super::hub_metadata::HubTreeEntry;
use super::key::BlobDigest;
use super::publication::CacheKernel;

impl CacheKernel {
    #[cfg(feature = "network")]
    #[allow(
        clippy::too_many_arguments,
        reason = "the acquisition boundary keeps all request and operation policy explicit"
    )]
    pub(super) async fn download_file(
        self: &std::sync::Arc<Self>,
        protocol: std::sync::Arc<crate::hub_protocol::HubProtocol>,
        repository: RepositorySpec,
        commit: CommitId,
        path: RepoPath,
        entry: HubTreeEntry,
        authorization: Option<AuthToken>,
        retry_policy: RetryPolicy,
        retry_clock: &dyn RetryClock,
        cancellation: CancellationToken,
        progress: std::sync::Arc<dyn ProgressObserver>,
    ) -> Result<BlobDigest, HubOperationError> {
        let cache = std::sync::Arc::clone(self);
        run_with_retry(retry_policy, retry_clock, move |_attempt| {
            let cache = std::sync::Arc::clone(&cache);
            let protocol = std::sync::Arc::clone(&protocol);
            let repository = repository.clone();
            let commit = commit.clone();
            let path = path.clone();
            let entry = entry.clone();
            let authorization = authorization.clone();
            let cancellation = cancellation.clone();
            let progress = std::sync::Arc::clone(&progress);
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err(HubOperationError::cancelled());
                }
                let target_digest = entry
                    .lfs_sha256()
                    .and_then(|value| BlobDigest::parse(value).ok());
                let resume = cache
                    .partial_resume_candidate(&commit, &path, entry.size(), target_digest.as_ref())
                    .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
                let request_resume = resume
                    .as_ref()
                    .map(|(offset, validator)| (*offset, validator.as_deref()));
                let mut response = protocol
                    .request_file(
                        &repository,
                        &commit,
                        &path,
                        request_resume,
                        authorization.as_ref(),
                    )
                    .await?;
                cache
                    .consume_file_response(
                        &mut response,
                        &commit,
                        &path,
                        &entry,
                        request_resume,
                        &cancellation,
                        progress.as_ref(),
                    )
                    .await
            })
        })
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the response boundary keeps remote identity, cancellation, and progress explicit"
    )]
    pub(super) async fn consume_file_response(
        &self,
        response: &mut crate::transport::TransportResponse,
        commit: &CommitId,
        path: &RepoPath,
        entry: &HubTreeEntry,
        requested_resume: Option<(u64, Option<&str>)>,
        cancellation: &CancellationToken,
        progress: &dyn ProgressObserver,
    ) -> Result<BlobDigest, HubOperationError> {
        let disposition = validate_file_response(
            response.status(),
            response.headers().get("content-length"),
            response.headers().get("content-range"),
            requested_resume.map(|(offset, _validator)| offset),
            entry.size(),
        )?;
        let response_validator = response
            .headers()
            .get("etag")
            .or_else(|| response.headers().get("last-modified"))
            .map(str::to_owned);
        match disposition {
            BodyDisposition::Fresh { .. } | BodyDisposition::Restart { .. } => {
                self.stream_fresh_file_to_blob(
                    response.body_mut(),
                    commit,
                    path,
                    entry,
                    response_validator.as_deref(),
                    cancellation,
                    progress,
                )
                .await
            }
            BodyDisposition::Resume { offset, .. } => {
                let requested_validator = requested_resume
                    .and_then(|(_offset, validator)| validator)
                    .map(str::to_owned);
                let has_target_digest = entry.lfs_sha256().is_some();
                if !has_target_digest
                    && (requested_validator.is_none()
                        || requested_validator.as_deref() != response_validator.as_deref())
                {
                    self.discard_partial_coordinated(commit, path)?;
                    return Err(HubOperationError::validation(ValidationError::new(
                        "resumed Hub file validator",
                        ValidationErrorKind::Malformed,
                    )));
                }
                self.resume_file_to_blob(
                    response.body_mut(),
                    commit,
                    path,
                    entry,
                    offset,
                    requested_validator.as_deref(),
                    cancellation,
                    progress,
                )
                .await
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the transfer boundary keeps remote identity, cancellation, and progress explicit"
    )]
    pub(super) async fn stream_fresh_file_to_blob(
        &self,
        body: &mut dyn TransportBody,
        commit: &CommitId,
        path: &RepoPath,
        entry: &HubTreeEntry,
        validator: Option<&str>,
        cancellation: &CancellationToken,
        progress: &dyn ProgressObserver,
    ) -> Result<BlobDigest, HubOperationError> {
        let _partial_guard = self
            .lock_partial(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let target_digest = entry
            .lfs_sha256()
            .and_then(|value| BlobDigest::parse(value).ok());
        if let Some(digest) = target_digest {
            let existing = self
                .open_blob(&digest, entry.size())
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
            if existing.is_some() {
                return Ok(digest);
            }
        }
        let mut sink = self
            .create_fresh_partial_sink(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let streamed = crate::transfer::stream_validated_body(
            body,
            &mut sink,
            path,
            entry,
            cancellation,
            progress,
        )
        .await;
        drop(sink);
        let digest = match streamed {
            Ok(digest) => digest,
            Err(error) => {
                if error.is_cancelled() || error.is_retryable() {
                    self.retain_resumable_partial(
                        commit,
                        path,
                        entry.size(),
                        validator,
                        target_digest.as_ref(),
                    );
                } else {
                    let _cleanup = self.discard_partial(commit, path);
                }
                return Err(error);
            }
        };
        let publication = self
            .publish_validated_partial(commit, path, entry.size(), digest)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io));
        if publication.is_ok() {
            self.discard_partial(commit, path)
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        }
        publication.map(|_publication| digest)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the resume boundary keeps remote identity, cancellation, and progress explicit"
    )]
    pub(super) async fn resume_file_to_blob(
        &self,
        body: &mut dyn TransportBody,
        commit: &CommitId,
        path: &RepoPath,
        entry: &HubTreeEntry,
        offset: u64,
        validator: Option<&str>,
        cancellation: &CancellationToken,
        progress: &dyn ProgressObserver,
    ) -> Result<BlobDigest, HubOperationError> {
        let _partial_guard = self
            .lock_partial(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let target_digest = entry
            .lfs_sha256()
            .and_then(|value| BlobDigest::parse(value).ok());
        if let Some(digest) = target_digest {
            if self
                .open_blob(&digest, entry.size())
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?
                .is_some()
            {
                return Ok(digest);
            }
        }
        let actual_size = self
            .partial_data_size(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?
            .ok_or_else(HubOperationError::protocol)?;
        let eligible = self
            .partial_resume_offset(
                commit,
                path,
                entry.size(),
                actual_size,
                validator,
                target_digest.as_ref(),
            )
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        if eligible != Some(offset) {
            self.discard_partial(commit, path)
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
            return Err(HubOperationError::protocol());
        }

        let mut sink = self
            .create_resume_partial_sink(commit, path, offset)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let appended = append_bounded_body(
            body,
            &mut sink,
            path,
            offset,
            entry.size(),
            cancellation,
            progress,
        )
        .await;
        drop(sink);
        if let Err(error) = appended {
            if error.is_cancelled() || error.is_retryable() {
                self.retain_resumable_partial(
                    commit,
                    path,
                    entry.size(),
                    validator,
                    target_digest.as_ref(),
                );
            } else {
                let _cleanup = self.discard_partial(commit, path);
            }
            return Err(error);
        }

        let mut reader = self
            .open_partial_reader(commit, path, entry.size())
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let digest = copy_and_validate_content(reader.as_mut(), &mut std::io::sink(), entry)
            .map_err(|_source| {
                HubOperationError::validation(ValidationError::new(
                    "resumed Hub file content",
                    ValidationErrorKind::Malformed,
                ))
            });
        drop(reader);
        let digest = match digest {
            Ok((_size, digest)) => digest,
            Err(error) => {
                let _cleanup = self.discard_partial(commit, path);
                return Err(error);
            }
        };
        let publication = self
            .publish_validated_partial(commit, path, entry.size(), digest)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io));
        if publication.is_ok() {
            self.discard_partial(commit, path)
                .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        }
        publication.map(|_publication| digest)
    }

    fn retain_resumable_partial(
        &self,
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        validator: Option<&str>,
        target_digest: Option<&BlobDigest>,
    ) {
        let Ok(Some(received)) = self.partial_data_size(commit, path) else {
            let _cleanup = self.discard_partial(commit, path);
            return;
        };
        if received > 0
            && received < expected_size
            && (validator.is_some() || target_digest.is_some())
            && self
                .persist_partial_record(
                    commit,
                    path,
                    expected_size,
                    received,
                    validator.map(str::to_owned),
                    target_digest.copied(),
                )
                .is_ok()
        {
            return;
        }
        let _cleanup = self.discard_partial(commit, path);
    }

    fn discard_partial_coordinated(
        &self,
        commit: &CommitId,
        path: &RepoPath,
    ) -> Result<(), HubOperationError> {
        let _partial_guard = self
            .lock_partial(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        self.discard_partial(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use tempfile::TempDir;

    #[cfg(not(feature = "network"))]
    use crate::RepositorySpec;
    use crate::transport::{TransportError, TransportFuture};
    use crate::{Endpoint, RepositoryId};

    use super::super::publication::{
        Effects, NoPublicationFaults, OsFileSystem, RandomOperationIds, SystemClock,
    };
    use super::*;

    #[derive(Debug)]
    struct MemoryBody(VecDeque<Box<[u8]>>);

    impl TransportBody for MemoryBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            Box::pin(std::future::ready(Ok(self.0.pop_front())))
        }
    }

    #[derive(Debug)]
    struct CancellingBody {
        chunk: Option<Box<[u8]>>,
        cancellation: CancellationToken,
    }

    #[derive(Debug)]
    struct CountingBody {
        chunk: Option<Box<[u8]>>,
        reads: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct DisconnectingBody {
        chunk: Option<Box<[u8]>>,
    }

    impl TransportBody for CountingBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            let chunk = self.chunk.take();
            if chunk.is_some() {
                self.reads.fetch_add(1, Ordering::AcqRel);
            }
            Box::pin(std::future::ready(Ok(chunk)))
        }
    }

    impl TransportBody for CancellingBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            let chunk = self.chunk.take();
            if chunk.is_some() {
                self.cancellation.cancel();
            }
            Box::pin(std::future::ready(Ok(chunk)))
        }
    }

    impl TransportBody for DisconnectingBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            if let Some(chunk) = self.chunk.take() {
                Box::pin(std::future::ready(Ok(Some(chunk))))
            } else {
                Box::pin(std::future::ready(Err(TransportError::body())))
            }
        }
    }

    #[cfg(feature = "network")]
    #[derive(Debug)]
    struct NoDelayClock;

    #[cfg(feature = "network")]
    impl crate::transfer::RetryClock for NoDelayClock {
        fn sleep(
            &self,
            _duration: std::time::Duration,
        ) -> crate::transfer::RetryFuture<'_, Result<(), HubOperationError>> {
            Box::pin(std::future::ready(Ok(())))
        }
    }

    #[test]
    fn cache_streaming_publishes_only_a_complete_validated_blob() -> Result<(), Box<dyn Error>> {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let bytes = b"validated body";
        let entry = HubTreeEntry::new(bytes.len() as u64, "opaque-validator")?;
        let mut body = MemoryBody(VecDeque::from([
            Box::<[u8]>::from(&bytes[..4]),
            Box::<[u8]>::from(&bytes[4..]),
        ]));
        let digest = run_ready(kernel.stream_fresh_file_to_blob(
            &mut body,
            &commit,
            &path,
            &entry,
            None,
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))?;
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        Ok(())
    }

    #[test]
    fn invalid_stream_is_removed_without_publishing_a_blob() -> Result<(), Box<dyn Error>> {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let bytes = b"short";
        let entry = HubTreeEntry::new(10, "opaque-validator")?;
        let mut body = MemoryBody(VecDeque::from([Box::<[u8]>::from(bytes.as_slice())]));
        run_ready(kernel.stream_fresh_file_to_blob(
            &mut body,
            &commit,
            &path,
            &entry,
            None,
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("accepted truncated content");
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert!(
            !kernel
                .blob_path(&BlobDigest::for_bytes(bytes))
                .try_exists()?
        );
        Ok(())
    }

    #[test]
    fn cancellation_preserves_only_an_identity_bound_resumable_partial()
    -> Result<(), Box<dyn Error>> {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let cancellation = CancellationToken::new();
        let mut body = CancellingBody {
            chunk: Some(Box::<[u8]>::from(&b"part"[..])),
            cancellation: cancellation.clone(),
        };
        let entry = HubTreeEntry::new(10, "opaque-validator")?;
        let error = run_ready(kernel.stream_fresh_file_to_blob(
            &mut body,
            &commit,
            &path,
            &entry,
            Some("etag"),
            &cancellation,
            &crate::progress::NoopProgress,
        ))
        .expect_err("published a cancelled transfer");
        assert!(error.is_cancelled());
        assert!(kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert_eq!(
            kernel.partial_resume_offset(&commit, &path, 10, 4, Some("etag"), None)?,
            Some(4)
        );
        assert!(
            !kernel
                .blob_path(&BlobDigest::for_bytes(b"part"))
                .try_exists()?
        );
        Ok(())
    }

    #[test]
    fn retryable_disconnect_can_resume_and_validates_the_whole_file() -> Result<(), Box<dyn Error>>
    {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let bytes = b"complete resumed content";
        let digest = BlobDigest::for_bytes(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), bytes.len() as u64)?;
        let split = 9_usize;
        let mut first = DisconnectingBody {
            chunk: Some(Box::<[u8]>::from(&bytes[..split])),
        };
        let error = run_ready(kernel.stream_fresh_file_to_blob(
            &mut first,
            &commit,
            &path,
            &entry,
            Some("stable-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("published a disconnected transfer");
        assert!(error.is_retryable());
        assert_eq!(
            kernel.partial_resume_offset(
                &commit,
                &path,
                bytes.len() as u64,
                split as u64,
                Some("stable-etag"),
                Some(&digest),
            )?,
            Some(split as u64)
        );

        let mut remainder = MemoryBody(VecDeque::from([Box::<[u8]>::from(&bytes[split..])]));
        let resumed = run_ready(kernel.resume_file_to_blob(
            &mut remainder,
            &commit,
            &path,
            &entry,
            split as u64,
            Some("stable-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))?;
        assert_eq!(resumed, digest);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn reqwest_fixture_range_resumes_into_one_validated_blob() -> Result<(), Box<dyn Error>> {
        use crate::hub_protocol::HubProtocol;
        use crate::reqwest_transport::ReqwestTransport;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        let bytes = b"fixture-backed resumable content";
        let split = 11_usize;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("weights/model.bin")?;
        let request_path = format!("/org/repo/resolve/{}/{}", commit.as_str(), path.as_str());
        let fixture = ScriptedHub::start([Exchange::new(
            ExpectedRequest::get(&request_path)
                .header("range", &format!("bytes={split}-"))
                .header("if-range", "stable-etag"),
            ScriptedResponse::new(206, &bytes[split..])
                .header(
                    "content-range",
                    &format!("bytes {split}-{}/{}", bytes.len() - 1, bytes.len()),
                )
                .header("etag", "stable-etag"),
        )])?;
        let endpoint = Endpoint::parse(fixture.endpoint())?;
        let protocol = HubProtocol::new(endpoint, Arc::new(ReqwestTransport::build()?))?;
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let digest = BlobDigest::for_bytes(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), bytes.len() as u64)?;
        let (_directory, kernel) = kernel()?;
        let mut disconnected = DisconnectingBody {
            chunk: Some(Box::<[u8]>::from(&bytes[..split])),
        };
        let first = run_ready(kernel.stream_fresh_file_to_blob(
            &mut disconnected,
            &commit,
            &path,
            &entry,
            Some("stable-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ));
        assert!(
            first
                .expect_err("published a disconnected response")
                .is_retryable()
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        assert_eq!(
            kernel.partial_resume_offset(
                &commit,
                &path,
                bytes.len() as u64,
                split as u64,
                Some("stable-etag"),
                Some(&digest),
            )?,
            Some(split as u64)
        );

        let resumed = runtime.block_on(async {
            let mut response = protocol
                .request_file(
                    &repository,
                    &commit,
                    &path,
                    Some((split as u64, Some("stable-etag"))),
                    None,
                )
                .await?;
            kernel
                .consume_file_response(
                    &mut response,
                    &commit,
                    &path,
                    &entry,
                    Some((split as u64, Some("stable-etag"))),
                    &CancellationToken::new(),
                    &crate::progress::NoopProgress,
                )
                .await
        });
        let observed = fixture.finish();
        let resumed = resumed.map_err(|error| format!("{error}; fixture: {observed:?}"))?;
        assert_eq!(resumed, digest);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        assert_eq!(observed?.len(), 1);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn reqwest_fixture_ignored_range_restarts_from_zero() -> Result<(), Box<dyn Error>> {
        use crate::hub_protocol::HubProtocol;
        use crate::reqwest_transport::ReqwestTransport;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        let bytes = b"server returned the whole file";
        let split = 7_usize;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let digest = BlobDigest::for_bytes(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), bytes.len() as u64)?;
        let (_directory, kernel) = kernel()?;
        let mut disconnected = DisconnectingBody {
            chunk: Some(Box::<[u8]>::from(&bytes[..split])),
        };
        run_ready(kernel.stream_fresh_file_to_blob(
            &mut disconnected,
            &commit,
            &path,
            &entry,
            Some("old-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("published a disconnected prefix");

        let request_path = format!("/org/repo/resolve/{}/model.bin", commit.as_str());
        let fixture = ScriptedHub::start([Exchange::new(
            ExpectedRequest::get(&request_path)
                .header("range", &format!("bytes={split}-"))
                .header("if-range", "old-etag"),
            ScriptedResponse::new(200, bytes.as_slice()).header("etag", "new-etag"),
        )])?;
        let protocol = HubProtocol::new(
            Endpoint::parse(fixture.endpoint())?,
            Arc::new(ReqwestTransport::build()?),
        )?;
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let published = runtime.block_on(async {
            let mut response = protocol
                .request_file(
                    &repository,
                    &commit,
                    &path,
                    Some((split as u64, Some("old-etag"))),
                    None,
                )
                .await?;
            kernel
                .consume_file_response(
                    &mut response,
                    &commit,
                    &path,
                    &entry,
                    Some((split as u64, Some("old-etag"))),
                    &CancellationToken::new(),
                    &crate::progress::NoopProgress,
                )
                .await
        });
        let observed = fixture.finish();
        let published = published.map_err(|error| format!("{error}; fixture: {observed:?}"))?;
        assert_eq!(published, digest);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert_eq!(observed?.len(), 1);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn automatic_download_retries_fixture_status_and_publishes_once() -> Result<(), Box<dyn Error>>
    {
        use crate::hub_protocol::HubProtocol;
        use crate::reqwest_transport::ReqwestTransport;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        let bytes = b"retry succeeded";
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let request_path = format!("/org/repo/resolve/{}/model.bin", commit.as_str());
        let fixture = ScriptedHub::start([
            Exchange::new(
                ExpectedRequest::get(&request_path),
                ScriptedResponse::new(503, b"retry".as_slice()).header("retry-after", "1"),
            ),
            Exchange::new(
                ExpectedRequest::get(&request_path),
                ScriptedResponse::new(200, bytes.as_slice()).header("etag", "stable-etag"),
            ),
        ])?;
        let protocol = Arc::new(HubProtocol::new(
            Endpoint::parse(fixture.endpoint())?,
            Arc::new(ReqwestTransport::build()?),
        )?);
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let digest = BlobDigest::for_bytes(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), bytes.len() as u64)?;
        let (_directory, kernel) = kernel()?;
        let kernel = Arc::new(kernel);
        let policy = RetryPolicy::new(
            2,
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(2),
        )
        .ok_or("invalid retry policy")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(kernel.download_file(
            protocol,
            repository,
            commit,
            path,
            entry,
            None,
            policy,
            &NoDelayClock,
            CancellationToken::new(),
            Arc::new(crate::progress::NoopProgress),
        ));
        let observed = fixture.finish();
        let published = result.map_err(|error| format!("{error}; fixture: {observed:?}"))?;
        assert_eq!(published, digest);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        assert_eq!(observed?.len(), 2);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn automatic_download_reports_fixture_retry_exhaustion_without_publication()
    -> Result<(), Box<dyn Error>> {
        use crate::hub_protocol::HubProtocol;
        use crate::reqwest_transport::ReqwestTransport;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let request_path = format!("/org/repo/resolve/{}/model.bin", commit.as_str());
        let fixture = ScriptedHub::start((0..2).map(|_attempt| {
            Exchange::new(
                ExpectedRequest::get(&request_path),
                ScriptedResponse::new(503, b"unavailable".as_slice()),
            )
        }))?;
        let protocol = Arc::new(HubProtocol::new(
            Endpoint::parse(fixture.endpoint())?,
            Arc::new(ReqwestTransport::build()?),
        )?);
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let entry = HubTreeEntry::new(10, "opaque")?;
        let (_directory, kernel) = kernel()?;
        let kernel = Arc::new(kernel);
        let policy = RetryPolicy::new(
            2,
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(2),
        )
        .ok_or("invalid retry policy")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(kernel.download_file(
            protocol,
            repository,
            commit.clone(),
            path.clone(),
            entry,
            None,
            policy,
            &NoDelayClock,
            CancellationToken::new(),
            Arc::new(crate::progress::NoopProgress),
        ));
        let observed = fixture.finish();
        let error = result.expect_err("retry exhaustion unexpectedly succeeded");
        assert!(error.is_retryable());
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert_eq!(observed?.len(), 2);
        Ok(())
    }

    #[cfg(feature = "network")]
    #[test]
    fn fixture_changed_validator_discards_opaque_resume_before_publication()
    -> Result<(), Box<dyn Error>> {
        use crate::hub_protocol::HubProtocol;
        use crate::reqwest_transport::ReqwestTransport;
        use crate::test_http_fixture::{Exchange, ExpectedRequest, ScriptedHub, ScriptedResponse};

        let expected = b"opaque bytes";
        let split = 4_usize;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("opaque.bin")?;
        let entry = HubTreeEntry::new(expected.len() as u64, "opaque-remote-id")?;
        let (_directory, kernel) = kernel()?;
        let mut disconnected = DisconnectingBody {
            chunk: Some(Box::<[u8]>::from(&expected[..split])),
        };
        run_ready(kernel.stream_fresh_file_to_blob(
            &mut disconnected,
            &commit,
            &path,
            &entry,
            Some("old-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("published a disconnected prefix");

        let request_path = format!("/org/repo/resolve/{}/opaque.bin", commit.as_str());
        let fixture = ScriptedHub::start([Exchange::new(
            ExpectedRequest::get(&request_path)
                .header("range", &format!("bytes={split}-"))
                .header("if-range", "old-etag"),
            ScriptedResponse::new(206, &expected[split..])
                .header(
                    "content-range",
                    &format!("bytes {split}-{}/{}", expected.len() - 1, expected.len()),
                )
                .header("etag", "new-etag"),
        )])?;
        let protocol = HubProtocol::new(
            Endpoint::parse(fixture.endpoint())?,
            Arc::new(ReqwestTransport::build()?),
        )?;
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(async {
            let mut response = protocol
                .request_file(
                    &repository,
                    &commit,
                    &path,
                    Some((split as u64, Some("old-etag"))),
                    None,
                )
                .await?;
            kernel
                .consume_file_response(
                    &mut response,
                    &commit,
                    &path,
                    &entry,
                    Some((split as u64, Some("old-etag"))),
                    &CancellationToken::new(),
                    &crate::progress::NoopProgress,
                )
                .await
        });
        let observed = fixture.finish();
        let error = result.expect_err("accepted a changed opaque validator");
        assert!(error.is_validation());
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert_eq!(observed?.len(), 1);
        Ok(())
    }

    #[test]
    fn resumed_content_with_the_wrong_identity_is_discarded() -> Result<(), Box<dyn Error>> {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let expected = b"expected bytes";
        let digest = BlobDigest::for_bytes(expected);
        let entry = HubTreeEntry::new(expected.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), expected.len() as u64)?;
        let split = 5_usize;
        let mut first = DisconnectingBody {
            chunk: Some(Box::<[u8]>::from(&expected[..split])),
        };
        run_ready(kernel.stream_fresh_file_to_blob(
            &mut first,
            &commit,
            &path,
            &entry,
            Some("stable-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("published a disconnected transfer");

        let mut wrong = MemoryBody(VecDeque::from([
            vec![b'x'; expected.len() - split].into_boxed_slice()
        ]));
        let error = run_ready(kernel.resume_file_to_blob(
            &mut wrong,
            &commit,
            &path,
            &entry,
            split as u64,
            Some("stable-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))
        .expect_err("published content with the wrong digest");
        assert!(error.is_validation());
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert!(!kernel.blob_path(&digest).try_exists()?);
        Ok(())
    }

    #[test]
    fn competing_transfer_workers_converge_before_the_second_body_is_read()
    -> Result<(), Box<dyn Error>> {
        let (_directory, kernel) = kernel()?;
        let kernel = Arc::new(kernel);
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let bytes = b"shared transfer";
        let digest = BlobDigest::for_bytes(bytes);
        let entry = HubTreeEntry::new(bytes.len() as u64, "pointer")?
            .with_lfs(digest.to_string(), bytes.len() as u64)?;
        let gate = Arc::new(Barrier::new(2));
        let reads = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _worker in 0..2 {
            let kernel = Arc::clone(&kernel);
            let commit = commit.clone();
            let path = path.clone();
            let entry = entry.clone();
            let gate = Arc::clone(&gate);
            let reads = Arc::clone(&reads);
            workers.push(std::thread::spawn(move || {
                let mut body = CountingBody {
                    chunk: Some(Box::<[u8]>::from(bytes.as_slice())),
                    reads,
                };
                gate.wait();
                run_ready(kernel.stream_fresh_file_to_blob(
                    &mut body,
                    &commit,
                    &path,
                    &entry,
                    Some("etag"),
                    &CancellationToken::new(),
                    &crate::progress::NoopProgress,
                ))
            }));
        }
        for worker in workers {
            assert_eq!(
                worker
                    .join()
                    .map_err(|_panic| "transfer worker panicked")??,
                digest
            );
        }
        assert_eq!(reads.load(Ordering::Acquire), 1);
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, bytes);
        Ok(())
    }

    #[test]
    fn incompatible_partial_identity_is_restarted_before_publication() -> Result<(), Box<dyn Error>>
    {
        let (_directory, kernel) = kernel()?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let path = RepoPath::parse("model.bin")?;
        let cancellation = CancellationToken::new();
        let entry = HubTreeEntry::new(10, "opaque-validator")?;
        let mut cancelled = CancellingBody {
            chunk: Some(Box::<[u8]>::from(&b"old!"[..])),
            cancellation: cancellation.clone(),
        };
        run_ready(kernel.stream_fresh_file_to_blob(
            &mut cancelled,
            &commit,
            &path,
            &entry,
            Some("old-etag"),
            &cancellation,
            &crate::progress::NoopProgress,
        ))
        .expect_err("cancelled prefix unexpectedly published");

        let complete = b"new-bytes!";
        let mut replacement = MemoryBody(VecDeque::from([Box::<[u8]>::from(complete.as_slice())]));
        let digest = run_ready(kernel.stream_fresh_file_to_blob(
            &mut replacement,
            &commit,
            &path,
            &entry,
            Some("new-etag"),
            &CancellationToken::new(),
            &crate::progress::NoopProgress,
        ))?;
        assert_eq!(std::fs::read(kernel.blob_path(&digest))?, complete);
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        Ok(())
    }

    fn kernel() -> Result<(TempDir, CacheKernel), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(RandomOperationIds),
            Arc::new(SystemClock),
            Arc::new(NoPublicationFaults),
        );
        let kernel = CacheKernel::new(
            directory.path(),
            &Endpoint::hugging_face(),
            &RepositorySpec::model(RepositoryId::parse("org/repo")?),
            effects,
        )?;
        kernel.initialize()?;
        Ok((directory, kernel))
    }

    fn run_ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("future unexpectedly remained pending"),
        }
    }
}
