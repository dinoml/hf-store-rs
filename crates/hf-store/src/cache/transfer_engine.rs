use crate::error::{CacheFailure, HubOperationError};
use crate::progress::ProgressObserver;
use crate::transfer::append_bounded_body;
use crate::transport::TransportBody;
use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CancellationToken, CommitId, RepoPath};

use super::hub_cache::copy_and_validate_content;
use super::hub_metadata::HubTreeEntry;
use super::key::BlobDigest;
use super::publication::CacheKernel;

impl CacheKernel {
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

    use crate::transport::{TransportError, TransportFuture};
    use crate::{Endpoint, RepositoryId, RepositorySpec};

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
