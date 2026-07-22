use crate::error::{CacheFailure, HubOperationError};
use crate::transfer::stream_validated_body;
use crate::transport::TransportBody;
use crate::{CommitId, RepoPath};

use super::hub_metadata::HubTreeEntry;
use super::key::BlobDigest;
use super::publication::CacheKernel;

impl CacheKernel {
    pub(super) async fn stream_fresh_file_to_blob(
        &self,
        body: &mut dyn TransportBody,
        commit: &CommitId,
        path: &RepoPath,
        entry: &HubTreeEntry,
    ) -> Result<BlobDigest, HubOperationError> {
        let mut sink = self
            .create_fresh_partial_sink(commit, path)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        let streamed = stream_validated_body(body, &mut sink, entry).await;
        drop(sink);
        let digest = match streamed {
            Ok(digest) => digest,
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
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::Arc;
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
        let digest =
            run_ready(kernel.stream_fresh_file_to_blob(&mut body, &commit, &path, &entry))?;
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
        run_ready(kernel.stream_fresh_file_to_blob(&mut body, &commit, &path, &entry))
            .expect_err("accepted truncated content");
        assert!(!kernel.partial_data_path(&commit, &path)?.try_exists()?);
        assert!(
            !kernel
                .blob_path(&BlobDigest::for_bytes(bytes))
                .try_exists()?
        );
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
