use std::fmt::Debug;
use std::io;

use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::cache::{BlobDigest, HubTreeEntry};
use crate::error::{CacheFailure, HubOperationError};
use crate::transport::TransportBody;

pub(crate) trait PartialSink: Debug + Send {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn sync_all(&self) -> io::Result<()>;
}

pub(crate) async fn stream_validated_body(
    body: &mut dyn TransportBody,
    sink: &mut dyn PartialSink,
    entry: &HubTreeEntry,
) -> Result<BlobDigest, HubOperationError> {
    let expected_lfs = match (entry.lfs_sha256(), entry.lfs_size()) {
        (Some(sha256), Some(size)) if is_lower_hex(sha256, 64) && size == entry.size() => {
            Some(sha256)
        }
        (None, None) => None,
        (Some(_), _) | (None, Some(_)) => {
            return Err(HubOperationError::validation(transfer_validation_error()));
        }
    };
    let expected_git =
        (expected_lfs.is_none() && is_lower_hex(entry.blob_id(), 40)).then_some(entry.blob_id());
    let mut git_hasher = expected_git.map(|_expected| {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", entry.size()).as_bytes());
        hasher
    });
    let mut local_hasher = Sha256::new();
    let mut received = 0_u64;

    while let Some(chunk) = body
        .next_chunk()
        .await
        .map_err(HubOperationError::transport)?
    {
        let chunk_size = u64::try_from(chunk.len())
            .map_err(|_overflow| HubOperationError::validation(transfer_validation_error()))?;
        received = received
            .checked_add(chunk_size)
            .ok_or_else(|| HubOperationError::validation(transfer_validation_error()))?;
        if received > entry.size() {
            return Err(HubOperationError::validation(transfer_validation_error()));
        }
        sink.write_all(&chunk)
            .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;
        local_hasher.update(&chunk);
        if let Some(hasher) = git_hasher.as_mut() {
            hasher.update(&chunk);
        }
    }
    if received != entry.size() {
        return Err(HubOperationError::validation(transfer_validation_error()));
    }
    sink.sync_all()
        .map_err(|_source| HubOperationError::cache(CacheFailure::Io))?;

    let digest = BlobDigest::from_bytes(local_hasher.finalize().into());
    if expected_lfs.is_some_and(|expected| digest.to_string() != expected) {
        return Err(HubOperationError::validation(transfer_validation_error()));
    }
    if let (Some(expected), Some(hasher)) = (expected_git, git_hasher) {
        if format!("{:x}", hasher.finalize()) != expected {
            return Err(HubOperationError::validation(transfer_validation_error()));
        }
    }
    Ok(digest)
}

fn transfer_validation_error() -> crate::ValidationError {
    crate::ValidationError::new(
        "Hub file content",
        crate::validation::ValidationErrorKind::Malformed,
    )
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyDisposition {
    Fresh {
        expected_body_bytes: u64,
    },
    Resume {
        offset: u64,
        expected_body_bytes: u64,
    },
    Restart {
        expected_body_bytes: u64,
    },
}

pub(crate) fn validate_file_response(
    status: u16,
    content_length: Option<&str>,
    content_range: Option<&str>,
    requested_offset: Option<u64>,
    expected_size: u64,
) -> Result<BodyDisposition, HubOperationError> {
    let content_length = content_length
        .map(parse_decimal)
        .transpose()?
        .map(|value| value.0);
    match requested_offset {
        None => {
            if status != 200 || content_range.is_some() {
                return Err(HubOperationError::protocol());
            }
            require_matching_length(content_length, expected_size)?;
            Ok(BodyDisposition::Fresh {
                expected_body_bytes: expected_size,
            })
        }
        Some(offset) => {
            if offset == 0 || offset >= expected_size {
                return Err(HubOperationError::protocol());
            }
            match status {
                200 => {
                    if content_range.is_some() {
                        return Err(HubOperationError::protocol());
                    }
                    require_matching_length(content_length, expected_size)?;
                    Ok(BodyDisposition::Restart {
                        expected_body_bytes: expected_size,
                    })
                }
                206 => {
                    let range = content_range
                        .ok_or_else(HubOperationError::protocol)
                        .and_then(parse_content_range)?;
                    let expected_body_bytes = expected_size
                        .checked_sub(offset)
                        .ok_or_else(HubOperationError::protocol)?;
                    if range.start != offset
                        || range.end != expected_size - 1
                        || range.total != expected_size
                    {
                        return Err(HubOperationError::protocol());
                    }
                    require_matching_length(content_length, expected_body_bytes)?;
                    Ok(BodyDisposition::Resume {
                        offset,
                        expected_body_bytes,
                    })
                }
                _ => Err(HubOperationError::protocol()),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

fn parse_content_range(value: &str) -> Result<ContentRange, HubOperationError> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(HubOperationError::protocol)?;
    let (bounds, total) = value
        .split_once('/')
        .ok_or_else(HubOperationError::protocol)?;
    let (start, end) = bounds
        .split_once('-')
        .ok_or_else(HubOperationError::protocol)?;
    let start = parse_decimal(start)?.0;
    let end = parse_decimal(end)?.0;
    let total = parse_decimal(total)?.0;
    if start > end || end >= total {
        return Err(HubOperationError::protocol());
    }
    Ok(ContentRange { start, end, total })
}

struct Decimal(u64);

fn parse_decimal(value: &str) -> Result<Decimal, HubOperationError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HubOperationError::protocol());
    }
    value
        .parse::<u64>()
        .map(Decimal)
        .map_err(|_source| HubOperationError::protocol())
}

fn require_matching_length(actual: Option<u64>, expected: u64) -> Result<(), HubOperationError> {
    if actual.is_some_and(|actual| actual != expected) {
        return Err(HubOperationError::protocol());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};

    use crate::transport::{TransportError, TransportFuture};

    use super::*;

    #[derive(Debug)]
    struct MemoryBody(VecDeque<Result<Box<[u8]>, TransportError>>);

    impl TransportBody for MemoryBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            Box::pin(std::future::ready(match self.0.pop_front() {
                Some(result) => result.map(Some),
                None => Ok(None),
            }))
        }
    }

    #[derive(Debug, Default)]
    struct MemorySink {
        bytes: Vec<u8>,
        fail_write: bool,
        synced: AtomicBool,
    }

    impl PartialSink for MemorySink {
        fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            if self.fail_write {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "fixture"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }

        fn sync_all(&self) -> io::Result<()> {
            self.synced.store(true, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn full_and_exact_partial_responses_are_accepted() -> Result<(), HubOperationError> {
        assert_eq!(
            validate_file_response(200, Some("9"), None, None, 9)?,
            BodyDisposition::Fresh {
                expected_body_bytes: 9
            }
        );
        assert_eq!(
            validate_file_response(206, Some("5"), Some("bytes 4-8/9"), Some(4), 9)?,
            BodyDisposition::Resume {
                offset: 4,
                expected_body_bytes: 5
            }
        );
        Ok(())
    }

    #[test]
    fn a_full_response_to_a_range_request_requires_a_safe_restart() -> Result<(), HubOperationError>
    {
        assert_eq!(
            validate_file_response(200, Some("9"), None, Some(4), 9)?,
            BodyDisposition::Restart {
                expected_body_bytes: 9
            }
        );
        Ok(())
    }

    #[test]
    fn malformed_mismatched_and_overrunning_ranges_are_rejected() {
        let cases = [
            (206, Some("5"), None, Some(4), 9),
            (206, Some("5"), Some("items 4-8/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 3-8/9"), Some(4), 9),
            (206, Some("4"), Some("bytes 4-8/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 4-8/10"), Some(4), 9),
            (206, Some("4"), Some("bytes 4-7/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 8-4/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 4-9/9"), Some(4), 9),
            (206, Some("5x"), Some("bytes 4-8/9"), Some(4), 9),
            (200, Some("8"), None, Some(4), 9),
            (200, Some("9"), Some("bytes 0-8/9"), Some(4), 9),
            (206, Some("9"), Some("bytes 0-8/9"), None, 9),
        ];
        for (status, length, range, offset, expected) in cases {
            assert!(
                validate_file_response(status, length, range, offset, expected)
                    .expect_err("accepted an invalid file response")
                    .is_protocol()
            );
        }
    }

    #[test]
    fn invalid_resume_offsets_and_decimal_overflow_are_rejected() {
        for offset in [0, 9, 10] {
            validate_file_response(206, None, Some("bytes 1-8/9"), Some(offset), 9)
                .expect_err("accepted an invalid resume offset");
        }
        validate_file_response(200, Some("18446744073709551616"), None, None, 9)
            .expect_err("accepted an overflowing decimal header");
    }

    #[test]
    fn streamed_git_and_lfs_content_is_bounded_hashed_and_synced()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"validated body";
        let mut git = Sha1::new();
        git.update(format!("blob {}\0", bytes.len()).as_bytes());
        git.update(bytes);
        let git_id = format!("{:x}", git.finalize());
        let lfs_id = format!("{:x}", Sha256::digest(bytes));
        for entry in [
            HubTreeEntry::new(bytes.len() as u64, git_id)?,
            HubTreeEntry::new(bytes.len() as u64, "pointer")?
                .with_lfs(lfs_id, bytes.len() as u64)?,
        ] {
            let mut body = MemoryBody(VecDeque::from([
                Ok(Box::<[u8]>::from(&bytes[..4])),
                Ok(Box::<[u8]>::from(&bytes[4..])),
            ]));
            let mut sink = MemorySink::default();
            let digest = run_ready(stream_validated_body(&mut body, &mut sink, &entry))?;
            assert_eq!(sink.bytes, bytes);
            assert!(sink.synced.load(Ordering::Acquire));
            assert_eq!(digest, BlobDigest::for_bytes(bytes));
        }
        Ok(())
    }

    #[test]
    fn truncated_overrunning_invalid_and_failed_streams_never_validate()
    -> Result<(), Box<dyn std::error::Error>> {
        let entry = HubTreeEntry::new(4, "opaque")?;
        for chunks in [
            VecDeque::from([Ok(Box::<[u8]>::from(&b"abc"[..]))]),
            VecDeque::from([Ok(Box::<[u8]>::from(&b"abcde"[..]))]),
            VecDeque::from([Err(TransportError::body())]),
        ] {
            let mut body = MemoryBody(chunks);
            let mut sink = MemorySink::default();
            run_ready(stream_validated_body(&mut body, &mut sink, &entry))
                .expect_err("accepted an invalid stream");
        }
        let mut body = MemoryBody(VecDeque::from([Ok(Box::<[u8]>::from(&b"abcd"[..]))]));
        let mut sink = MemorySink {
            fail_write: true,
            ..MemorySink::default()
        };
        assert!(
            run_ready(stream_validated_body(&mut body, &mut sink, &entry))
                .expect_err("accepted a sink failure")
                .is_cache()
        );
        Ok(())
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
