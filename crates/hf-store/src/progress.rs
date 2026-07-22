use std::fmt::Debug;

use crate::RepoPath;

/// A stable high-level phase of repository acquisition.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgressPhase {
    /// Repository metadata is being resolved and selected.
    Planning,
    /// File body bytes are being transferred.
    Transferring,
    /// Completed bytes are being validated.
    Validating,
    /// Validated state is being atomically published.
    Publishing,
    /// A retry delay is being observed.
    RetryWaiting,
    /// The requested operation is complete.
    Complete,
}

/// Describes whether file body bytes came from an existing validated source.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReuseDecision {
    /// Reuse has not yet been decided.
    Pending,
    /// Existing validated bytes satisfied the request.
    Reused,
    /// Bytes were received from the configured Hub endpoint.
    Downloaded,
}

/// A credential-free structured acquisition progress update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressEvent {
    phase: ProgressPhase,
    path: Option<RepoPath>,
    validated_bytes: u64,
    transferred_bytes: u64,
    total_bytes: Option<u64>,
    attempt: u32,
    reuse: ReuseDecision,
    retryable: bool,
}

impl ProgressEvent {
    pub(crate) const fn transfer(path: RepoPath, transferred_bytes: u64, total_bytes: u64) -> Self {
        Self {
            phase: ProgressPhase::Transferring,
            path: Some(path),
            validated_bytes: 0,
            transferred_bytes,
            total_bytes: Some(total_bytes),
            attempt: 1,
            reuse: ReuseDecision::Downloaded,
            retryable: false,
        }
    }

    pub(crate) const fn validated(path: RepoPath, bytes: u64) -> Self {
        Self {
            phase: ProgressPhase::Validating,
            path: Some(path),
            validated_bytes: bytes,
            transferred_bytes: bytes,
            total_bytes: Some(bytes),
            attempt: 1,
            reuse: ReuseDecision::Downloaded,
            retryable: false,
        }
    }

    /// Returns the operation phase.
    #[must_use]
    pub const fn phase(&self) -> ProgressPhase {
        self.phase
    }

    /// Returns the affected repository path, when the event is file-specific.
    #[must_use]
    pub const fn path(&self) -> Option<&RepoPath> {
        self.path.as_ref()
    }

    /// Returns bytes whose content validation has completed.
    #[must_use]
    pub const fn validated_bytes(&self) -> u64 {
        self.validated_bytes
    }

    /// Returns bytes transferred from the network for this file.
    #[must_use]
    pub const fn transferred_bytes(&self) -> u64 {
        self.transferred_bytes
    }

    /// Returns the expected byte total when known.
    #[must_use]
    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    /// Returns the one-based attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the current byte reuse decision.
    #[must_use]
    pub const fn reuse(&self) -> ReuseDecision {
        self.reuse
    }

    /// Returns whether the triggering failure is eligible for retry.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }
}

/// Receives structured progress synchronously at cooperative boundaries.
///
/// Implementations should return quickly and hand off expensive UI or logging
/// work to their own bounded channel.
pub trait ProgressObserver: Debug + Send + Sync {
    /// Observes one credential-free progress update.
    fn observe(&self, event: &ProgressEvent);
}

#[derive(Debug)]
#[cfg(test)]
pub(crate) struct NoopProgress;

#[cfg(test)]
impl ProgressObserver for NoopProgress {
    fn observe(&self, _event: &ProgressEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_values_are_send_and_contain_only_safe_typed_state()
    -> Result<(), Box<dyn std::error::Error>> {
        fn assert_send<T: Send>() {}
        assert_send::<ProgressEvent>();
        let event = ProgressEvent::transfer(RepoPath::parse("model.bin")?, 4, 10);
        assert_eq!(event.phase(), ProgressPhase::Transferring);
        assert_eq!(event.path().map(RepoPath::as_str), Some("model.bin"));
        assert_eq!(event.transferred_bytes(), 4);
        assert_eq!(event.total_bytes(), Some(10));
        assert_eq!(event.reuse(), ReuseDecision::Downloaded);
        assert!(!event.is_retryable());
        Ok(())
    }
}
