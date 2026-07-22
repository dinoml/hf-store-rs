use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::time::Duration;

use crate::transport::TransportError;
use crate::validation::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheFailure {
    Missing,
    Incomplete,
    Corrupt,
    UnsupportedVersion,
    Busy,
    Io,
}

/// A safe, classified failure from Hub planning or acquisition.
///
/// Human-readable text and private causes are not a stable classification API;
/// use the query methods when selecting retry or user-interface policy.
pub struct HubOperationError {
    kind: Box<HubOperationErrorKind>,
    backtrace: Backtrace,
}

enum HubOperationErrorKind {
    Authentication,
    Gated,
    Missing,
    RateLimited { retry_after: Option<Duration> },
    Transport(TransportError),
    TransportStatus(u16),
    Protocol,
    Validation(ValidationError),
    Cancelled,
    Cache(CacheFailure),
}

impl HubOperationError {
    fn new(kind: HubOperationErrorKind) -> Self {
        Self {
            kind: Box::new(kind),
            backtrace: Backtrace::capture(),
        }
    }

    pub(crate) fn authentication() -> Self {
        Self::new(HubOperationErrorKind::Authentication)
    }

    pub(crate) fn gated() -> Self {
        Self::new(HubOperationErrorKind::Gated)
    }

    pub(crate) fn missing() -> Self {
        Self::new(HubOperationErrorKind::Missing)
    }

    pub(crate) fn rate_limited(retry_after: Option<Duration>) -> Self {
        Self::new(HubOperationErrorKind::RateLimited { retry_after })
    }

    pub(crate) fn transport(source: TransportError) -> Self {
        if source.is_authentication() {
            Self::authentication()
        } else if source.is_protocol() || source.is_redirect() {
            Self::protocol()
        } else {
            Self::new(HubOperationErrorKind::Transport(source))
        }
    }

    pub(crate) fn protocol() -> Self {
        Self::new(HubOperationErrorKind::Protocol)
    }

    pub(crate) fn validation(source: ValidationError) -> Self {
        Self::new(HubOperationErrorKind::Validation(source))
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(HubOperationErrorKind::Cancelled)
    }

    pub(crate) fn cache(failure: CacheFailure) -> Self {
        Self::new(HubOperationErrorKind::Cache(failure))
    }

    pub(crate) fn from_status(status: u16, retry_after: Option<Duration>) -> Option<Self> {
        match status {
            200..=299 => None,
            401 => Some(Self::authentication()),
            403 => Some(Self::gated()),
            404 => Some(Self::missing()),
            429 => Some(Self::rate_limited(retry_after)),
            408 | 500..=599 => Some(Self::new(HubOperationErrorKind::TransportStatus(status))),
            _ => Some(Self::protocol()),
        }
    }

    /// Returns whether request authentication failed.
    #[must_use]
    pub fn is_authentication(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Authentication)
    }

    /// Returns whether access requires accepting a gated repository's terms.
    #[must_use]
    pub fn is_gated(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Gated)
    }

    /// Returns whether the requested repository object was not found.
    #[must_use]
    pub fn is_missing(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Missing)
    }

    /// Returns whether the Hub rate limited the request.
    #[must_use]
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            HubOperationErrorKind::RateLimited { .. }
        )
    }

    /// Returns a server-provided retry delay when safely parsed.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self.kind.as_ref() {
            HubOperationErrorKind::RateLimited { retry_after } => *retry_after,
            _ => None,
        }
    }

    /// Returns whether an HTTP transport operation failed.
    #[must_use]
    pub fn is_transport(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            HubOperationErrorKind::Transport(_) | HubOperationErrorKind::TransportStatus(_)
        )
    }

    /// Returns whether a response violated the supported Hub protocol.
    #[must_use]
    pub fn is_protocol(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Protocol)
    }

    /// Returns whether remote data failed identity or safety validation.
    #[must_use]
    pub fn is_validation(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Validation(_))
    }

    /// Returns whether cooperative cancellation stopped the operation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Cancelled)
    }

    pub(crate) fn cache_failure(&self) -> Option<CacheFailure> {
        match self.kind.as_ref() {
            HubOperationErrorKind::Cache(failure) => Some(*failure),
            _ => None,
        }
    }

    /// Returns whether local cache state prevented the operation.
    #[must_use]
    pub fn is_cache(&self) -> bool {
        matches!(self.kind.as_ref(), HubOperationErrorKind::Cache(_))
    }

    /// Returns whether required cache content is absent or logically incomplete.
    #[must_use]
    pub fn is_cache_incomplete(&self) -> bool {
        matches!(
            self.cache_failure(),
            Some(CacheFailure::Missing | CacheFailure::Incomplete)
        )
    }

    /// Returns whether cache content or metadata is corrupt.
    #[must_use]
    pub fn is_cache_corrupt(&self) -> bool {
        self.cache_failure() == Some(CacheFailure::Corrupt)
    }

    /// Returns whether cache metadata uses an unsupported version.
    #[must_use]
    pub fn is_cache_unsupported(&self) -> bool {
        self.cache_failure() == Some(CacheFailure::UnsupportedVersion)
    }

    /// Returns whether active coordination prevents a cache operation.
    #[must_use]
    pub fn is_cache_busy(&self) -> bool {
        self.cache_failure() == Some(CacheFailure::Busy)
    }

    /// Returns whether this build has no usable network backend.
    #[must_use]
    pub fn is_backend_unavailable(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            HubOperationErrorKind::Transport(source) if source.is_unavailable()
        )
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self.kind.as_ref() {
            HubOperationErrorKind::RateLimited { .. } => true,
            HubOperationErrorKind::Transport(source) => source.is_connection() || source.is_body(),
            HubOperationErrorKind::TransportStatus(status) => {
                matches!(*status, 408 | 500 | 502 | 503 | 504)
            }
            HubOperationErrorKind::Authentication
            | HubOperationErrorKind::Gated
            | HubOperationErrorKind::Missing
            | HubOperationErrorKind::Protocol
            | HubOperationErrorKind::Validation(_)
            | HubOperationErrorKind::Cancelled
            | HubOperationErrorKind::Cache(_) => false,
        }
    }

    /// Returns the captured operation backtrace.
    pub const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Debug for HubOperationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind.as_ref() {
            HubOperationErrorKind::Authentication => "Authentication",
            HubOperationErrorKind::Gated => "Gated",
            HubOperationErrorKind::Missing => "Missing",
            HubOperationErrorKind::RateLimited { .. } => "RateLimited",
            HubOperationErrorKind::Transport(_) | HubOperationErrorKind::TransportStatus(_) => {
                "Transport"
            }
            HubOperationErrorKind::Protocol => "Protocol",
            HubOperationErrorKind::Validation(_) => "Validation",
            HubOperationErrorKind::Cancelled => "Cancelled",
            HubOperationErrorKind::Cache(_) => "Cache",
        };
        formatter
            .debug_struct("HubOperationError")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

impl Display for HubOperationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind.as_ref() {
            HubOperationErrorKind::Authentication => "Hub authentication failed",
            HubOperationErrorKind::Gated => "Hub repository access is gated",
            HubOperationErrorKind::Missing => "Hub object was not found",
            HubOperationErrorKind::RateLimited { .. } => "Hub request was rate limited",
            HubOperationErrorKind::Transport(_) | HubOperationErrorKind::TransportStatus(_) => {
                "Hub transport failed"
            }
            HubOperationErrorKind::Protocol => "Hub response violated the protocol",
            HubOperationErrorKind::Validation(_) => "Hub data failed validation",
            HubOperationErrorKind::Cancelled => "Hub operation was cancelled",
            HubOperationErrorKind::Cache(_) => "Hub cache operation failed",
        };
        formatter.write_str(message)
    }
}

impl Error for HubOperationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            HubOperationErrorKind::Transport(source) => Some(source),
            HubOperationErrorKind::Authentication
            | HubOperationErrorKind::Gated
            | HubOperationErrorKind::Missing
            | HubOperationErrorKind::RateLimited { .. }
            | HubOperationErrorKind::Protocol
            | HubOperationErrorKind::TransportStatus(_)
            | HubOperationErrorKind::Validation(_)
            | HubOperationErrorKind::Cancelled
            | HubOperationErrorKind::Cache(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::ValidationErrorKind;

    const SECRET: &str = "hf_secret_operation_error_sentinel";

    #[test]
    fn every_supported_failure_has_a_stable_classification_helper() {
        let retry = Duration::from_secs(7);
        let cases = [
            HubOperationError::authentication().is_authentication(),
            HubOperationError::gated().is_gated(),
            HubOperationError::missing().is_missing(),
            HubOperationError::rate_limited(Some(retry)).is_rate_limited(),
            HubOperationError::transport(TransportError::connection()).is_transport(),
            HubOperationError::protocol().is_protocol(),
            HubOperationError::validation(ValidationError::new(
                "fixture",
                ValidationErrorKind::Malformed,
            ))
            .is_validation(),
            HubOperationError::cancelled().is_cancelled(),
            HubOperationError::cache(CacheFailure::Corrupt).cache_failure()
                == Some(CacheFailure::Corrupt),
        ];
        assert!(cases.into_iter().all(std::convert::identity));
        assert_eq!(
            HubOperationError::rate_limited(Some(retry)).retry_after(),
            Some(retry)
        );
        assert!(HubOperationError::from_status(204, None).is_none());
        assert!(
            HubOperationError::from_status(401, None)
                .is_some_and(|error| error.is_authentication())
        );
        assert!(HubOperationError::from_status(403, None).is_some_and(|error| error.is_gated()));
        assert!(HubOperationError::from_status(404, None).is_some_and(|error| error.is_missing()));
        assert!(
            HubOperationError::from_status(429, Some(retry))
                .is_some_and(|error| error.retry_after() == Some(retry))
        );
    }

    #[test]
    fn operation_errors_are_send_and_never_echo_sensitive_context() {
        fn assert_send<T: Send>() {}
        assert_send::<HubOperationError>();
        for error in [
            HubOperationError::transport(TransportError::body()),
            HubOperationError::validation(ValidationError::new(
                SECRET,
                ValidationErrorKind::Malformed,
            )),
            HubOperationError::cache(CacheFailure::Io),
        ] {
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains(SECRET));
            let mut source = error.source();
            while let Some(current) = source {
                assert!(!current.to_string().contains(SECRET));
                assert!(!format!("{current:?}").contains(SECRET));
                source = current.source();
            }
        }
    }
}
