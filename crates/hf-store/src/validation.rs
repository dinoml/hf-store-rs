use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub(crate) enum ValidationErrorKind {
    Empty,
    ContainsNul,
    TooLong,
    Malformed,
    InvalidCharacter,
    UnsafePath,
    Collision,
}

/// Reports why a public value could not be validated.
///
/// The rejected input is deliberately absent so secrets and untrusted values
/// cannot be echoed through diagnostics.
#[derive(Debug)]
pub struct ValidationError {
    subject: &'static str,
    kind: ValidationErrorKind,
    backtrace: Backtrace,
}

impl ValidationError {
    pub(crate) fn new(subject: &'static str, kind: ValidationErrorKind) -> Self {
        Self {
            subject,
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// Returns the logical value category that failed validation.
    #[must_use]
    pub fn subject(&self) -> &'static str {
        self.subject
    }

    /// Returns whether the input was empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self.kind, ValidationErrorKind::Empty)
    }

    /// Returns whether the input contained a NUL character.
    #[must_use]
    pub fn contains_nul(&self) -> bool {
        matches!(self.kind, ValidationErrorKind::ContainsNul)
    }

    /// Returns whether the input had an invalid shape or character.
    #[must_use]
    pub fn is_malformed(&self) -> bool {
        matches!(
            self.kind,
            ValidationErrorKind::TooLong
                | ValidationErrorKind::Malformed
                | ValidationErrorKind::InvalidCharacter
        )
    }

    /// Returns whether a path was unsafe to materialize.
    #[must_use]
    pub fn is_unsafe_path(&self) -> bool {
        matches!(
            self.kind,
            ValidationErrorKind::UnsafePath | ValidationErrorKind::Collision
        )
    }

    /// Returns the captured validation backtrace.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for ValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let reason = match self.kind {
            ValidationErrorKind::Empty => "the value is empty",
            ValidationErrorKind::ContainsNul => "the value contains a NUL character",
            ValidationErrorKind::TooLong => "the value exceeds its maximum length",
            ValidationErrorKind::Malformed => "the value has an invalid form",
            ValidationErrorKind::InvalidCharacter => "the value contains an invalid character",
            ValidationErrorKind::UnsafePath => "the value is unsafe to materialize",
            ValidationErrorKind::Collision => "the value collides during materialization",
        };

        write!(formatter, "invalid {}: {reason}", self.subject)
    }
}

impl Error for ValidationError {}
