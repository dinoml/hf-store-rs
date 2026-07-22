use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;

use zeroize::Zeroizing;

use crate::validation::{ValidationError, ValidationErrorKind};

/// Holds a request-time authentication token with redacted diagnostics.
///
/// The token is zeroed when its final owned value is dropped. This type does
/// not persist credentials or discover them from ambient configuration.
#[derive(Clone)]
pub struct AuthToken(
    #[allow(
        dead_code,
        reason = "the private transport seam consumes the token only in tests until its adapter lands"
    )]
    Zeroizing<String>,
);

impl AuthToken {
    /// Creates a non-empty request-time authentication token.
    ///
    /// # Errors
    ///
    /// Returns an error when the token is empty or contains a NUL character.
    /// The error never contains the rejected token.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = Zeroizing::new(value.into());
        if value.is_empty() {
            return Err(ValidationError::new(
                "authentication token",
                ValidationErrorKind::Empty,
            ));
        }
        if value.contains('\0') {
            return Err(ValidationError::new(
                "authentication token",
                ValidationErrorKind::ContainsNul,
            ));
        }

        Ok(Self(value))
    }

    #[allow(
        dead_code,
        reason = "the production network adapter will consume the validated request-time token"
    )]
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for AuthToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthToken([REDACTED])")
    }
}

impl FromStr for AuthToken {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
