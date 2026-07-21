use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use crate::validation::{ValidationError, ValidationErrorKind};

/// Names a symbolic repository revision such as a branch, tag, or pull request.
///
/// Revisions are opaque logical identifiers and may contain `/`. They are never
/// suitable for direct use as host path components.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Revision(Box<str>);

impl Revision {
    /// Parses a non-empty symbolic revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the revision is empty or contains a NUL character.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(ValidationError::new("revision", ValidationErrorKind::Empty));
        }
        if value.contains('\0') {
            return Err(ValidationError::new(
                "revision",
                ValidationErrorKind::ContainsNul,
            ));
        }

        Ok(Self(value.into()))
    }

    /// Returns the revision exactly as it was parsed.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Revision {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for Revision {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Revision {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Identifies one immutable Hub commit.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitId(Box<str>);

impl CommitId {
    /// Parses a full lowercase 40-hex-character Hub commit identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not exactly 40 lowercase hexadecimal
    /// characters.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        if value.len() != 40
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ValidationError::new(
                "commit identifier",
                ValidationErrorKind::Malformed,
            ));
        }

        Ok(Self(value.into()))
    }

    /// Returns the lowercase hexadecimal commit identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CommitId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for CommitId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CommitId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}
