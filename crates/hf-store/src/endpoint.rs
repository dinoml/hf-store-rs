use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use url::Url;

use crate::validation::{ValidationError, ValidationErrorKind};

/// Identifies one canonical HTTP or HTTPS Hub-compatible endpoint.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Endpoint(Box<str>);

impl Endpoint {
    /// Parses and canonicalizes a Hub-compatible base endpoint.
    ///
    /// Scheme and host case, default ports, and trailing slashes are
    /// canonicalized. Base paths are preserved.
    ///
    /// # Errors
    ///
    /// Returns an error for non-HTTP schemes, relative URLs, credentials,
    /// queries, fragments, NUL characters, or malformed URLs.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(endpoint_error(ValidationErrorKind::Empty));
        }
        if value.contains('\0') {
            return Err(endpoint_error(ValidationErrorKind::ContainsNul));
        }

        let Ok(mut url) = Url::parse(value) else {
            return Err(endpoint_error(ValidationErrorKind::Malformed));
        };
        if !matches!(url.scheme(), "http" | "https")
            || url.cannot_be_a_base()
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(endpoint_error(ValidationErrorKind::Malformed));
        }

        let default_port = matches!(
            (url.scheme(), url.port()),
            ("http", Some(80)) | ("https", Some(443))
        );
        if default_port && url.set_port(None).is_err() {
            return Err(endpoint_error(ValidationErrorKind::Malformed));
        }

        let path = url.path().trim_end_matches('/').to_owned();
        if path.is_empty() {
            url.set_path("/");
        } else {
            url.set_path(&path);
        }

        let normalized = url.to_string().trim_end_matches('/').to_owned();
        Ok(Self(normalized.into()))
    }

    /// Returns the canonical public Hugging Face Hub endpoint.
    #[must_use]
    pub fn hugging_face() -> Self {
        Self("https://huggingface.co".into())
    }

    /// Returns the canonical endpoint URL without a trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Endpoint {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for Endpoint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Endpoint {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

fn endpoint_error(kind: ValidationErrorKind) -> ValidationError {
    ValidationError::new("endpoint", kind)
}
