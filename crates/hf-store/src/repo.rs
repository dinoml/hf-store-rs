use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use crate::validation::{ValidationError, ValidationErrorKind};

const MAX_REPOSITORY_ID_LENGTH: usize = 96;

/// Identifies a repository namespace on a Hugging Face Hub endpoint.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RepositoryKind {
    /// A model repository.
    Model,
    /// A dataset repository.
    Dataset,
    /// A Space application repository.
    Space,
}

impl Display for RepositoryKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
        };

        formatter.write_str(name)
    }
}

impl RepositoryKind {
    pub(crate) const fn cache_tag(self) -> u8 {
        match self {
            Self::Model => 1,
            Self::Dataset => 2,
            Self::Space => 3,
        }
    }

    pub(crate) const fn cache_directory(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }
}

/// Identifies one repository within a Hub repository-kind namespace.
///
/// Identifiers contain either a repository name or `namespace/name`. Spelling
/// and case are preserved. Repository kind remains a separate value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepositoryId(Box<str>);

impl RepositoryId {
    /// Parses and validates a Hub repository identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the identifier is empty, longer than 96 ASCII
    /// characters, has more than one namespace separator, or violates Hub name
    /// syntax.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        validate_repository_id(value)?;
        Ok(Self(value.into()))
    }

    /// Returns the identifier exactly as it was parsed.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the optional repository namespace.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.0.split_once('/').map(|(namespace, _)| namespace)
    }

    /// Returns the repository name without its optional namespace.
    #[must_use]
    pub fn name(&self) -> &str {
        match self.0.split_once('/') {
            Some((_, name)) => name,
            None => &self.0,
        }
    }
}

impl AsRef<str> for RepositoryId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for RepositoryId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RepositoryId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Combines a repository kind with its validated identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepositorySpec {
    kind: RepositoryKind,
    id: RepositoryId,
}

impl RepositorySpec {
    /// Creates a repository specification from a kind and identifier.
    #[must_use]
    pub const fn new(kind: RepositoryKind, id: RepositoryId) -> Self {
        Self { kind, id }
    }

    /// Creates a model repository specification.
    #[must_use]
    pub const fn model(id: RepositoryId) -> Self {
        Self::new(RepositoryKind::Model, id)
    }

    /// Creates a dataset repository specification.
    #[must_use]
    pub const fn dataset(id: RepositoryId) -> Self {
        Self::new(RepositoryKind::Dataset, id)
    }

    /// Creates a Space repository specification.
    #[must_use]
    pub const fn space(id: RepositoryId) -> Self {
        Self::new(RepositoryKind::Space, id)
    }

    /// Returns the repository kind.
    #[must_use]
    pub const fn kind(&self) -> RepositoryKind {
        self.kind
    }

    /// Returns the repository identifier.
    #[must_use]
    pub const fn id(&self) -> &RepositoryId {
        &self.id
    }
}

fn validate_repository_id(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::Empty,
        ));
    }
    if value.contains('\0') {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::ContainsNul,
        ));
    }
    if value.len() > MAX_REPOSITORY_ID_LENGTH {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::TooLong,
        ));
    }
    let has_git_suffix = value
        .get(value.len().saturating_sub(4)..)
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".git"));
    if value.contains("--") || value.contains("..") || has_git_suffix {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::Malformed,
        ));
    }

    let mut components = value.split('/');
    let first = components.next();
    let second = components.next();
    if first.is_none() || components.next().is_some() {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::Malformed,
        ));
    }

    for component in first.into_iter().chain(second) {
        validate_repository_component(component)?;
    }

    Ok(())
}

fn validate_repository_component(component: &str) -> Result<(), ValidationError> {
    if component.is_empty() || component.starts_with(['-', '.']) || component.ends_with(['-', '.'])
    {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::Malformed,
        ));
    }

    if !component
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ValidationError::new(
            "repository identifier",
            ValidationErrorKind::InvalidCharacter,
        ));
    }

    Ok(())
}
