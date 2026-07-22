use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use crate::validation::{ValidationError, ValidationErrorKind};

const MAX_COMPONENT_CODE_UNITS: usize = 255;
const MAX_COMPONENT_UTF8_BYTES: usize = 255;

/// Identifies a portable POSIX-style file path inside a repository.
///
/// A `RepoPath` has no absolute, parent, current-directory, empty, or
/// platform-specific components and can therefore be mapped beneath a trusted
/// cache directory without interpreting untrusted host path syntax.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoPath(Box<str>);

impl RepoPath {
    /// Parses and validates a portable repository path.
    ///
    /// # Errors
    ///
    /// Returns an error for traversal, absolute paths, empty components,
    /// backslashes, control characters, Windows device names, alternate data
    /// streams, overlong components, trailing dots or spaces, and other
    /// non-portable punctuation.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        validate_repo_path(value)?;
        Ok(Self(value.into()))
    }

    /// Returns the normalized POSIX-style repository path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RepoPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for RepoPath {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RepoPath {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

fn validate_repo_path(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(path_error(ValidationErrorKind::Empty));
    }
    if value.contains('\0') {
        return Err(path_error(ValidationErrorKind::ContainsNul));
    }
    if value.starts_with('/') || value.contains('\\') {
        return Err(path_error(ValidationErrorKind::UnsafePath));
    }

    for component in value.split('/') {
        validate_path_component(component)?;
    }

    Ok(())
}

fn validate_path_component(component: &str) -> Result<(), ValidationError> {
    if component.is_empty()
        || matches!(component, "." | "..")
        || component.ends_with('.')
        || component.ends_with(' ')
        || component.len() > MAX_COMPONENT_UTF8_BYTES
        || component.encode_utf16().count() > MAX_COMPONENT_CODE_UNITS
    {
        return Err(path_error(ValidationErrorKind::UnsafePath));
    }

    if component.chars().any(|character| {
        character.is_control() || matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*')
    }) {
        return Err(path_error(ValidationErrorKind::UnsafePath));
    }

    if is_windows_reserved_name(component) {
        return Err(path_error(ValidationErrorKind::UnsafePath));
    }

    Ok(())
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = match component.split_once('.') {
        Some((stem, _)) => stem,
        None => component,
    };
    let uppercase = stem.to_ascii_uppercase();

    if matches!(
        uppercase.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
    ) {
        return true;
    }

    let suffix = uppercase
        .strip_prefix("COM")
        .or_else(|| uppercase.strip_prefix("LPT"));
    suffix.is_some_and(|suffix| {
        matches!(
            suffix,
            "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "\u{b9}" | "\u{b2}" | "\u{b3}"
        )
    })
}

fn path_error(kind: ValidationErrorKind) -> ValidationError {
    ValidationError::new("repository path", kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_untrusted_paths_never_escape_or_leak_diagnostics() {
        let alphabet = b"abcXYZ019./\\:*? <>\0\r\n";
        let mut state = 0x6a09_e667_f3bc_c909_u64;
        for _case in 0..10_000 {
            let length = usize::try_from(state & 31).unwrap_or(0);
            let mut value = String::with_capacity(length);
            for _index in 0..length {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let generated = state.to_le_bytes()[0];
                value.push(char::from(alphabet[usize::from(generated) % alphabet.len()]));
            }
            match RepoPath::parse(&value) {
                Ok(path) => {
                    assert_eq!(path.as_str(), value);
                    assert!(!path.as_str().starts_with('/'));
                    assert!(!path.as_str().contains(['\\', '\0']));
                    assert!(path.as_str().split('/').all(|component| {
                        !component.is_empty() && !matches!(component, "." | "..")
                    }));
                }
                Err(error) => {
                    assert!(!error.to_string().contains(&value) || value.is_empty());
                    assert!(!format!("{error:?}").contains(&value) || value.is_empty());
                }
            }
        }
    }
}
