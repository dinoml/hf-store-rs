use std::fmt::{self, Display, Formatter};

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
