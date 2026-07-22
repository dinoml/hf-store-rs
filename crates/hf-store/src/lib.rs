//! Rust-native storage primitives for Hugging Face Hub repositories.
//!
//! This pre-alpha crate currently exposes validated repository, revision, path,
//! endpoint, and request-time authentication vocabulary. It does not yet expose
//! a Hub transport or cache service.
//!
//! # Examples
//!
//! ```
//! use hf_store::RepositoryKind;
//!
//! assert_eq!(RepositoryKind::Model.to_string(), "model");
//! ```

mod auth;
#[allow(
    dead_code,
    reason = "the local cache kernel is internal until the HubStore service is introduced"
)]
mod cache;
mod endpoint;
#[allow(
    dead_code,
    reason = "private operation errors precede the first public HubStore request API"
)]
mod error;
mod repo;
mod repo_path;
mod revision;
mod validation;

#[cfg(test)]
mod test_http_fixture;

#[allow(
    dead_code,
    reason = "private transport seams are being built before the public HubStore service"
)]
mod transport;

#[doc(inline)]
pub use auth::AuthToken;
#[doc(inline)]
pub use endpoint::Endpoint;

#[doc(inline)]
pub use repo::{RepositoryId, RepositoryKind, RepositorySpec};
#[doc(inline)]
pub use repo_path::RepoPath;
#[doc(inline)]
pub use revision::{CommitId, Revision};
#[doc(inline)]
pub use validation::ValidationError;
