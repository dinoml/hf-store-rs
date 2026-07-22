//! Rust-native storage primitives for Hugging Face Hub repositories.
//!
//! This pre-alpha crate currently exposes validated repository, revision, path,
//! endpoint, and request-time authentication vocabulary. It does not yet expose
//! a Hub transport or cache service.
//!
//! # Examples
//!
//! ```

#![allow(
    clippy::multiple_crate_versions,
    reason = "the Rustls/Reqwest graph and cache primitives currently require parallel transitive versions"
)]
//! use hf_store::RepositoryKind;
//!
//! assert_eq!(RepositoryKind::Model.to_string(), "model");
//! ```

mod api;
mod auth;
#[allow(
    dead_code,
    reason = "the local cache kernel is internal until the HubStore service is introduced"
)]
mod cache;
mod cancellation;
mod endpoint;
#[allow(
    dead_code,
    reason = "private operation errors precede the first public HubStore request API"
)]
mod error;
#[allow(
    dead_code,
    reason = "private fetch plans precede the public request and snapshot APIs"
)]
mod fetch_plan;
#[allow(
    dead_code,
    reason = "private Hub protocol implementation precedes the public request API"
)]
mod hub_protocol;
mod repo;
mod repo_path;
#[cfg(feature = "network")]
mod reqwest_transport;
mod revision;
mod validation;

#[cfg(test)]
mod test_http_fixture;

#[allow(
    dead_code,
    reason = "the private transfer state machine is being completed before acquisition is public"
)]
mod transfer;
#[allow(
    dead_code,
    reason = "private transport seams are being built before the public HubStore service"
)]
mod transport;

#[doc(inline)]
pub use api::{FetchRequest, HubStore, HubStoreBuilder};
#[doc(inline)]
pub use auth::AuthToken;
#[doc(inline)]
pub use cache::SelectionId;
#[doc(inline)]
pub use cancellation::CancellationToken;
#[doc(inline)]
pub use endpoint::Endpoint;
#[doc(inline)]
pub use error::HubOperationError as HubError;
#[doc(inline)]
pub use fetch_plan::{FetchPlan, PlannedFile};

#[doc(inline)]
pub use repo::{RepositoryId, RepositoryKind, RepositorySpec};
#[doc(inline)]
pub use repo_path::RepoPath;
#[doc(inline)]
pub use revision::{CommitId, Revision};
#[doc(inline)]
pub use validation::ValidationError;
