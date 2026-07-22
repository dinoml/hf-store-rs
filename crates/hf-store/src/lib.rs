//! Rust-native storage primitives for Hugging Face Hub repositories.
//!
//! This crate exposes validated repository identity, online Hub
//! acquisition, Python-compatible and owned cache views, transport-free offline
//! lookup, and independent `local_dir` materialization.
//!
//! # Examples
//!
#![allow(
    clippy::multiple_crate_versions,
    reason = "the Rustls/Reqwest graph and cache primitives currently require parallel transitive versions"
)]
//! ```
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
mod gc;
#[allow(
    dead_code,
    reason = "private Hub protocol implementation precedes the public request API"
)]
mod hub_protocol;
mod progress;
mod repo;
mod repo_path;
mod report;
#[cfg(feature = "network")]
mod reqwest_transport;
mod revision;
mod snapshot;
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
pub use api::{CacheMode, FetchOptions, FetchRequest, HubStore, HubStoreBuilder, OfflineStore};
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
pub use gc::{
    GcBlocker, GcBlockerKind, GcCandidate, GcCandidateKind, GcCandidateReason, GcExecutionReport,
    GcPlan, GcPolicy,
};
#[doc(inline)]
pub use progress::{ProgressEvent, ProgressObserver, ProgressPhase, ReuseDecision};
#[doc(inline)]
pub use report::{CacheInventoryReport, InventoryEntry, InventoryState};
#[doc(inline)]
pub use report::{
    InspectedFile, InspectionFinding, InspectionReport, InspectionState, VerificationFinding,
    VerificationReport,
};

#[doc(inline)]
pub use repo::{RepositoryId, RepositoryKind, RepositorySpec};
#[doc(inline)]
pub use repo_path::RepoPath;
#[doc(inline)]
pub use revision::{CommitId, Revision};
#[doc(inline)]
pub use snapshot::{LocalDirectory, LocalDirectoryFile, Snapshot, SnapshotFile, SnapshotFileForm};
#[doc(inline)]
pub use validation::ValidationError;
