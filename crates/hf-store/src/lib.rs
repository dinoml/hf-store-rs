//! Rust-native storage primitives for Hugging Face Hub repositories.
//!
//! This pre-alpha crate currently exposes repository identity vocabulary only.
//! It does not yet perform network or filesystem operations.
//!
//! # Examples
//!
//! ```
//! use hf_store::RepositoryKind;
//!
//! assert_eq!(RepositoryKind::Model.to_string(), "model");
//! ```

mod repo;

#[doc(inline)]
pub use repo::RepositoryKind;
