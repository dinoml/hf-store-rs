//! Public contract tests for repository kinds.

use hf_store::RepositoryKind;

#[test]
fn repository_kinds_have_stable_diagnostic_names() {
    assert_eq!(RepositoryKind::Model.to_string(), "model");
    assert_eq!(RepositoryKind::Dataset.to_string(), "dataset");
    assert_eq!(RepositoryKind::Space.to_string(), "space");
}

#[test]
fn repository_kind_is_safe_to_share_between_workers() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RepositoryKind>();
}
