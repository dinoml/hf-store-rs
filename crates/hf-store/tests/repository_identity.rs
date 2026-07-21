//! Public contract tests for repository identities.

use std::str::FromStr;

use hf_store::{RepositoryId, RepositoryKind, RepositorySpec};

#[test]
fn repository_ids_round_trip_without_changing_spelling() -> Result<(), Box<dyn std::error::Error>> {
    for value in [
        "foo",
        "org/model",
        "123",
        "Foo-BAR_foo.bar123",
        "_leading_underscore",
    ] {
        let id = RepositoryId::parse(value)?;
        let displayed = id.to_string();

        assert_eq!(id.as_str(), value);
        assert_eq!(displayed, value);
        assert_eq!(RepositoryId::from_str(&displayed)?, id);
    }

    Ok(())
}

#[test]
fn repository_ids_reject_malformed_values_without_echoing_them() {
    let invalid = [
        "",
        "org\0secret",
        "org/repo/extra",
        ".repo",
        "repo.",
        "-repo",
        "repo-",
        "foo--bar",
        "foo..bar",
        "foo.git",
        "has space",
        "org\\repo",
    ];

    for value in invalid {
        let error = RepositoryId::parse(value).expect_err("value must be rejected");

        if !value.is_empty() {
            assert!(!error.to_string().contains(value), "error echoed {value:?}");
        }
    }

    let too_long = "a".repeat(97);
    RepositoryId::parse(&too_long).expect_err("overlong identifier must be rejected");
}

#[test]
fn repository_specs_keep_kind_separate_from_identifier() -> Result<(), Box<dyn std::error::Error>> {
    let id = RepositoryId::parse("org/shared-name")?;
    let model = RepositorySpec::model(id.clone());
    let dataset = RepositorySpec::dataset(id.clone());
    let space = RepositorySpec::space(id);

    assert_eq!(model.kind(), RepositoryKind::Model);
    assert_eq!(dataset.kind(), RepositoryKind::Dataset);
    assert_eq!(space.kind(), RepositoryKind::Space);
    assert_eq!(model.id().as_str(), "org/shared-name");
    assert_ne!(model, dataset);
    assert_ne!(dataset, space);

    Ok(())
}

#[test]
fn repository_identity_types_are_safe_to_share_between_workers() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RepositoryId>();
    assert_send_sync::<RepositorySpec>();
}
