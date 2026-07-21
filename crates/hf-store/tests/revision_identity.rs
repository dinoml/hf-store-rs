//! Public contract tests for symbolic and immutable revisions.

use std::str::FromStr;

use hf_store::{CommitId, Revision};

#[test]
fn revisions_are_opaque_and_allow_slashes() -> Result<(), Box<dyn std::error::Error>> {
    for value in [
        "main",
        "release-v1",
        "feature/cache-v2",
        "refs/pr/17",
        "refs/convert/parquet",
    ] {
        let revision = Revision::parse(value)?;

        assert_eq!(revision.as_str(), value);
        assert_eq!(Revision::from_str(value)?, revision);
        assert_eq!(revision.to_string(), value);
    }

    Ok(())
}

#[test]
fn revisions_reject_only_values_that_cannot_be_safe_logical_identities() {
    Revision::parse("").expect_err("empty revision must be rejected");
    Revision::parse("refs/pr/1\0secret").expect_err("NUL revision must be rejected");
}

#[test]
fn commit_ids_require_a_full_lowercase_hub_commit() -> Result<(), Box<dyn std::error::Error>> {
    let value = "0123456789abcdef0123456789abcdef01234567";
    let commit = CommitId::parse(value)?;

    assert_eq!(commit.as_str(), value);
    assert_eq!(commit.to_string(), value);
    assert_eq!(CommitId::from_str(value)?, commit);

    for invalid in [
        "0123456789abcdef",
        "0123456789ABCDEF0123456789ABCDEF01234567",
        "g123456789abcdef0123456789abcdef01234567",
        "0123456789abcdef0123456789abcdef012345678",
    ] {
        CommitId::parse(invalid)
            .expect_err(&format!("accepted malformed commit identifier {invalid:?}"));
    }

    Ok(())
}

#[test]
fn revision_types_are_safe_to_share_between_workers() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<Revision>();
    assert_send_sync::<CommitId>();
}
