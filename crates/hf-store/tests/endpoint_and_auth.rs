//! Public contract tests for endpoints and request-time authentication.

use std::str::FromStr;

use hf_store::{AuthToken, Endpoint};

#[test]
fn endpoints_have_one_canonical_origin_spelling() -> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        ("HTTPS://HuggingFace.co:443/", "https://huggingface.co"),
        ("http://LOCALHOST:80/", "http://localhost"),
        (
            "https://hub.example.test:8443/api///",
            "https://hub.example.test:8443/api",
        ),
    ];

    for (input, expected) in cases {
        let endpoint = Endpoint::parse(input)?;

        assert_eq!(endpoint.as_str(), expected);
        assert_eq!(endpoint.to_string(), expected);
        assert_eq!(Endpoint::from_str(expected)?, endpoint);
    }

    assert_eq!(Endpoint::hugging_face().as_str(), "https://huggingface.co");

    Ok(())
}

#[test]
fn endpoints_reject_ambiguous_or_secret_bearing_urls() {
    let invalid = [
        "",
        "huggingface.co",
        "ftp://huggingface.co",
        "https://user:super-secret@huggingface.co",
        "https://huggingface.co?token=super-secret",
        "https://huggingface.co/#fragment",
        "https://huggingface.co/\0secret",
    ];

    for value in invalid {
        let error = Endpoint::parse(value).expect_err("endpoint must be rejected");

        assert!(!error.to_string().contains("super-secret"));
        if !value.is_empty() {
            assert!(!error.to_string().contains(value));
        }
    }
}

#[test]
fn authentication_tokens_are_always_redacted() -> Result<(), Box<dyn std::error::Error>> {
    let secret = "hf_super_secret_value";
    let token = AuthToken::new(secret)?;
    let debug = format!("{token:?}");

    assert_eq!(debug, "AuthToken([REDACTED])");
    assert!(!debug.contains(secret));

    let invalid_secret = "hf_private\0suffix";
    let error = AuthToken::new(invalid_secret).expect_err("NUL must be rejected");
    assert!(!error.to_string().contains(invalid_secret));

    Ok(())
}

#[test]
fn endpoint_and_authentication_types_are_safe_to_share_between_workers() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<Endpoint>();
    assert_send_sync::<AuthToken>();
}
