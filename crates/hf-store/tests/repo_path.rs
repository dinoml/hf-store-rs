//! Public contract tests for portable repository paths.

use std::str::FromStr;

use hf_store::{RepoPath, ValidationError};

#[test]
fn repository_paths_round_trip_as_portable_posix_paths() -> Result<(), Box<dyn std::error::Error>> {
    for value in [
        "config.json",
        "tokenizer/tokenizer.json",
        "weights/model-00001-of-00002.safetensors",
        "names/a..b.txt",
        "devices/com10.txt",
    ] {
        let path = RepoPath::parse(value)?;

        assert_eq!(path.as_str(), value);
        assert_eq!(RepoPath::from_str(value)?, path);
        assert_eq!(path.to_string(), value);
    }

    Ok(())
}

#[test]
fn repository_paths_reject_cross_platform_unsafe_components() {
    let invalid = [
        "",
        "/absolute",
        "//server/share",
        "C:/drive",
        "C:drive-relative",
        "dir\\file",
        ".",
        "..",
        "dir/./file",
        "dir/../file",
        "dir//file",
        "dir/",
        "CON",
        "con.txt",
        "CONIN$",
        "conout$.txt",
        "CLOCK$",
        "dir/AUX.json",
        "NUL",
        "COM1.bin",
        "COM\u{b9}",
        "com\u{b3}.txt",
        "LPT9",
        "LPT\u{b2}.json",
        "file:stream",
        "trailing.",
        "trailing ",
        "question?.txt",
        "less<than",
        "greater>than",
        "quote\"name",
        "pipe|name",
        "star*name",
        "control\u{001f}name",
        "nul\0name",
    ];

    for value in invalid {
        RepoPath::parse(value).expect_err(&format!("accepted unsafe path {value:?}"));
    }

    let overlong_component = "a".repeat(256);
    RepoPath::parse(&overlong_component).expect_err("accepted a non-portable path component");
}

#[test]
fn repository_path_errors_do_not_echo_untrusted_input() {
    let secret = "private-token\0.json";
    let error = RepoPath::parse(secret).expect_err("NUL must be rejected");

    assert!(!error.to_string().contains(secret));
}

#[test]
fn repository_paths_are_safe_to_share_between_workers() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RepoPath>();
    assert_send_sync::<ValidationError>();
}
