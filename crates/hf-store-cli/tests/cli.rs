//! Black-box contract tests for the `hf-store` executable.

use std::error::Error;
use std::process::{Command, Output};

use tempfile::TempDir;

fn run(arguments: &[&str]) -> Result<Output, Box<dyn Error>> {
    Ok(Command::new(env!("CARGO_BIN_EXE_hf-store"))
        .args(arguments)
        .output()?)
}

#[test]
fn json_inspection_and_verification_emit_one_stable_envelope() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let cache = directory.path().to_string_lossy();
    let inspection = run(&[
        "--format",
        "json",
        "--cache-dir",
        &cache,
        "inspect",
        "--repo-kind",
        "model",
        "org/repo",
    ])?;
    assert!(inspection.status.success());
    let inspection_json: serde_json::Value = serde_json::from_slice(&inspection.stdout)?;
    assert_eq!(inspection_json["schema"], "hf-store.cli.output");
    assert_eq!(inspection_json["command"], "inspect");
    assert_eq!(inspection_json["exit_code"], 0);
    assert_eq!(
        String::from_utf8_lossy(&inspection.stdout).lines().count(),
        1
    );

    let verification = run(&[
        "--format",
        "json",
        "--cache-dir",
        &cache,
        "verify",
        "--repo-kind",
        "model",
        "org/repo",
        "--path",
        "config.json",
    ])?;
    assert_eq!(verification.status.code(), Some(1));
    let verification_json: serde_json::Value = serde_json::from_slice(&verification.stdout)?;
    assert_eq!(verification_json["status"], "findings");
    assert_eq!(verification_json["classification"], "findings");
    assert_eq!(verification_json["result"]["valid"], false);
    Ok(())
}

#[test]
fn gc_plan_is_create_new_and_execute_requires_confirmation() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let cache = directory.path().join("cache");
    std::fs::create_dir(&cache)?;
    let plan = directory.path().join("plan.json");
    let cache_text = cache.to_string_lossy();
    let plan_text = plan.to_string_lossy();
    let planned = run(&[
        "--format",
        "json",
        "--cache-mode",
        "owned",
        "--cache-dir",
        &cache_text,
        "gc",
        "plan",
        "--repo-kind",
        "model",
        "org/repo",
        "--output",
        &plan_text,
    ])?;
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    assert!(plan.is_file());
    let repeated = run(&[
        "--cache-mode",
        "owned",
        "--cache-dir",
        &cache_text,
        "gc",
        "plan",
        "--repo-kind",
        "model",
        "org/repo",
        "--output",
        &plan_text,
    ])?;
    assert_eq!(repeated.status.code(), Some(2));
    let unconfirmed = run(&[
        "--cache-mode",
        "owned",
        "--cache-dir",
        &cache_text,
        "gc",
        "execute",
        "--repo-kind",
        "model",
        "org/repo",
        "--plan",
        &plan_text,
    ])?;
    assert_eq!(unconfirmed.status.code(), Some(2));
    Ok(())
}

#[test]
fn raw_token_argument_is_absent_and_secret_value_is_not_echoed() -> Result<(), Box<dyn Error>> {
    let secret = "hf_secret_cli_argument_sentinel";
    let output = run(&[
        "fetch",
        "--repo-kind",
        "model",
        "org/repo",
        "--token",
        secret,
    ])?;
    assert_eq!(output.status.code(), Some(2));
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!rendered.contains(secret));
    Ok(())
}
