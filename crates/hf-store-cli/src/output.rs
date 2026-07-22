use std::io::{self, Write};
use std::process::ExitCode;

use hf_store::HubError;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutputFormat {
    Human,
    Json,
}

#[derive(Debug)]
pub(crate) struct CommandOutcome {
    pub(crate) command: &'static str,
    pub(crate) status: &'static str,
    pub(crate) classification: &'static str,
    pub(crate) code: u8,
    pub(crate) human: String,
    pub(crate) result: Value,
}

#[derive(Serialize)]
struct Envelope<'a> {
    schema: &'static str,
    version: u32,
    command: &'a str,
    status: &'a str,
    classification: &'a str,
    exit_code: u8,
    result: Option<&'a Value>,
    error: Option<SafeError<'a>>,
}

#[derive(Serialize)]
struct SafeError<'a> {
    classification: &'a str,
    message: &'a str,
}

pub(crate) fn emit(format: OutputFormat, outcome: &CommandOutcome) -> ExitCode {
    let write = match format {
        OutputFormat::Human => write_stdout(outcome.human.as_bytes()),
        OutputFormat::Json => serde_json::to_vec(&Envelope {
            schema: "hf-store.cli.output",
            version: 1,
            command: outcome.command,
            status: outcome.status,
            classification: outcome.classification,
            exit_code: outcome.code,
            result: Some(&outcome.result),
            error: None,
        })
        .map_err(io::Error::other)
        .and_then(|bytes| write_stdout(&bytes)),
    };
    if write.is_err() {
        ExitCode::from(10)
    } else {
        ExitCode::from(outcome.code)
    }
}

pub(crate) fn emit_error(
    format: OutputFormat,
    command: &'static str,
    error: &HubError,
) -> ExitCode {
    let (classification, code) = classify_error(error);
    let message = error.to_string();
    let write = match format {
        OutputFormat::Human => write_stderr(message.as_bytes()),
        OutputFormat::Json => serde_json::to_vec(&Envelope {
            schema: "hf-store.cli.output",
            version: 1,
            command,
            status: "error",
            classification,
            exit_code: code,
            result: None,
            error: Some(SafeError {
                classification,
                message: &message,
            }),
        })
        .map_err(io::Error::other)
        .and_then(|bytes| write_stdout(&bytes)),
    };
    if write.is_err() {
        ExitCode::from(10)
    } else {
        ExitCode::from(code)
    }
}

pub(crate) fn usage(message: &str) -> ExitCode {
    let _result = write_stderr(message.as_bytes());
    ExitCode::from(2)
}

fn classify_error(error: &HubError) -> (&'static str, u8) {
    if error.is_cancelled() {
        ("cancelled", 11)
    } else if error.is_authentication() || error.is_gated() {
        ("access", 4)
    } else if error.is_cache_incomplete() || error.is_backend_unavailable() {
        ("offline-miss", 3)
    } else if error.is_missing() {
        ("not-found", 5)
    } else if error.is_transport() || error.is_rate_limited() || error.is_protocol() {
        ("transport", 6)
    } else if error.is_validation() || error.is_cache_corrupt() || error.is_cache_unsupported() {
        ("validation", 7)
    } else if error.is_cache_busy() {
        ("busy", 9)
    } else if error.is_cache() {
        ("io", 10)
    } else {
        ("internal", 70)
    }
}

fn write_stdout(bytes: &[u8]) -> io::Result<()> {
    let mut output = io::stdout().lock();
    output.write_all(bytes)?;
    output.write_all(b"\n")?;
    output.flush()
}

fn write_stderr(bytes: &[u8]) -> io::Result<()> {
    let mut output = io::stderr().lock();
    output.write_all(bytes)?;
    output.write_all(b"\n")?;
    output.flush()
}
