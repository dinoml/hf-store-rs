//! Thin command-line adapter for the `hf-store` library.
#![allow(
    clippy::multiple_crate_versions,
    reason = "CLI and capability dependencies have independently pinned transitive platforms"
)]

mod cli;
mod config;
mod output;

use std::process::ExitCode;

fn main() -> ExitCode {
    cli::run(std::env::args_os())
}
