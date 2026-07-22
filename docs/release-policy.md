# Release policy

## Versioning

The Rust library, serialized report schemas, executable GC plan schema, and CLI
automation envelope are versioned contracts. Semantic Versioning applies to the
Rust and CLI APIs. On-disk metadata, reports, and plans carry their own explicit
schema versions and fail closed when unsupported.

Before 1.0, breaking public API changes require a minor version increase and a
changelog entry. Patch releases may add non-exhaustive classifications and
fix behavior without weakening validation, cache containment, or credential
redaction.

## Release checklist

1. Update `CHANGELOG.md` and package versions.
2. Run formatting, strict all-feature clippy, all-feature tests, warning-free
   rustdoc, cache-only tests, and Rust 1.85 checks.
3. Run pinned Python conformance and cache/locking/replacement tests on Linux,
   macOS, and Windows.
4. Run generated parser/state properties and the credential/signed-URL audit.
5. Run `cargo package` and `cargo publish --dry-run` for each published package.
6. Publish `hf-store` before `hf-store-cli`, create the signed or annotated
   `vX.Y.Z` tag, and create a GitHub release from the matching changelog entry.

Releases are built only from a green `main` commit. Package contents must not
include credentials, generated caches, local plans, or build output.

