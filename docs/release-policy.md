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
5. Run `cargo package` for packages intended to remain publishable.
6. Create the signed or annotated `vX.Y.Z` tag and a GitHub source release from
   the matching changelog entry.

Releases are built only from a green `main` commit. Package contents must not
include credentials, generated caches, local plans, or build output.

Publishing to a Cargo registry is an optional distribution step and is not a
requirement for a supported release. If registry publication is requested in
the future, validate it separately with `cargo publish --dry-run` and publish
`hf-store` before `hf-store-cli`.
