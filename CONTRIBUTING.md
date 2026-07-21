# Contributing

Thank you for helping build `hf-store-rs`.

## Before changing code

1. Read [ADR 0001](adr/0001-hub-transport-and-cache-boundary.md) and the active
   [v0.1 RFC](rfcs/0001-hub-store-v0.1.md).
2. Open or link an issue for behavior changes. Public API and on-disk format
   changes must update the RFC or add an ADR.
3. Keep support claims narrower than the tested behavior.

## Development rules

- Prefer behavior-first integration tests under each crate's `tests/` folder.
- Run network tests against a deterministic local HTTP server. The default test
  suite must not require Hub credentials or public network access.
- Keep filesystem, network, clock, and cancellation behavior mockable.
- Never log or persist access tokens.
- Treat repository metadata and paths as untrusted input.
- Do not require symlinks for correct cache behavior on Windows.
- Do not use `unwrap`, `expect`, or panics in fallible library paths.
- Document every public item and every observable error condition.

## Required checks

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Also run the test suite on Windows when changing paths, locking, replacement,
or snapshot activation.

## Pull requests

Keep pull requests bounded to one contract or delivery phase. Include the
observable behavior, failure modes, tests, and any on-disk compatibility impact
in the description.
