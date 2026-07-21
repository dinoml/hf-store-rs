# Agent instructions

These instructions apply to the entire repository.

## Start here

Read these files before implementation work:

1. `docs/implementation-handoff.md`
2. `rfcs/0001-hub-store-v0.1.md`
3. `adr/0001-hub-transport-and-cache-boundary.md`
4. [GitHub tracking issue #1](https://github.com/dinoml/hf-store-rs/issues/1)

The RFC is proposed. Resolve its blocking decisions before stabilizing a public
API or on-disk layout. Record accepted choices as ADRs.

## Boundary

This repository owns Hub repository identity, revision resolution,
authentication at request time, transfer, validation, caching, immutable
snapshots, offline lookup, inspection, and garbage collection.

It does not own DinoML package installation, artifact selection, model loading,
device execution, application policy, or long-lived credential storage.

## Implementation rules

- Add observable-behavior tests before implementation.
- Keep default tests hermetic; use a deterministic local HTTP fixture.
- Make network, filesystem, clock, and cancellation effects substitutable.
- Preserve immutable-snapshot and atomic-activation invariants.
- Keep Windows correct without symlinks.
- Never emit credentials in logs, errors, cache metadata, or debug output.
- Reject unsafe path components before joining them to cache paths.
- Do not claim Hub-cache compatibility, resumability, offline completeness, or
  backend support until the corresponding conformance tests pass.
- Avoid `unwrap`, `expect`, global mutable state, and hidden ambient config in
  production library code.
- Keep public types documented, `Debug`, and `Send` where applicable.

## Verification

Run all four checks before handing work off:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

For cache, locking, replacement, or path changes, also run tests on Windows,
Linux, and macOS.
