## Outcome

Describe the observable behavior this change adds or corrects.

## Contract and compatibility

- Related issue/RFC/ADR:
- Public API impact:
- On-disk format impact:
- Platform-specific impact:

## Verification

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- [ ] `cargo test --workspace --all-features --locked`
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`
- [ ] Windows behavior tested when paths, locking, replacement, or activation changed
- [ ] Support claims are limited to fixture-backed behavior
- [ ] Diagnostics contain no credentials or signed URLs
