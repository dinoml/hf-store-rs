# Implementation handoff

## Current state

The repository bootstrap provides an Apache-2.0 Rust 2024 workspace, strict
workspace lints, three-platform CI, contribution and security guidance, a single
small repository-kind value type, an accepted boundary ADR, and a proposed v0.1
RFC.

Version `0.0.0` is intentionally non-publishable. No Hub, network, filesystem,
cache, authentication, or download behavior is implemented or claimed.

Tracking issue: [Repository: hf-store-rs](https://github.com/dinoml/hf-store-rs/issues/1)

## Next bounded objective

Complete Phase 0 and then implement only Phase 1 from RFC 0001: validated domain
types plus a crash-safe local cache kernel. Do not implement HTTP or claim remote
Hub support in that session.

Before coding, resolve and record the Phase 1 decisions that affect permanent
disk state: cache compatibility, blob digest, path mapping, materialization,
locking, atomic replacement, and durability. A proposed layout in the RFC is not
an accepted format.

## Required first-session deliverables

1. Add validated `RepositoryId`, `RepositorySpec`, `Revision`, `CommitId`,
   `RepoPath`, `Endpoint`, and redacted `AuthToken` types.
2. Add an internal versioned cache layout without exposing raw identities as host
   path components.
3. Add private versioned metadata records for origin, repository, ref, remote
   tree, partial transfer state, and snapshot manifest.
4. Add deterministic selection identity over a canonical sorted file list.
5. Add same-filesystem staged writes and atomic publication primitives.
6. Add content-addressed blob publication from a sans-I/O reader while computing
   and validating size and digest.
7. Make filesystem, clock, operation identity, and publication failure points
   deterministic in tests.
8. Record accepted design choices as ADRs and keep RFC 0001 current.

## Required tests

- Identity round trips, empty and NUL rejection, and slash-containing revisions.
- Repository, revision, and endpoint key separation.
- Traversal, absolute, UNC, drive, backslash, alternate-stream,
  Windows-reserved-name, trailing-dot/space, and case-collision paths.
- Deterministic selection identity independent of input ordering.
- Unknown metadata-version and corrupt-record rejection.
- `AuthToken` redaction in `Debug` and errors.
- Atomic file and ref publication under injected failures.
- Size and digest mismatch rejection.
- Competing blob publishers converging on one validated blob.
- No staging or partial data visible through normal lookup.
- Public value types remaining `Send + Sync` where appropriate.

## Constraints

- Do not implement HTTP, Hub API calls, downloads, or GC execution in Phase 1.
- Do not add a fake `fetch` API or TODO-only public methods.
- Do not expose third-party HTTP, runtime, URL, glob, secrecy, serialization,
  temporary-file, hashing, or locking types in public APIs.
- Do not derive `Debug` for secrets.
- Do not use raw IDs or revisions as cache directory names.
- Do not use `unwrap`, `expect`, panics, unsafe code, or mutable global state in
  production paths.
- Keep integration tests hermetic and under `tests/` when they exercise only the
  public API.
- Preserve LF normalization through `.gitattributes`.

## Verification

```text
cargo fmt --all --check
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

Run cache, path, locking, and replacement behavior on Windows, Linux, and macOS.

## Copy-paste prompt for the next Codex project

```text
Work in H:\hf-store-rs.

Implement the first bounded hf-store-rs v0.1 slice: finish Phase 0 decisions and
then implement Phase 1, validated domain types plus the crash-safe local cache
kernel. Do not implement HTTP, Hub API calls, downloads, snapshot GC execution,
or claim remote support in this session.

First read AGENTS.md, README.md, CONTRIBUTING.md,
rfcs/0001-hub-store-v0.1.md,
adr/0001-hub-transport-and-cache-boundary.md,
docs/implementation-handoff.md, and the linked GitHub tracking issue.

Confirm the branch, worktree, Rust/MSRV, and scaffold. Preserve user changes.
Resolve the RFC decisions that affect Phase 1 disk compatibility and record each
accepted choice as an ADR before treating it as stable.

Implement the deliverables and tests listed in docs/implementation-handoff.md.
Keep effects mockable, tests hermetic, secrets redacted, cache paths derived from
validated fixed-size keys, snapshots immutable, and publication atomic. Do not
leak implementation dependencies through public APIs or overstate support.

Run every verification command from AGENTS.md. Report changed files, checks,
remaining risks, and the exact next phase. Do not push or open a pull request
unless explicitly asked.
```
