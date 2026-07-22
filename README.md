# hf-store-rs

[![CI](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml)

`hf-store-rs` is the planned Rust-native storage boundary for Hugging Face Hub
repositories. It will resolve revisions, download and validate files, and expose
immutable local snapshots through a cross-platform cache.

Its primary integration surface is a typed Rust library for in-process runtimes
and user interfaces, including future DinoML consumers. Those consumers can
reuse existing `huggingface_hub` downloads or fetch missing repository content
without understanding cache internals. The CLI is a thin adapter over the same
library contracts, not the application integration boundary.

> [!IMPORTANT]
> This repository is pre-alpha. Version `0.0.0` does not yet provide a
> transport-backed public fetch API or a stability guarantee. Its private
> shared-cache adapter is conformance-tested specifically against
> `huggingface_hub` v1.24.0 at commit
> `36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`; compatibility with other versions
> is not implied.

## Intended scope

- Model, dataset, and Space repositories.
- Branch, tag, commit, and pull-request revisions.
- Authentication and gated repositories without persisting secrets.
- Concurrent and resumable downloads with range validation.
- ETag and content-hash validation.
- Allow and ignore filters.
- Offline and local-files-only operation.
- Content-addressed blobs and immutable, atomically activated snapshots.
- Conformance-tested sharing with the canonical `huggingface_hub` cache.
- Independent `local_dir`-style materialization with private completion metadata.
- Cache inspection and safe garbage collection.
- Progress, cancellation, custom endpoints, and symlink-free Windows behavior.

Application-managed installation, package activation, artifact selection, model
loading, device placement, and execution remain outside this repository.

## Design status

The repository boundary is recorded in
[ADR 0001](adr/0001-hub-transport-and-cache-boundary.md). Cache identity,
interoperability, and publication decisions are recorded in
[ADR 0002](adr/0002-cache-identity-and-format.md) and
[ADR 0003](adr/0003-cache-publication-and-coordination.md). The accepted v0.1
contract, invariants, and delivery phases live in
[RFC 0001](rfcs/0001-hub-store-v0.1.md). Progress is tracked in
[issue #1](https://github.com/dinoml/hf-store-rs/issues/1).

The library-first downstream contract is accepted in
[ADR 0009](adr/0009-library-first-integration.md). Exact public type names remain
unstable until their phase gates pass, but online acquisition and transport-free
offline lookup will be distinct capabilities returning validated, lease-backed
snapshot and file handles. See the [ADR index](adr/README.md) for all accepted
decisions.

## Development

The workspace uses Rust 2024 and supports Rust 1.85 or newer.

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --all-features --no-deps
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing a public contract. The
accepted scope and phased plan are maintained in
[RFC 0001](rfcs/0001-hub-store-v0.1.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
