# hf-store-rs

[![CI](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml)

`hf-store-rs` is the planned Rust-native storage boundary for Hugging Face Hub
repositories. It will resolve revisions, download and validate files, and expose
immutable local snapshots through a cross-platform cache.

> [!IMPORTANT]
> This repository is a pre-alpha bootstrap. Version `0.0.0` does not yet fetch
> repositories and makes no compatibility or stability claim.

## Intended scope

- Model, dataset, and Space repositories.
- Branch, tag, commit, and pull-request revisions.
- Authentication and gated repositories without persisting secrets.
- Concurrent and resumable downloads with range validation.
- ETag and content-hash validation.
- Allow and ignore filters.
- Offline and local-files-only operation.
- Content-addressed blobs and immutable, atomically activated snapshots.
- Cache inspection and safe garbage collection.
- Progress, cancellation, custom endpoints, and symlink-free Windows behavior.

Application-managed installation, package activation, artifact selection, model
loading, device placement, and execution remain outside this repository.

## Design status

The repository boundary is recorded in
[ADR 0001](adr/0001-hub-transport-and-cache-boundary.md). The proposed v0.1
contract, invariants, open decisions, and delivery phases live in
[RFC 0001](rfcs/0001-hub-store-v0.1.md). Progress is tracked in
[issue #1](https://github.com/dinoml/hf-store-rs/issues/1).

The API below is a design target, not implemented or stable:

```rust,ignore
let snapshot = HubStore::new(cache_dir)
    .model("org/model")
    .revision("commit-or-tag")
    .allow(["*.json", "*.safetensors", "*.model"])
    .fetch()?;
```

## Development

The workspace uses Rust 2024 and supports Rust 1.85 or newer.

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --all-features --no-deps
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing a public contract. The
next implementation session should begin with the
[implementation handoff](docs/implementation-handoff.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
