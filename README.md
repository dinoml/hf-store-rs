# hf-store-rs

[![CI](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/dinoml/hf-store-rs/actions/workflows/ci.yml)

`hf-store-rs` is a Rust-native storage boundary for Hugging Face Hub
repositories. It resolves revisions, downloads and validates files, and exposes
immutable local snapshots through a cross-platform cache.

Its primary integration surface is a typed Rust library for in-process runtimes
and user interfaces, including future DinoML consumers. Those consumers can
reuse existing `huggingface_hub` downloads or fetch missing repository content
without understanding cache internals. The CLI is a thin adapter over the same
library contracts, not the application integration boundary.

> [!IMPORTANT]
> Version `0.1` is the first supported library and CLI contract. Its
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

The cross-platform publication guarantee is atomic visibility, not general
power-loss durability. The tested capability matrix and exact boundary are in
[Platform filesystem capabilities](docs/platform-capabilities.md).

The library-first downstream contract is accepted in
[ADR 0009](adr/0009-library-first-integration.md). Exact public type names remain
distinct between online acquisition and transport-free offline lookup. Both
return validated, lease-backed snapshot and file handles. See the
[ADR index](adr/README.md) for all accepted decisions.

## Library usage

The [full usage guide](https://github.com/dinoml/hf-store-rs/blob/main/docs/usage.md)
covers dependency setup, cache selection,
authentication, filtering, online and offline flows, `local_dir`, progress,
cancellation, error handling, maintenance, DinoML-style integration, and CLI
automation. The examples below are the shortest path to a snapshot.

Online operations are async and run on the caller's entered Tokio runtime. The
library never discovers credentials; pass an `AuthToken` on the request that
needs it.

```rust,no_run
use hf_store::{
    AuthToken, CacheMode, FetchOptions, FetchRequest, HubStore, RepoPath,
    RepositoryId, RepositorySpec, Revision,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai/gpt-oss-20b")?),
    Revision::parse("main")?,
)
.allow_patterns(["*.json", "*.safetensors"])
.authorization(AuthToken::new("request-time-token")?);

let store = HubStore::builder()
    .cache_mode(CacheMode::Compatible)
    .cache_root("/explicit/huggingface/hub")
    .max_concurrent_downloads(8)
    .build();
let snapshot = store.fetch(request, FetchOptions::default()).await?;

let config = RepoPath::parse("config.json")?;
let config_file = snapshot.file(&config).ok_or("config was not selected")?;
// Retain `snapshot` while downstream code uses this path; it owns the GC lease.
println!("{}", config_file.local_path().display());
# Ok(())
# }
```

Strict offline lookup is synchronous and remains available with
`--no-default-features`. It cannot construct a network transport.

```rust,no_run
use hf_store::{
    CacheMode, FetchRequest, OfflineStore, RepositoryId, RepositorySpec, Revision,
};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai/gpt-oss-20b")?),
    Revision::parse("main")?,
)
.allow_patterns(["*.json", "*.safetensors"]);
let offline = OfflineStore::new("/explicit/huggingface/hub")
    .cache_mode(CacheMode::Compatible);
let snapshot = offline.open_request(&request)?;
println!("{}", snapshot.directory().display());
# Ok(())
# }
```

For a caller-owned mutable directory, use `HubStore::fetch_to_local_dir` online
or `OfflineStore::materialize_request_to_local_dir` from a complete cache. The
result is an independent copy and is not an immutable cache snapshot.

## CLI

The `hf-store` executable is a thin adapter over the library. Examples:

```text
hf-store fetch --repo-kind model openai/gpt-oss-20b --allow "*.json" --allow "*.safetensors"
hf-store fetch --offline --repo-kind model openai/gpt-oss-20b --allow "*.safetensors"
hf-store inspect --repo-kind model openai/gpt-oss-20b --format json
hf-store verify --repo-kind model openai/gpt-oss-20b --path config.json --format json
hf-store gc plan --repo-kind model openai/gpt-oss-20b --output gc-plan.json
hf-store gc execute --repo-kind model openai/gpt-oss-20b --plan gc-plan.json --yes
```

The CLI supports explicit `--cache-dir`, `--cache-mode`, `--endpoint`, and
credential-free `--proxy` values. Only online fetch discovers ambient tokens,
using the precedence fixed by ADR 0008. There is intentionally no raw `--token`
argument.

Detailed operational contracts are in
[the full usage guide](https://github.com/dinoml/hf-store-rs/blob/main/docs/usage.md),
[cache modes, local directories, and offline guarantees](docs/cache-and-offline.md),
[the security policy](SECURITY.md), and [the release policy](docs/release-policy.md).

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
