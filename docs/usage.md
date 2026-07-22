# Usage guide

This guide covers the supported library and command-line workflows. The Rust
library is the primary integration surface; the CLI is a thin adapter for shell
use and automation.

## Add the library

`hf-store` is distributed as GitHub source. Pin a release tag so builds do not
silently change:

```toml
[dependencies]
hf-store = { git = "https://github.com/dinoml/hf-store-rs", tag = "v0.1.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The default `network` feature enables online planning and acquisition. A
consumer that only opens existing downloads can omit the HTTP stack and Tokio:

```toml
[dependencies]
hf-store = { git = "https://github.com/dinoml/hf-store-rs", tag = "v0.1.0", default-features = false }
```

The minimum supported Rust version is 1.85.

## Choose a cache mode and root

The library never reads cache-related environment variables. Pass the cache
root explicitly to both `HubStore` and `OfflineStore`.

| Mode | Use it when | Cache root |
| --- | --- | --- |
| `CacheMode::Compatible` | Rust and Python should reuse the same canonical Hub downloads | The `huggingface_hub` Hub cache itself, such as the value of `HF_HUB_CACHE`; do not pass its parent `HF_HOME` |
| `CacheMode::Owned` | hf-store owns the cache or a custom endpoint is used | Any explicit application-managed directory |

Compatible mode is conformance-tested against `huggingface_hub` v1.24.0 at
commit `36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`. It writes normal
Python-visible cache entries plus private completeness metadata under each
repository. Existing Python snapshots are imported only after their selected
files have been validated.

Use owned mode for custom endpoints unless the compatible root is explicitly
dedicated to that endpoint. The upstream compatible layout does not encode the
endpoint in its repository directory name.

## Build a request

A request always contains a typed repository identity and revision:

```rust,no_run
use hf_store::{FetchRequest, RepositoryId, RepositorySpec, Revision};

# fn request() -> Result<(), Box<dyn std::error::Error>> {
let model = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
    Revision::parse("main")?,
);

let dataset = FetchRequest::new(
    RepositorySpec::dataset(RepositoryId::parse("namespace/dataset")?),
    Revision::parse("refs/pr/7")?,
);

let space = FetchRequest::new(
    RepositorySpec::space(RepositoryId::parse("namespace/demo")?),
    Revision::parse("0123456789abcdef0123456789abcdef01234567")?,
);
# let _ = (model, dataset, space);
# Ok(())
# }
```

Branches, tags, pull-request revisions, and full lowercase 40-character commit
IDs are accepted. Online planning resolves symbolic revisions to an immutable
commit before files are activated.

### Select files

Use allow and ignore patterns to select a repository subset:

```rust,no_run
# use hf_store::{FetchRequest, RepositoryId, RepositorySpec, Revision};
# fn request() -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
    Revision::parse("main")?,
)
.allow_patterns(["*.json", "*.safetensors"])
.ignore_patterns(["optimizer*.safetensors"]);
# let _ = request;
# Ok(())
# }
```

Patterns follow Python's case-sensitive `fnmatchcase` behavior on every
platform. They are matched against the whole POSIX repository path. In
particular, `*` also matches `/`, so `*.json` selects nested JSON files. Ignore
patterns win after allow patterns. Omitting the allow list selects everything
not ignored; an explicitly empty allow list selects nothing.

## Download or reuse a snapshot

Online operations run on the caller's entered Tokio runtime. `HubStore` is
lazy: building it does not construct an HTTP client or contact the endpoint.

```rust,no_run
use hf_store::{CacheMode, FetchOptions, FetchRequest, HubStore, RepositoryId, RepositorySpec, Revision};

# async fn download() -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
    Revision::parse("main")?,
)
.allow_patterns(["config.json", "tokenizer.json"]);

let store = HubStore::builder()
    .cache_mode(CacheMode::Compatible)
    .cache_root("/explicit/huggingface/hub")
    .max_concurrent_downloads(4)
    .build();

let snapshot = store.fetch(request, FetchOptions::default()).await?;
println!("resolved commit: {}", snapshot.commit());
println!("reused complete snapshot: {}", snapshot.was_reused());

for file in snapshot.files() {
    println!("{} -> {}", file.path(), file.local_path().display());
}

// Keep `snapshot` alive while another component uses its paths. It owns the
// cooperative reader lease that prevents hf-store GC from removing them.
# Ok(())
# }
```

`fetch` first reuses a complete validated selection when available. Otherwise
it resolves metadata, resumes eligible partial transfers, validates every file,
publishes an immutable snapshot, and updates the mutable revision last. A
successful return is therefore safe for a runtime or UI to hand to a model
loader; it does not mean the model format itself is supported.

Use `HubStore::plan` when a UI needs the resolved commit and selected file list
before downloading. Planning is an online metadata operation and does not
publish a snapshot.

The complete runnable version is
[`examples/download.rs`](../crates/hf-store/examples/download.rs):

```text
cargo run -p hf-store --example download -- /path/to/huggingface/hub
```

## Authenticate one request

The library never discovers, persists, or refreshes credentials. Obtain the
secret in the application boundary and attach it only to the request that needs
it:

```rust,no_run
# use hf_store::{AuthToken, FetchRequest, RepositoryId, RepositorySpec, Revision};
# fn request(secret_from_application: String) -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("organization/gated-model")?),
    Revision::parse("main")?,
)
.authorization(AuthToken::new(secret_from_application)?);
# let _ = request;
# Ok(())
# }
```

`AuthToken` has redacted `Debug` output. Do not place the original secret in
logs or wrap errors with messages that contain it. Authorization is stripped
when a redirect crosses to an untrusted origin.

## Strict offline reuse

`OfflineStore` is synchronous and contains no transport, TLS, proxy, token, or
async-runtime capability. `open_request` applies the same filters using a
cached commit-bound tree:

```rust,no_run
use hf_store::{CacheMode, FetchRequest, OfflineStore, RepositoryId, RepositorySpec, Revision};

# fn open() -> Result<(), Box<dyn std::error::Error>> {
let request = FetchRequest::new(
    RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
    Revision::parse("main")?,
)
.allow_patterns(["config.json", "tokenizer.json"]);

let store = OfflineStore::new("/explicit/huggingface/hub")
    .cache_mode(CacheMode::Compatible);
let snapshot = store.open_request(&request)?;
println!("{}", snapshot.directory().display());
# Ok(())
# }
```

This succeeds only if the mutable ref, cached tree, exact-selection manifest,
snapshot, and every selected file are complete and valid. There is no online
fallback. Use `OfflineStore::open` instead when the caller already knows the
exact `RepoPath` list and does not need cached tree filtering.

The runnable example is
[`examples/offline.rs`](../crates/hf-store/examples/offline.rs). It also builds
with `hf-store`'s default features disabled.

For normal online application behavior, calling `HubStore::fetch` is sufficient:
it already reuses complete cached downloads before transferring missing bytes.
Use `OfflineStore` when network prohibition is itself part of the contract.

## Materialize a `local_dir`

Use `fetch_to_local_dir` when another component needs a normal caller-owned
directory rather than an immutable cache snapshot:

```rust,no_run
# use hf_store::{CacheMode, FetchOptions, FetchRequest, HubStore, RepositoryId, RepositorySpec, Revision};
# async fn materialize() -> Result<(), Box<dyn std::error::Error>> {
# let request = FetchRequest::new(RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?), Revision::parse("main")?).allow_patterns(["config.json"]);
let store = HubStore::builder()
    .cache_mode(CacheMode::Compatible)
    .cache_root("/explicit/huggingface/hub")
    .build();

let local = store
    .fetch_to_local_dir(
        request,
        FetchOptions::default(),
        "/application/models/gpt2",
        false,
    )
    .await?;
println!("{}", local.root().display());
# Ok(())
# }
```

The last argument controls replacement of differing selected regular files.
`false` reports a conflict; `true` permits atomic replacement of those selected
files only. Unselected files are preserved, while links, directories, special
files, and unsafe ancestors are always rejected.

Files are copied independently; they are never symlinked or hard-linked to
shared cache blobs. Later user edits therefore cannot mutate the cache. hf-store
writes private completion metadata only after every selected file is installed.

To populate a destination without networking, use
`OfflineStore::materialize_request_to_local_dir`. To validate an existing
completed destination without consulting the cache, use
`OfflineStore::open_local_dir` with its immutable commit and exact path list.
The runnable online example is
[`examples/local_dir.rs`](../crates/hf-store/examples/local_dir.rs).

## Progress, cancellation, retries, and concurrency

`FetchOptions` controls one operation. `HubStoreBuilder` controls service-wide
transfer concurrency:

```rust,no_run
use std::sync::Arc;
use hf_store::{CancellationToken, FetchOptions, ProgressEvent, ProgressObserver};

#[derive(Debug)]
struct UiProgress;

impl ProgressObserver for UiProgress {
    fn observe(&self, event: &ProgressEvent) {
        // Keep this callback short; send the typed event to a bounded UI channel.
        let _phase = event.phase();
        let _path = event.path();
        let _transferred = event.transferred_bytes();
        let _total = event.total_bytes();
        let _reuse = event.reuse();
    }
}

let cancellation = CancellationToken::new();
let cancellation_handle = cancellation.clone();
let options = FetchOptions::default()
    .max_attempts(4)
    .cancellation(cancellation)
    .progress(Arc::new(UiProgress));

// A UI action or task may call this cooperatively.
cancellation_handle.cancel();
# let _ = options;
```

The observer is synchronous and must return quickly. Events contain structured,
credential-free state rather than URLs or headers. Cancellation may preserve a
valid resumable partial, but it cannot publish a blob, snapshot, ref, or
`local_dir` completion record. A zero retry-attempt or concurrency value is
rejected when acquisition starts.

## Custom endpoints and proxies

Endpoint and proxy configuration is explicit:

```rust,no_run
# use hf_store::{CacheMode, Endpoint, HubStore};
# fn store() -> Result<(), Box<dyn std::error::Error>> {
let store = HubStore::builder()
    .endpoint(Endpoint::parse("https://hub.example.test")?)
    .proxy(Endpoint::parse("http://proxy.example.test:8080")?)
    .cache_mode(CacheMode::Owned)
    .cache_root("/application/cache/hf-store")
    .build();
# let _ = store;
# Ok(())
# }
```

Ambient proxy variables are not read. Endpoint values must be origins: user
information, queries, fragments, embedded credentials, and unsafe spellings are
rejected. Plain HTTP authentication is rejected outside loopback fixture use.

## Handle errors by classification

`HubError` deliberately exposes classification helpers instead of a permanent
exhaustive error enum:

```rust,no_run
# use hf_store::HubError;
fn classify(error: &HubError) -> &'static str {
    if error.is_cancelled() {
        "cancelled"
    } else if error.is_authentication() || error.is_gated() {
        "access"
    } else if error.is_missing() {
        "not-found"
    } else if error.is_cache_incomplete() {
        "offline-miss"
    } else if error.is_cache_corrupt() || error.is_validation() {
        "validation"
    } else if error.is_rate_limited() || error.is_transport() {
        "transport"
    } else if error.is_cache_busy() {
        "busy"
    } else {
        "other"
    }
}
```

Check cancellation first. A rate-limited error may include a safe
`retry_after()` duration. Error display and debug output are redacted; callers
should still avoid adding raw request secrets or signed URLs as context.

## Inspect, verify, and garbage-collect

All operational APIs live on `OfflineStore` and do not contact the network:

```rust,no_run
use std::time::{Duration, SystemTime};
use hf_store::{CacheMode, GcPolicy, OfflineStore, RepoPath, RepositoryId, RepositorySpec, Revision};

# fn operations() -> Result<(), Box<dyn std::error::Error>> {
let repository = RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?);
let revision = Revision::parse("main")?;
let paths = [RepoPath::parse("config.json")?];
let store = OfflineStore::new("/explicit/hf-store-cache").cache_mode(CacheMode::Owned);

let inventory = store.inspect_repository(&repository)?;
let verification = store.verify(&repository, &revision, &paths);
let policy = GcPolicy::expired_partials(Duration::from_secs(24 * 60 * 60))
    .ok_or("retention duration is too large")?;
let plan = store.gc_plan(&repository, policy, SystemTime::now())?;

println!("inventory entries: {}", inventory.entries().len());
println!("verification findings: {}", verification.findings().len());
println!("GC candidates: {}", plan.candidates().len());
# Ok(())
# }
```

Inspection reports recognized state; verification revalidates an exact
selection. GC is always two-phase: serialize or review the immutable plan, then
call `gc_execute` with the same repository, cache mode, endpoint, and a clock no
earlier than the plan. Execution reacquires coordination and skips candidates
that changed or became busy. Compatible mode blocks deletion of Python-visible
cache state.

## DinoML-style integration

An in-process runtime or UI should generally:

1. Own the cache root, cache mode, endpoint, and concurrency policy in its
   application configuration.
2. Build a typed `FetchRequest` from the selected repository and files.
3. Call `HubStore::fetch` on an existing Tokio runtime. This handles both
   validated reuse and missing downloads.
4. Retain the returned `Snapshot` while a loader reads its paths.
5. Record the returned immutable commit, selection identity, repository paths,
   sizes, and SHA-256 values in application state if reproducibility matters.
6. Use `OfflineStore` for explicit local-files-only behavior, startup recovery,
   verification, and maintenance.
7. Use `fetch_to_local_dir` only when a consumer requires mutable ordinary files
   outside the cache.

Do not parse cache directories, resolve symlinks, or infer completeness in the
application. `Snapshot` and `LocalDirectory` are the proof-carrying boundaries.
Model artifact selection, configuration parsing, loading, device policy, and
execution remain the caller's responsibility.

## CLI installation

Install the thin CLI directly from a release tag:

```text
cargo install --git https://github.com/dinoml/hf-store-rs --tag v0.1.0 hf-store-cli
```

Run `hf-store --help` or `hf-store <command> --help` for the complete option
surface.

### Fetch and reuse

The CLI discovers the normal Python Hub cache location in compatible mode, so
`--cache-dir` is optional for the canonical endpoint:

```text
hf-store fetch --repo-kind model openai-community/gpt2 --allow config.json --allow tokenizer.json
hf-store fetch --repo-kind dataset namespace/dataset --revision refs/pr/7 --allow "data/*.parquet"
hf-store fetch --repo-kind space namespace/demo --revision main
```

Use a caller-owned directory when needed:

```text
hf-store fetch --repo-kind model openai-community/gpt2 --allow "*.json" --local-dir ./gpt2
hf-store fetch --repo-kind model openai-community/gpt2 --allow "*.json" --local-dir ./gpt2 --force
```

`--force` replaces conflicting selected regular files only. It does not delete
unselected files or bypass validation.

### Offline CLI operation

The same logical filtered request can be reopened from cached tree metadata:

```text
hf-store fetch --offline --repo-kind model openai-community/gpt2 --allow config.json --allow tokenizer.json
```

When the exact selected paths are already known, repeat `--path` instead:

```text
hf-store fetch --offline --repo-kind model openai-community/gpt2 --revision main --path config.json --path tokenizer.json
```

Offline mode does not discover tokens, initialize a proxy, construct TLS state,
or fall back to the network.

### CLI authentication

There is intentionally no raw `--token` argument. Online fetch uses this
precedence: `--no-token`, `--token-file`, `HF_TOKEN`, legacy
`HUGGING_FACE_HUB_TOKEN`, `HF_TOKEN_PATH`, then the normal Hugging Face token
file. Pipe secret-manager output without exposing it in process arguments:

```text
secret-manager read hf-token | hf-store fetch --token-file - --repo-kind model organization/gated-model
```

Inspect, verify, GC, and offline fetch never discover credentials.

### Inspect, verify, and GC

```text
hf-store inspect --repo-kind model openai-community/gpt2
hf-store verify --repo-kind model openai-community/gpt2 --revision main --path config.json
hf-store gc plan --cache-mode owned --repo-kind model openai-community/gpt2 --partial-min-age-seconds 86400 --output gc-plan.json
hf-store gc execute --cache-mode owned --repo-kind model openai-community/gpt2 --plan gc-plan.json --yes
```

GC planning is read-only and creates a new plan file without overwriting an
existing one. `--yes` confirms only the candidates already in that plan; fresh
revalidation may still skip them.

### Machine-readable output

Add `--format json` for one versioned JSON envelope on standard output:

```text
hf-store --format json verify --repo-kind model openai-community/gpt2 --path config.json
```

Automation should branch on `schema`, `version`, `status`, `classification`,
and `exit_code`, not the human message. Human output is intentionally not a
stable parsing format. Progress and diagnostics use standard error and are
disabled in JSON mode.

The stable exit classifications and numeric codes are specified in
[ADR 0008](../adr/0008-command-line-interface.md#exit-statuses).

## Common failures

- **Offline miss:** confirm the cache mode, cache root, endpoint, revision,
  filters, and exact paths match the original acquisition. Python metadata by
  itself does not prove hf-store completeness.
- **Custom endpoint rejected in compatible mode:** use owned mode, or select an
  explicit cache root dedicated to that endpoint.
- **Existing `local_dir` file conflicts:** inspect the file, then opt into
  replacement only if the application owns that selected path.
- **A snapshot path later disappears:** keep the `Snapshot` handle alive and do
  not let non-cooperating tools mutate the compatible cache concurrently.
- **`*.json` selected nested files:** this is expected because `*` matches `/`
  under the pinned Python-compatible filter grammar.
- **Cache-only build reports backend unavailable from `HubStore`:** construct an
  `OfflineStore`; online methods are intentionally unavailable without the
  `network` feature.

For the precise cache and durability guarantees, continue with
[Cache modes, local directories, and offline guarantees](cache-and-offline.md).
