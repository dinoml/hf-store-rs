---
rfc: "0001"
title: Hub store v0.1
status: proposed
owners: [hlky]
created: 2026-07-21
updated: 2026-07-21
tracking_issue: https://github.com/dinoml/hf-store-rs/issues/1
project: https://github.com/orgs/dinoml/projects/1
depends_on: []
related: []
source_material:
  - DinoML open-source auxiliary ecosystem plan
---

# RFC 0001: Hub store v0.1

## Summary

`hf-store-rs` will resolve a Hugging Face Hub repository revision, retrieve and
validate a selected file set, and atomically expose an immutable local snapshot.
The same request must later work without network access when its complete,
validated content is present.

This RFC proposes behavior and invariants. It deliberately does not yet accept a
public async/runtime contract or a permanent cache layout.

## Outcome

Given an endpoint, repository kind and identifier, revision, and optional file
filters, v0.1 should return a local snapshot whose files are complete, validated,
and bound to an immutable commit identity.

The initial claim covers model, dataset, and Space repositories on Linux,
macOS, and Windows. Windows correctness must not require symlink privileges.

## Goals

- Resolve branches, tags, full commits, and pull-request revisions.
- Support explicit bearer authentication and actionable gated-repository errors.
- Support one configurable Hub-compatible endpoint per store.
- Plan filtered snapshots using allow and ignore patterns.
- Bound concurrent transfers and support safe range-based resumption.
- Validate remote identity, expected size, and available content digests.
- Compute a local cryptographic digest before publishing every blob.
- Keep shared content-addressed blobs and immutable snapshot manifests.
- Publish snapshots and mutable refs atomically.
- Report progress and support cooperative cancellation.
- Open complete snapshots without contacting the network.
- Inspect, verify, and conservatively garbage-collect cache content.

## Non-goals

- Uploads, commits, repository creation, or Git/LFS publishing.
- Application package installation, activation, rollback, or artifact selection.
- Model configuration parsing, checkpoint decoding, or tensor access.
- Device execution, sessions, scheduling, or memory placement.
- Python execution or pickle loading.
- Credential persistence, browser login, or ambient global authentication state.
- Exact on-disk compatibility with Python `huggingface_hub` unless separately
  approved.
- `local_dir` mirroring semantics, a cache daemon, or automatic quota eviction.
- Multi-endpoint failover, Xet acceleration, or remote shard leases in v0.1.

## Boundary

This repository owns repository identity, revision resolution, request-time
authentication, transport, validation, cache storage, immutable snapshots,
offline lookup, inspection, and garbage collection.

DinoML's application service owns installation policy, package activation,
artifact selection, model lifecycle, and execution.

## Proposed public seams

Names are illustrative until this RFC is accepted:

```rust,ignore
let store = HubStore::builder(cache_root)
    .endpoint(endpoint)
    .max_concurrent_downloads(8)
    .build()?;

let request = SnapshotRequest::builder(
        RepositorySpec::model(RepositoryId::parse("org/model")?),
    )
    .revision(Revision::parse("main")?)
    .allow_pattern("*.json")
    .allow_pattern("*.safetensors")
    .ignore_pattern("*.bin")
    .build()?;

let snapshot = store.fetch(&request, &options).await?;
let config = snapshot.file("config.json")?;
```

Expected value and service types include:

- `RepositoryKind`, `RepositoryId`, `RepositorySpec`, and `RepoPath`;
- `Revision` and immutable `CommitId`;
- `Endpoint` and redacted `AuthToken`;
- `HubStore`, `SnapshotRequest`, `FetchOptions`, and `FetchMode`;
- `Snapshot`, `SnapshotFile`, `ProgressEvent`, and `CancellationToken`;
- inspection, verification, garbage-collection plan, and report types.

Public APIs must not expose HTTP-client, async-runtime, URL, glob, secrecy,
serialization, temporary-file, or locking implementation types. Authentication
is explicit in the library; environment and config-file discovery belong in the
future CLI.

## Required invariants

### Identity and paths

1. Namespace cache content by normalized endpoint, repository kind, and
   repository identifier.
2. Convert untrusted identities to fixed-size internal keys before using them as
   directory names.
3. Normalize remote paths as POSIX paths before conversion to host paths.
4. Reject absolute paths, empty unsafe segments, `.` and `..`, backslashes,
   NULs, drive or UNC prefixes, alternate data streams, Windows-reserved names,
   trailing spaces or dots, and materialization collisions.
5. Resolve symbolic revisions to an immutable commit before activation.

### Transfer and validation

1. Partial bodies never occupy the published blob namespace.
2. Resume only when stored remote identity, validator, expected size, and target
   identity still match.
3. Validate status and `Content-Range`; a full response to a range request
   restarts safely from zero.
4. Treat ETags as transport validators unless their digest semantics are known.
5. Compute a local cryptographic digest for every completed blob.
6. A mismatch cannot publish a blob or activate a snapshot.
7. Cancellation may preserve a valid resumable partial but cannot change the
   active ref or expose completed state.

### Snapshots and refs

1. Build a snapshot entirely in same-volume staging.
2. Record an immutable, versioned manifest covering selected paths and blob
   identities.
3. Publish the snapshot only after all entries and metadata are complete.
4. Update a mutable ref last through atomic replacement.
5. Readers never observe staging or incomplete snapshots.
6. Equivalent selected file sets share a selection identity independent of
   filter expression order.
7. Unknown cache or manifest versions fail explicitly.
8. Materialization must fall back to copying and must never require symlinks.

### Offline and garbage collection

1. Cache-only mode cannot instantiate or invoke a network transport.
2. An offline hit requires a complete manifest and all selected validated blobs.
3. Garbage collection first produces an immutable dry-run plan.
4. Execution reacquires coordination, revalidates the plan, and skips changed or
   busy objects.
5. Reachable or actively leased content is never deleted.
6. v0.1 has no automatic background deletion.

### Secrets and diagnostics

1. Tokens, authorization headers, and signed URLs never enter logs, errors,
   debug output, progress events, or cache metadata.
2. `AuthToken` debug output is always redacted.
3. Redirect handling cannot forward authorization across an untrusted origin.
4. Error types expose safe classification methods without exposing a permanent,
   exhaustive implementation enum.

## Conceptual cache model

The accepted format may use a versioned, origin-namespaced structure resembling:

```text
<root>/hf-store-v1/
  format.json
  origins/<origin-key>/
    origin.json
    repos/<kind>/<repository-key>/
      repo.json
      refs/
      trees/
      blobs/
      snapshots/
      partials/
      staging/
      locks/
      trash/
```

Original identities remain in validated metadata; hash-derived path keys prevent
traversal, case folding, reserved-name, and path-length ambiguity. This is a
conceptual model, not an accepted disk-format promise.

## Delivery phases

### Phase 0: decisions and fixtures

- Decide behavioral versus shared Python cache compatibility.
- Decide async runtime and HTTP/TLS implementation policy.
- Decide blob digest, materialization, locking, durability, and GC coordination.
- Build deterministic filesystem and local HTTP fixtures with failure injection.

Exit: blocking choices have ADRs and tests describe the v0.1 contract.

### Phase 1: identity and local cache kernel

- Add validated identity, revision, commit, endpoint, path, and secret types.
- Add a versioned layout, metadata records, atomic writes, and blob publication.
- Add collision, traversal, corruption, competing-writer, and crash-boundary
  tests.

Exit: local content can be validated and published without exposing partial data.

### Phase 2: metadata and fetch planning

- Resolve supported revisions for all repository kinds against a mock Hub.
- Retrieve tree metadata and create deterministic filtered fetch plans.
- Classify authentication, gated, missing, and protocol failures.

Exit: fixture-backed requests produce immutable commit-bound plans.

### Phase 3: transfer engine

- Add bounded concurrency, range resumption, validation, progress, and
  cancellation.
- Coordinate competing processes and recover or reject stale partials.

Exit: interruption and concurrency tests converge on validated blobs.

### Phase 4: activation and offline operation

- Materialize, validate, and atomically publish immutable snapshots.
- Update refs last and implement strict cache-only lookup.

Exit: no failure point exposes an incomplete snapshot, and offline tests prove
zero network calls.

### Phase 5: operations

- Add inspect, verify, dry-run GC, conservative GC execution, and stable
  machine-readable reports.
- Add the `hf-store` CLI only once operational contracts are accepted.

Exit: corruption and reachability are explainable and GC is demonstrably safe.

### Phase 6: conformance and release

- Pin upstream reference fixtures and their provenance.
- Run current and MSRV Rust plus Linux, macOS, and Windows CI.
- Complete security review, documentation, and release policy.

Exit: every published v0.1 support claim is fixture-backed.

## Acceptance criteria

- A filtered public snapshot succeeds for each claimed repository kind.
- Mutable revisions resolve to a recorded immutable commit.
- Repeated fetches reuse validated content.
- The same request succeeds cache-only with a transport that fails if called.
- Interrupted transfers resume safely; changed validators force a restart.
- Cancellation never exposes a completed ref or snapshot.
- Concurrent identical fetches produce one coherent result.
- Every activated snapshot has a complete manifest and verified selected files.
- Windows behavior requires neither Developer Mode nor administrator privileges.
- Inspection and verification identify corruption precisely.
- Garbage collection never removes reachable or busy data.
- Authenticated and gated fixture flows work without diagnostic secret leakage.
- The crate builds without DinoML dependencies.
- Formatting, strict linting, tests, docs, MSRV, and three-OS CI pass.

## Decisions required before Phase 1

1. Behavioral compatibility or shared on-disk compatibility with
   `huggingface_hub`.
2. Async runtime, HTTP client, TLS defaults, and feature policy.
3. Canonical blob digest and interpretation of Git/LFS metadata and ETags.
4. Snapshot materialization fallback order.
5. Cross-process locking, reader leases, stale-lock handling, and durability.
6. Filter grammar and filtered-snapshot identity.
7. Garbage-collection reachability and retention rules.
8. Initial CLI scope and stable machine-readable output.

The locking and atomic-replacement model requires a focused Windows/Linux spike.
The RFC must not assume identical rename or directory-durability semantics across
platforms without tests.

## References

- [Hugging Face Hub download API](https://huggingface.co/docs/huggingface_hub/package_reference/file_download)
- [Hugging Face Hub cache inspection API](https://huggingface.co/docs/huggingface_hub/package_reference/cache)
- [Hugging Face Hub authentication API](https://huggingface.co/docs/huggingface_hub/package_reference/authentication)
