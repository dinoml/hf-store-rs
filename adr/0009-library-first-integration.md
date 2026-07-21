# ADR 0009: Library-first downstream integration contract

- Status: Accepted
- Date: 2026-07-21

## Context

`hf-store-rs` is a standalone auxiliary project, but its primary planned use is
inside future `dinoml_v2` Rust runtimes and user interfaces. Those consumers
need to resolve and download Hugging Face model repositories or reuse content
that is already present in an owned or Python-compatible cache, then hand a
complete local checkpoint to model-specific code.

The CLI accepted by ADR 0008 is useful for people and automation, but a
subprocess and its serialized output are not an adequate integration boundary
for an in-process Rust runtime or UI. The reusable library therefore needs a
strong typed contract for identity, acquisition, local lifetime, progress,
cancellation, and failure classification.

Current `dinoml_v2` Rust checkpoint consumers already open validated checkpoint
directories and retain immutable Safetensors snapshots through
`dinoml-checkpoint`. Its workspace currently declares Rust 1.95. These are
motivating examples of the downstream seam, not dependencies or reasons to
raise this repository's Rust 1.85 MSRV.

## Decision

### Library is the product boundary

The documented Rust library is the authoritative integration surface. The CLI
is a thin adapter that discovers ambient inputs, constructs library values,
invokes library operations, and renders their typed results. Acquisition,
offline lookup, inspection, verification, and garbage-collection behavior do
not live only in the CLI.

The library has no dependency on DinoML crates and exposes no DinoML artifact,
tensor, model, device, session, or UI type. It returns validated local repository
content and provenance; downstream code decides which files are model inputs
and how to load or execute them.

This ADR fixes semantic requirements, not exact public type names, trait shapes,
builders, callbacks, or module paths. A surface remains private until its phase
gates and observable-behavior tests are complete.

### Online and offline services are distinct capabilities

The public seam has two explicit construction paths:

1. An online acquisition capability is available with the `network` feature.
   It receives validated endpoint, cache, transport-policy, request, and
   operation values. Authentication is an optional redacted input supplied for
   one request; the service does not discover or persist credentials. Network
   work is asynchronous on the caller's entered runtime under ADR 0004.
2. An offline lookup capability is constructed only from local cache or
   `local_dir` dependencies. Its representation contains no transport,
   transport factory, TLS state, proxy state, or fallback to online behavior.
   It remains usable without default features and without an async runtime.

Offline is therefore not merely a boolean checked late inside an otherwise
network-capable request. Shared identity and selection values may be accepted by
both capabilities, but asking the offline service to contact the Hub must be
unrepresentable. An online service may reuse complete validated state; that
reuse does not weaken the separate transport-free offline proof.

### Cache and destination policy is explicit

Callers explicitly select the compatible or owned cache view and its root,
subject to ADR 0002's endpoint rules. A compatible-cache hit may reuse bytes
written by `huggingface_hub`; an owned-cache hit may reuse hf-store objects. The
result records which validated source was used without exposing private layout
types.

An explicit `local_dir` is a separate mutable reconciliation destination under
ADR 0006. Its result reports exact completion and conflicts; it is not silently
treated as an immutable cache snapshot. Force replacement, compatible-cache
selection, owned-cache selection, and offline operation remain independent
policies rather than one ambiguous mode flag.

### Results preserve identity, lifetime, and provenance

A successful snapshot result is an immutable, lease-backed handle, not only a
bare path. It binds:

- the validated endpoint origin, repository kind, and repository identifier;
- the requested revision and resolved immutable commit;
- the canonical selected path set and its selection identity;
- every selected file's validated size and local digest, together with any
  proven remote content identity; and
- the cache view, validation outcome, and whether transfer bytes were reused or
  downloaded.

File lookup returns validated file handles tied to the snapshot lifetime. A
directory path may also be exposed for existing checkpoint-directory consumers,
but documentation and examples require retaining the owning snapshot handle for
the whole downstream use so its reader lease remains active. Internal sharing
or synchronization wrappers do not leak into the public signature solely for
implementation convenience.

The lease coordinates hf-store readers, replacement, and garbage collection. It
cannot prevent a non-cooperating Python process or user from mutating externally
owned compatible-cache or `local_dir` state. Compatible files are revalidated
when acquired and opened, and a mutable `local_dir` uses its distinct completion
contract; the API does not overstate operating-system immutability.

Provenance never contains credentials, authorization headers, signed URLs,
temporary paths, or untrusted raw response data. Mutable revision text is never
a substitute for the resolved commit in a successful result.

### Errors, progress, cancellation, and runtime are library contracts

Library operations return documented error structs with stable classification
queries suitable for runtime policy and UI presentation. Human messages and
private implementation causes are not the classification API, and no permanent
exhaustive public enum prevents adding internal failure detail. Public errors
and value types implement safe `Debug`; relevant handles, futures, and events
are `Send`; secrets remain redacted in every representation.

Online operations expose structured progress at the library boundary, including
safe operation phase, repository path or role, validated and transferred byte
counts, reuse decisions, and retry classification. The delivery mechanism and
exact names remain gated, but progress never relies on parsing log text and
never contains a credential or signed URL. The CLI only renders these events.

Cancellation is an explicit cooperative operation input shared with internal
planning and transfer work. Observing cancellation returns a classified error,
joins bounded request work, and cannot activate a ref, snapshot, or completion
record. It may retain only a valid resumable partial under the transfer
contract.

The library does not create, own, enter, or block on an async runtime and does
not detach request work. Public APIs expose neither Tokio nor HTTP-client types.
Cache-only lookup, compatible-cache reuse, and local validation remain available
without constructing runtime or network state.

### Reuse and downstream conformance are observable

Cache reuse is proved through byte-counting and fail-if-used test doubles, not
inferred from a returned path. Exact-commit compatible-cache, owned-cache, and
`local_dir` cases must demonstrate zero downloaded body bytes when complete
validated content already exists. Offline tests additionally use a construction
path in which no transport factory is present. A symbolic online revision may
still require metadata resolution; reports distinguish that from file transfer.

Before stabilizing the integration surface, this repository adds downstream
contract fixtures that:

- compile the intended consumer flow with default features and with a
  cache-only, no-default-features build;
- assert the principal handles and online futures satisfy their documented
  `Send` bounds without exposing third-party types;
- retain a snapshot while a checkpoint-directory consumer opens configuration,
  tokenizer, and weight files;
- drive structured progress, cancellation, and error classification as a UI
  would; and
- prove that compatible Python downloads and hf-store downloads are both
  reusable without another model-file transfer.

`dinoml_v2` may add its own integration test against a pinned hf-store revision,
but hf-store's tests use a small independent consumer fixture so this crate does
not gain a DinoML dependency. The two projects may support different MSRVs; the
integration lane verifies their actual overlap instead of coupling their
release policies.

## Consequences

- DinoML runtimes and UIs can integrate in process without invoking or parsing
  the CLI.
- Existing Python downloads are first-class acquisition inputs, while exact
  commit, selection, validation, and lifetime remain stronger than a cache path.
- Downstream code must retain snapshot handles while using exposed directories
  or files; dropping a lease and retaining only a path forfeits that guarantee.
- Offline binaries can omit network dependencies, and offline behavior in a
  network-enabled binary follows the same transport-free construction seam.
- Model loading and execution remain downstream concerns, so the library stays
  independently reusable and testable.
- Public API stabilization waits for integration contract tests and the
  corresponding phase gates, even if the CLI could temporarily reach private
  implementation seams sooner.

## References

- [RFC 0001](../rfcs/0001-hub-store-v0.1.md)
- [ADR 0001](0001-hub-transport-and-cache-boundary.md)
- [ADR 0002](0002-cache-identity-and-format.md)
- [ADR 0003](0003-cache-publication-and-coordination.md)
- [ADR 0004](0004-transport-runtime-tls-and-features.md)
- [ADR 0006](0006-local-directory-reconciliation-and-completion.md)
- [ADR 0007](0007-cache-garbage-collection.md)
- [ADR 0008](0008-command-line-interface.md)
- [Tracking issue #1](https://github.com/dinoml/hf-store-rs/issues/1)
- [`dinoml_v2` workspace policy](https://github.com/hlky/dinoml_v2/blob/c1bdc59d7d8344eb5da273906c40a1eae2af6172/Cargo.toml)
- [`dinoml-checkpoint` immutable Safetensors snapshot](https://github.com/hlky/dinoml_v2/blob/c1bdc59d7d8344eb5da273906c40a1eae2af6172/crates/dinoml-checkpoint/src/checkpoint.rs)
- [`dinoml-checkpoint`-based checkpoint-directory consumer](https://github.com/hlky/dinoml_v2/blob/c1bdc59d7d8344eb5da273906c40a1eae2af6172/crates/dinoml-autoencoder-kl/src/checkpoint.rs)
