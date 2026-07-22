# Cache modes, local directories, and offline guarantees

## Compatible cache

`CacheMode::Compatible` reads and writes the canonical `huggingface_hub`
standard-cache layout for the canonical Hub endpoint. Compatibility is tested
against `huggingface_hub` v1.24.0 at commit
`36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`; other versions are not implied.
hf-store adds private, versioned `.hf-store/hf-store-v1` bindings and
exact-selection manifests. Python-visible refs, snapshots, blobs, tree records,
and negative records retain their upstream meaning.

The compatible view recognizes relative-symlink snapshots on Unix and both
regular-file forms on every platform. Windows does not require symlink
privileges. Normal garbage collection does not delete Python-visible state,
because hf-store cannot prove that arbitrary Python processes are quiescent.

## Owned cache

`CacheMode::Owned` uses the versioned hf-store layout. It stores validated
content-addressed blobs, immutable exact-selection snapshots, cached remote
trees, and mutable refs activated last. This mode supports custom endpoints and
full owned-cache garbage collection.

The on-disk format is versioned independently from the Rust API. Unknown
versions fail closed. A future format migration will use a new namespace or an
explicit migration tool; v0.1 never silently reinterprets unknown records.

## Strict offline operation

`OfflineStore` contains no HTTP client, proxy, TLS, token, or transport factory.
`open_request` resolves a cached ref and cached commit tree, applies the same
allow/ignore filter as online planning, and revalidates every selected snapshot
entry. `open` handles an already known exact path set. Missing, changed,
incomplete, corrupt, unsupported, or unsafe state returns a classified error;
there is no online fallback.

Keep the returned `Snapshot` alive while downstream code uses its file paths.
The handle owns the cooperative reader lease used by hf-store GC. Compatible
cache files can still be changed by non-cooperating Python or user processes.

## `local_dir`

A `local_dir` is caller-owned mutable output, not a cache snapshot. Each
selected file is independently copied through destination-volume staging,
validated, flushed, and atomically installed. Files are never symlinked or
hard-linked to shared cache blobs. Unselected files are preserved. Conflicting
selected regular files are replaced only when the caller opts in; special
files and unsafe ancestors are rejected.

Completion requires hf-store's private exact-selection manifest. Upstream
download metadata is only a reuse hint. `OfflineStore::open_local_dir` hashes
and validates the exact commit and selection again, so a later edit invalidates
completion. `materialize_request_to_local_dir` can reconstruct the destination
from a complete cache without transport.

## Durability

Publication provides atomic visibility: readers observe the old complete state
or the new complete state. Staged file data and supported parent directories are
synchronized, but v0.1 does not claim general power-loss durability,
particularly for newly created ancestor chains or Windows directory metadata.
See [platform capabilities](platform-capabilities.md).

