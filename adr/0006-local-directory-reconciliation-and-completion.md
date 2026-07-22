# ADR 0006: Local directory reconciliation and completion

- Status: Accepted
- Date: 2026-07-21

## Context

An explicit `local_dir` is user-owned and mutable. It can contain unrelated
files, files written by an earlier selection, and files edited after hf-store
materialized them. It also exposes repository files directly while a multi-file
update is in progress, so it cannot use an immutable cache snapshot's
create-once directory activation.

The upstream `huggingface_hub` bookkeeping below `.cache/huggingface` helps
Python decide whether an individual file might be reusable. It does not bind a
complete selected path set to validated local bytes and therefore cannot prove
that an hf-store request is complete offline.

Reconciliation policy must be fixed before a public `local_dir` option exposes
observable overwrite, deletion, concurrency, and recovery behavior.

## Decision

`local_dir` materialization is an explicit reconciliation operation, not a
directory mirror and not an immutable snapshot. hf-store manages only the paths
selected by the current commit-bound request and its own two reserved metadata
namespaces:

- `.cache/huggingface` contains upstream-compatible reuse hints; and
- `.cache/hf-store/hf-store-v1` contains hf-store coordination and completion
  records.

Unselected files and directories are preserved. A later request does not delete
paths selected by an earlier request merely because they are absent from the
new selection. Automatic pruning is deferred beyond v0.1. Repository paths
that collide portably with either reserved namespace or with another selected
path are rejected before any destination is changed.

Every selected destination is reconciled from validated content as follows:

1. A regular file whose exact size and SHA-256 digest already match the target
   is reused without replacing its bytes.
2. A missing destination is created through destination-volume staging and
   atomic replacement.
3. A directory, symlink, reparse point, special file, or regular file with
   different bytes is a conflict by default.
4. An explicit force-replacement option may replace a conflicting selected
   path, but it does not authorize deletion of unselected paths or replacement
   of a directory that contains other entries.

The same conflict rule applies whether the existing path was unrelated,
previously managed, or modified by the user. Previous completion metadata may
help diagnostics explain which case was observed, but it never grants implicit
permission to discard current bytes. Conflict errors and debug output do not
include file content or untrusted metadata values.

User-visible repository files are always independent copies. v0.1 never uses a
symlink, hard link, or reflink from cache or source content into `local_dir`,
even when the source is an immutable shared blob. A missing-path create-once
install may transiently hard-link the already independent destination-volume
staging file to its final name before unlinking the staging name. This is an
atomic no-clobber installation detail, not sharing with a cache blob. If the
filesystem reports that operation as unsupported, materialization fails instead
of falling back to a visible partial file or a racy overwrite. Each replacement
is staged
on the destination volume, written and hashed while copying, flushed, checked
against the expected size and known remote digest, and atomically installed. A
validated existing file is rechecked after the materialization lock is acquired
before it is reused or replaced.

Cooperating materializers serialize the entire reconciliation with an
operating-system advisory lock in `.cache/hf-store/hf-store-v1`. The lock is
acquired before reading the prior completion record and held through the final
completion publication. A non-cooperating writer can still modify a user-owned
directory; readers therefore validate bytes rather than trusting the lock or
timestamps.

A versioned hf-store completion record binds the canonical endpoint,
repository, immutable commit, selection identity, and every selected path to
its size and local SHA-256 digest. Before changing the first selected file, a
materializer invalidates any previous completion record through atomic
replacement with an in-progress state. It then reconciles files in canonical
path order and writes compatible per-file metadata only after each final file
is visible. The compatible tree record is written after the selected files.
The new complete record is published last through atomic replacement.

Consequently, an interrupted multi-file operation may leave a mixture of old
and new user-visible files, but that mixture is never reported as a complete
hf-store result. Reopening offline requires a complete record for the exact
request and revalidates every selected destination. An in-progress, missing,
unknown-version, stale, or mismatched record is incomplete. Staging files and
lock files never establish completeness.

The upstream three-line download metadata remains a reuse hint. Its commit,
ETag, timestamp, freshness relationship to the destination mtime, and lock path
are read and written according to the pinned Python behavior, but a fresh
record cannot replace hf-store byte validation or completion metadata. The
final repository file is installed before its upstream metadata so Python
cannot observe metadata claiming a file that has not become visible.

This ADR fixes private behavior only. It does not accept public option names,
force-method signatures, conflict error types, callbacks, or pruning APIs.

## Consequences

- Normal materialization preserves unrelated files and user edits rather than
  silently deleting or overwriting them.
- Force replacement has a narrow, selected-path scope and cannot become a
  recursive directory deletion operation.
- Old selected files may remain on disk after a narrower or newer request; the
  completion record, not directory enumeration, defines the current result.
- A multi-file destination is not physically atomic, but its logical complete
  state is: offline readers accept only a fully validated final record.
- Python and Rust can reuse compatible per-file bookkeeping without treating
  Python metadata as proof of hf-store completeness.
- Concurrent cooperating materializers cannot interleave their completion
  records, while non-cooperating edits are detected by final byte validation.

## References

- [ADR 0002](0002-cache-identity-and-format.md)
- [ADR 0003](0003-cache-publication-and-coordination.md)
- [RFC 0001](../rfcs/0001-hub-store-v0.1.md)
