# ADR 0003: Cache publication and coordination

- Status: Accepted
- Date: 2026-07-21

## Context

The cache must survive cancellation, process termination, competing writers,
and Windows filesystem semantics without exposing partial content. Replacing an
existing file, publishing a new immutable object, and activating a directory
have different cross-platform guarantees and therefore cannot share an
unchecked rename assumption.

## Decision

All content is written to unique staging entries on the same filesystem as its
destination. A writer completes, flushes, synchronizes, and validates staged
bytes before publication. Normal lookup never searches staging or partial
namespaces.

The caller-authorized cache or materialization root is opened once as a
filesystem capability and retained for the operation. Read and mutation
adapters are derived from that same opened directory handle; neither adapter
may reopen the caller's path independently. Every descendant path is relative,
contains only normal components, and is traversed or created one component at a
time without following symbolic links or Windows reparse points. Windows
adapters inspect the reparse attribute before and after opening entries so tags
that are not reported as symbolic links are rejected too. Reads, locks,
staging, publication, replacement, cleanup, and directory synchronization all
operate through that retained root. A descendant directory or final entry that
is a link, reparse point, or other unexpected file type is rejected; it is
never resolved through an ambient joined path. The authorized root itself may
intentionally name a caller-selected link, but no nested cache component
becomes a new authority boundary.

Cooperating processes coordinate with operating-system advisory file locks on
fixed-size lock keys. Writers take exclusive locks for a blob or mutable ref;
readers take shared locks where replacement or garbage collection could race.
The lock file may persist, but the operating system releases its lock when the
process exits, so elapsed time and PID guessing are not used to break a lock.
Filesystems that cannot provide the required lock fail explicitly rather than
falling back to a racy sentinel file.

Immutable blobs and snapshots use create-once publication. After taking the
object lock, a publisher validates an existing destination and reuses it, or
atomically installs the already validated staged object. A conflicting or
corrupt destination is an error and is never silently overwritten.

Mutable records, including refs, use an atomic file-replacement primitive in
the destination directory. The previous complete record remains visible until
the replacement commits. Snapshot content and its manifest are published
before a ref is replaced, and a ref update is always the final activation step.

Durable mode synchronizes staged file data before publication and synchronizes
directory metadata where the operating system and filesystem expose a supported
operation. Unsupported directory synchronization is reported or documented by
the platform adapter; it is not presented as a stronger power-loss guarantee.
Tests inject failures before and after each publication boundary and accept only
the old complete state or the new complete state.

The current private Phase 1 kernel implements atomic visibility, not the future
durable mode. It synchronizes staged files and the immediate destination parent
where supported, but it does not yet synchronize newly created ancestor chains,
and its Windows directory adapter provides no power-loss flush guarantee. A
durable-success contract remains blocked on explicit platform capabilities,
top-down directory synchronization, and cross-process tests on all three target
operating systems.

Internal immutable-snapshot materialization may first attempt a hard link to an
immutable blob on the same filesystem and otherwise copies and validates the
bytes. An explicit user-owned `local_dir` always receives an independent copy,
or a separately proven copy-on-write clone, so later user edits cannot mutate a
shared blob. Symlinks are not required. Readers hold a shared snapshot lease;
garbage collection must acquire the corresponding exclusive lease before
removing snapshot or blob reachability.

## Consequences

- Abrupt termination can leave staging files and inert lock files, but cannot
  make them visible as completed blobs, snapshots, or refs.
- A mutable shared cache cannot redirect hf-store reads or writes through a
  nested link or reparse point outside the caller-authorized root.
- Competing publishers converge on one validated immutable object.
- Windows correctness does not require Developer Mode or symlink privileges.
- Cache roots on filesystems without reliable advisory locking are rejected for
  coordinated writes.
- Cleanup of abandoned staging entries and conservative garbage collection are
  later operations built on the same locks and leases.
