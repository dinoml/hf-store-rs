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
- Competing publishers converge on one validated immutable object.
- Windows correctness does not require Developer Mode or symlink privileges.
- Cache roots on filesystems without reliable advisory locking are rejected for
  coordinated writes.
- Cleanup of abandoned staging entries and conservative garbage collection are
  later operations built on the same locks and leases.
