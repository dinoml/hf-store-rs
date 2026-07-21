# ADR 0007: Cache garbage-collection reachability and retention

- Status: Accepted
- Date: 2026-07-22

## Context

The owned cache and the Hub-compatible cache contain immutable snapshots and
shared blobs together with mutable refs, resumable partials, publication
staging, advisory-lock files, reader leases, and private metadata. Deleting an
apparently unused blob is unsafe unless every way of reaching it has been
understood. A scan can also become stale while another process publishes a
snapshot, moves a ref, opens a reader lease, or resumes a transfer.

`huggingface_hub` v1.24.0 provides an immutable revision-deletion strategy and
deletes snapshots before their unshared blobs. That behavior informs the
compatible view, but it does not coordinate with hf-store reader leases or
revalidate reachability when a dry run is executed. Python processes also do
not participate in hf-store's cache-wide maintenance coordination. The RFC's
stronger rule is that reachable, changed, busy, or actively leased content is
never deleted.

## Decision

Garbage collection is an explicit, two-step operation: a read-only scan creates
an immutable plan, and a separate execution attempts only the removals in that
plan. Version-one has no background collector, automatic quota eviction, or
implicit deletion during fetch or offline lookup. Public API and report type
names remain deferred until their observable behavior is implemented.

### Reachability graph

Reachability is evaluated per validated repository while preserving the cache
root, format record, origin and repository identity records, lock files, and
lease files as structural anchors. Persistent lock files are not considered
stale from their age or recorded process identifier and are not ordinary GC
candidates. Leaving empty structural directories and inert lock files is
preferable to risking a split lock domain.

The roots of the graph are:

1. every valid mutable ref, which retains its immutable commit and every
   complete exact-selection snapshot currently published for that commit;
2. immutable commits, snapshots, or selections explicitly retained by the
   caller's policy, including the policy's per-repository keep floor;
3. every snapshot or blob protected by an active shared reader lease, and every
   object protected by an active exclusive publication, transfer, or
   maintenance lease; and
4. in a compatible repository, every valid Python-visible ref and every
   Python-visible snapshot retained by the same policy.

A retained snapshot reaches its validated manifest, every selected snapshot
entry, every referenced local blob identity, and every compatible binding
needed to relate an upstream physical blob key to its local digest and size.
A retained commit reaches its valid tree and negative-cache records when those
records are needed to reproduce lookup or planning for that commit. A sidecar
manifest is an edge only after its kind, version, commit, selection identity,
file set, sizes, digests, and physical bindings validate; sidecar presence does
not make an incomplete upstream snapshot complete.

The compatible scanner accounts for all upstream forms covered by the pinned
conformance corpus. A contained relative snapshot symlink reaches its physical
blob target. A snapshot-only regular file is content owned by that snapshot. A
copied regular snapshot file and a retained physical blob are related only when
a validated exact-selection manifest and binding prove the relationship.
Before deleting any compatible physical blob, the scanner must enumerate and
validate every Python-visible ref and snapshot in that repository, including
slash-containing refs, and prove that no retained snapshot or valid sidecar
edge reaches the blob. If any regular-file, symlink, binding, or snapshot form
cannot be classified completely, physical-blob deletion is disabled for that
repository.

An unreferenced snapshot is not immediately garbage: it remains retained until
the retention policy makes it eligible. Once eligible, its snapshot content,
manifest, tree or negative records used only by it, and bindings used only by
it form a deletion unit. A blob is eligible only after all deletion units that
reach it have been quarantined and a fresh graph contains no remaining edge to
it. Bindings without a retained manifest, and manifests without a complete
snapshot, are eligible only after their own grace period and validation. An
explicit user-owned `local_dir` and its completion metadata are outside cache
GC scope and are never traversed or deleted by this collector.

### Retention and time

Every destructive plan is built from an explicit caller policy. The policy
selects repository scope, additional keep roots, a minimum age for unreferenced
snapshots and metadata, separate minimum ages for partials, staging entries,
and trash, a per-repository snapshot keep floor, and optional maximum object
and logical-byte budgets or a reclaim target. Later CLI decisions may choose
human-facing defaults; this ADR does not silently establish them. An omitted
destructive policy produces inspection information, not deletion candidates.

Candidate ordering is deterministic: recognized trash first, then abandoned
staging and unusable partials, then unreferenced snapshot deletion units, then
newly unreachable bindings and blobs. Within a class, older observations sort
first and stable internal identity breaks ties. Limits apply to whole deletion
units so a limit cannot schedule half of a snapshot closure. Reported reclaim
size is an estimate of logical cache bytes; hard links, compression, allocation
units, and concurrent changes can make actual filesystem reclamation differ.

Age uses a wall clock supplied to the operation and one plan-time instant.
Access time is not used because it is optional, platform-dependent, and may be
disabled. Publication metadata is preferred; otherwise the newest validated
modification time in the deletion unit is used conservatively and becomes part
of its observed fingerprint. A missing, unreadable, future, contradictory, or
insufficiently precise timestamp retains the object. Execution obtains a fresh
instant from the same substitutable clock abstraction and skips an object if
time moved backwards or its minimum age is no longer proven. This makes time
and grace-boundary tests deterministic without ambient clock state.

Recognized, identity-compatible resumable partials younger than their grace
remain available for resumption. A partial is eligible only when its versioned
record and payload agree, it is past the configured grace, and its transfer
lock can be acquired exclusively. Unknown partial records remain untouched.
Staging is never a lookup root, but only a recognized hf-store staging entry
past its grace may be collected, under the publication lock that owns its
destination. Python `*.incomplete` files are Python-visible transient state and
follow the compatible-maintenance rule below. Trash payloads are eligible only
when their tombstone is recognized, complete, past its trash grace, and not
leased or busy.

### Dry-run plan and execution revalidation

Creating a plan performs no cache writes, ref updates, lease acquisition held
beyond the scan, timestamp refresh, quarantine, or deletion. The immutable plan
records the cache layout and repository identities, plan-time instant, complete
policy, stable candidate order, object kinds and fixed-size identities,
reachability reasons, expected logical sizes, blockers, and observed
fingerprints. Fingerprints cover the metadata version and digest, filesystem
entry type, size, relevant timestamps, directory membership, and symlink target
or platform file identity where applicable. A path string alone is never an
object identity. Plans and reports contain no credential, authorization header,
signed URL, or unredacted endpoint secret.

Execution may shrink a plan but never add a target, substitute a path, exceed a
plan limit, or reinterpret an unknown record. For each repository and deletion
unit it:

1. reacquires cache and repository maintenance coordination in a fixed lock
   order, then attempts the corresponding object locks and exclusive leases;
2. rescans refs, snapshots, manifests, bindings, blobs, partials, staging, and
   relevant trash from a handle rooted at the validated cache directory;
3. rechecks layout versions, root and object identities, the full reachability
   graph, policy and grace, observed fingerprints, and path containment; and
4. skips and reports any object that is now reachable, changed, missing,
   replaced, busy, leased, ambiguous, corrupt, or unsupported.

Lease acquisition at execution is authoritative. It is non-destructive when an
active shared or exclusive lease prevents exclusive acquisition; elapsed time,
PID files, and process probing never override an operating-system lock. Direct
blob readers hold a shared blob lease, while a snapshot lease protects its
manifest and all transitively referenced blobs. A reader must acquire its
shared lease before accepting a path, so an exclusive GC lease prevents a new
hf-store reader from entering between revalidation and quarantine. A filesystem
without the advisory-lock behavior required by ADR 0003 cannot execute GC.

### Compatible-cache maintenance

Compatible refs, snapshots, regular snapshot content, blob files, `.no_exist`
records, tree metadata, and Python `*.incomplete` files are Python-visible
state. The `.hf-store/hf-store-v1` sidecar is private state, but its manifests
and bindings contribute edges into that state. The sidecar is scanned with the
repository and is never treated as an independent authority for deleting an
upstream object.

The hf-store maintenance lock coordinates hf-store processes only; it cannot
prove that an arbitrary Python process is quiescent. Consequently, normal
compatible-cache execution may remove only unreachable private sidecar,
staging, and recognized trash entries. A plan may report reclaimable
Python-visible objects, but execution skips them unless the caller enters an
explicit exclusive compatible-maintenance context and guarantees that
non-cooperating users of the root are stopped. This precondition is reported,
not inferred from process enumeration. Even in that context, execution must
freshly revalidate every Python-visible ref and snapshot, all sidecar edges, and
all hf-store leases immediately before quarantine. This is stricter than the
pinned Python deletion strategy and is required to preserve the RFC invariant.

GC never deletes a valid mutable Python ref merely because its target is old.
Deleting a named ref or requested revision is an explicit cache-management
operation outside automatic reachability collection. A detached compatible
snapshot may become eligible under the caller's retention policy and exclusive
maintenance precondition. Unknown files outside the recognized compatible
namespaces are never adopted as GC targets.

### Quarantine and unlink

Deletion uses the repository's same-filesystem `trash` namespace. After final
revalidation and exclusive lease acquisition, execution prepares a unique,
versioned tombstone and atomically renames the exact no-follow source entry to
its quarantine name without replacement. Normal readers never search trash.
The rename removes snapshot content and manifests before any newly unreachable
binding or blob is quarantined; blobs are always last. Refs selected as roots
are never renamed. Source disappearance, destination collision, unsupported
rename behavior, or an open-handle sharing violation causes a skip rather than
a fallback copy-and-delete.

Only after a source is successfully quarantined may its trash payload be
unlinked. A crash before quarantine leaves the source visible; a crash after
quarantine leaves an unreachable, inspectable tombstone; and a crash during
unlink leaves only trash debris. Recovery never guesses that an unrecognized
trash entry is disposable. Directory cleanup is handle-relative and never
follows symbolic links, junctions, mount points, or other reparse entries.
Those entries may be unlinked as entries only when the validated deletion unit
contains them. No operation requires creating a symlink, and Windows sharing or
rename failures leave the source or tombstone for a later plan.

Quarantine provides atomic visibility, not a stronger power-loss promise than
ADR 0003. Durable mode must synchronize the tombstone, rename parent, and
affected directory metadata only where the platform adapter has established
that capability. Unsupported durability is reported and never described as a
successful durable deletion.

### Corrupt, unknown, and unsafe state

Collection fails closed at the smallest scope whose reachability is no longer
provable. An unknown root format or unsafe root blocks the entire cache. An
unknown or corrupt ref, snapshot manifest, or unexpected repository structure
blocks snapshot and blob deletion for that repository. An unknown compatible
binding or unclassified snapshot entry blocks compatible physical-blob
deletion for that repository. An unknown partial, staging entry, or tombstone
is retained rather than recursively removed. A blocker in one validated
repository does not authorize deletion there, but it need not prevent a fully
independent repository from being planned and revalidated.

All traversal is rooted, component-safe, and no-follow. Escaping or dangling
links, special files, substituted ancestors, unexpected reparse points,
case-colliding names, and entries that change type are blockers and are
reported. Recursive deletion never follows an entry outside the validated
deletion unit.

## Consequences

- Dry runs are useful for review but never serve as deletion authority without
  a fresh reachability scan and exclusive coordination.
- Valid refs, caller-retained snapshots, and active readers preserve their
  complete transitive content; a corrupt record loses space reclamation rather
  than risking data loss.
- Shared Python roots can be inspected normally, while deleting Python-visible
  content requires an explicit quiescent maintenance context that hf-store
  cannot infer on Python's behalf.
- Same-volume trash sequencing makes interrupted cleanup inspectable and keeps
  blobs available until their last snapshot edge is gone.
- Conservative timestamps, whole-unit limits, and deterministic ordering make
  behavior testable across Linux, macOS, and Windows, at the cost of sometimes
  retaining reclaimable bytes.
- This ADR accepts the future behavior only. It does not implement deletion or
  establish an owned-cache or compatible-cache GC support claim.
