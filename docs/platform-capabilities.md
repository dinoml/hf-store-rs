# Platform filesystem capabilities

The v0.1 cache contract provides **atomic visibility**, not a general
power-loss durability guarantee. Linux, macOS, and Windows run the same
observable lock, staging, replacement, reader-lease, crash-boundary, and
cross-process writer tests in CI.

## Established capabilities

| Capability | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Advisory shared/exclusive file locks | Tested | Tested | Tested |
| Lock release on process exit | Tested | Tested | Tested |
| Create-once immutable publication | Tested | Tested | Tested |
| Atomic replacement of mutable records | Tested | Tested | Tested |
| Same-volume hard-link snapshot staging | Attempted with validated copy fallback | Attempted with validated copy fallback | Attempted with validated copy fallback |
| Symlink-free snapshot and `local_dir` operation | Tested | Tested | Tested |
| Directory metadata synchronization | Supported adapter operation | Supported adapter operation | No power-loss flush claim |

Every published file is completed and synchronized before it enters a visible
namespace. Immutable blobs and snapshots are installed create-once. Mutable
refs and completion records are replaced atomically only after their referenced
content is complete. Readers ignore staging and partial namespaces and hold a
shared lease while using snapshot paths.

## Durability boundary

Successful publication means concurrent readers observe either the old
complete state or the new complete state. It does not mean every ancestor
directory entry is guaranteed to survive sudden power loss. In particular:

- newly created ancestor chains are not synchronized top-down;
- Windows currently exposes no directory-flush guarantee through the selected
  safe filesystem adapter; and
- filesystem, mount, virtualization, and storage-controller behavior can be
  weaker than the operating-system API contract.

Accordingly, v0.1 does not expose a "durable" mode or report durable success.
Interrupted operations may leave inert staging, partial, lock, or trash entries
for later inspection and conservative cleanup, but normal lookup cannot treat
them as completed content.

## CI evidence

The `Test (ubuntu-latest)`, `Test (macos-latest)`, and
`Test (windows-latest)` jobs exercise the platform-neutral contract. Separate
three-platform Rust/Python conformance and mixed-writer jobs exercise compatible
cache publication and process-exit behavior against the pinned
`huggingface_hub` baseline. Any future durability claim requires a separate
capability decision and dedicated power-loss-oriented evidence.
