# ADR 0002: Cache identity, interoperability, and format

- Status: Accepted
- Date: 2026-07-21

## Context

`huggingface_hub` provides the behavioral reference for repository resolution
and cached snapshots, but its cache names repositories from raw identifiers and
stores slash-containing revisions as nested paths. It also does not include the
Hub endpoint in repository identity.

Those choices alone cannot satisfy this repository's endpoint separation,
fixed-size internal identity, or complete-snapshot requirements. Nevertheless,
Rust and Python users need to reuse the same downloaded content without keeping
two full caches.

## Decision

`hf-store-rs` supports two cache views behind one logical cache contract:

1. a Hub-compatible view that reads and writes the `huggingface_hub` repository
   layout; and
2. an owned view for endpoints or features that cannot be represented safely in
   that layout.

Hub-compatible support is a conformance claim. It is not enabled or documented
as compatible until pinned fixtures from `huggingface_hub` pass bidirectional
tests. The canonical Hugging Face endpoint may share a normal Python cache root.
A custom endpoint may use the compatible layout only in an explicitly dedicated
root because the upstream directory identity does not contain an endpoint.

The compatibility fixture baseline is `huggingface_hub` v1.24.0 at commit
`36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`. The local development reference
was also inspected at commit `93b3b808f8b198c88799cdce1e9cbb1df5121597`;
observing a development head does not move the pinned conformance baseline.

The owned cache format is rooted at `hf-store-v1`. Every origin and repository
directory name is a lowercase SHA-256 key over a domain separator and a
canonical identity. The canonical repository identity includes the normalized
endpoint, repository kind, and validated repository identifier. Revisions are
also mapped to domain-separated fixed-size keys before they become path
components. Original validated values are retained only in versioned metadata.

Every blob has a logical lowercase SHA-256 identity over its exact bytes. In the
owned view that digest is also its physical address. In the Hub-compatible view,
the upstream ETag or object identifier remains the physical blob filename and a
versioned hf-store sidecar binds that file to its computed SHA-256 identity and
validated size. Git object identifiers, LFS OIDs, and other known remote digests
may be checked in addition to the local digest. ETags remain transport validators
unless a specific protocol proves digest semantics.

Private sidecar records use an explicit record kind and format version. Readers
reject corrupt records, the wrong record kind, and unknown versions. A selected
file set is identified by hashing a domain separator and a length-delimited,
lexicographically sorted list of validated repository paths. Materialization
case collisions are rejected before this identity is computed.

The owned version-one layout is:

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
      leases/
      trash/
```

When sharing a Hub-compatible root, hf-store metadata lives below each upstream
repository in a reserved `.hf-store/hf-store-v1` sidecar. A root-level sidecar
is forbidden because Python cache inspection treats unknown root entries as
corrupt repositories. Sidecar presence never makes an upstream snapshot
complete by itself. An hf-store lookup exposes a compatible snapshot only after
the sidecar manifest covers the requested selection and every referenced file
has been revalidated. Slash-containing upstream refs are mapped with a dedicated
safe-ref validator; the general `Revision` value is never blindly joined to a
host path.

The owned format and sidecar are private while the crate remains
non-publishable, but any change to an accepted field or path requires a new
metadata or layout version rather than silently reinterpreting existing state.

## Consequences

- Python and Rust can reuse entries in one compatible cache root after
  conformance and hf-store validation.
- Endpoint, repository, and revision text never becomes an untrusted host path
  component in the owned view; the compatible view uses separately validated
  upstream mappings and is limited where the upstream identity is ambiguous.
- Every published blob has one uniform local integrity identity even when the
  remote protocol exposes no trustworthy digest.
- Cache inspection can explain original identities from metadata without
  relying on reversible directory names.
- Upstream cache changes require pinned compatibility fixtures and may require a
  new adapter version without changing the owned layout.
