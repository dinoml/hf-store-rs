# ADR 0001: Hub transport and cache boundary

- Status: Accepted
- Date: 2026-07-21

## Context

DinoML needs reusable access to Hub-hosted models, datasets, processors,
adapters, and compiler inputs. Repository acquisition is useful independently of
model execution and is also needed by non-DinoML consumers.

Combining acquisition with application installation or device runtime policy
would make the library depend on private lifecycle and execution concepts.

## Decision

`hf-store-rs` owns Hub repository identity, revision resolution, request-time
authentication, transfer, byte validation, cache mechanics, immutable snapshots,
offline lookup, inspection, and garbage collection.

It remains independent of DinoML-internal crates.

Application-managed installation, artifact selection, package activation,
rollback, model lifecycle, device placement, and execution remain in the DinoML
service boundary. Model configuration and weight-format interpretation belong in
their own auxiliary packages.

## Consequences

- The crate can be reused by models, datasets, processors, adapters, importers,
  and unrelated Rust applications.
- A successful fetch means bytes are locally complete and validated; it does not
  mean a model or package is installable or executable.
- Public APIs cannot expose DinoML artifact, scheduler, tensor, or session types.
- The cache may record repository facts but not application installation state.
- Application policy may build on snapshots without changing their immutability.
