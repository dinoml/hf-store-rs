# ADR 0004: Transport runtime, TLS, proxy, and feature policy

- Status: Accepted
- Date: 2026-07-21

## Context

RFC 0001 requires asynchronous, bounded, resumable HTTP transfers without
exposing an HTTP client or async runtime in the public contract. It also
requires explicit request-time authentication, safe cross-origin redirects,
custom endpoints, deterministic tests, and a cache-only path that cannot
construct or invoke a network transport.

The HTTP stack's defaults are not sufficient as a contract. Reqwest enables
automatic redirects, system proxy discovery, and an implicitly selected TLS
configuration through its default features. Those choices introduce ambient
configuration and do not enforce hf-store's definition of a trusted origin.
TLS and proxy behavior must therefore be selected deliberately before
transport-backed APIs are exposed.

## Decision

The initial production adapter uses the asynchronous `reqwest` 0.13 client on
Tokio 1. Both dependencies remain implementation details. Public values,
errors, request bodies, response bodies, cancellation, and progress reporting
must not expose Reqwest, Tokio, Hyper, HTTP, URL, Rustls, or certificate-library
types.

The library does not create, own, or enter a Tokio runtime and never calls
`block_on`. Network operations run on the caller's entered Tokio runtime.
Internal concurrent work is bounded and joined to its request; it is not
detached onto process-global tasks. Clock, cancellation, transport, and body
effects remain privately substitutable for deterministic tests. Cache-only
operations do not require a Tokio runtime.

When transport is introduced, Cargo exposes one additive capability feature
named `network`, enabled by default. Cache parsing, compatible-cache reuse,
offline lookup, inspection, verification, and local materialization continue to
compile with `--no-default-features`. The `network` feature selects the private
production adapter; it does not change cache formats or result semantics.

Reqwest is built with default features disabled. The initial adapter enables
its Rustls backend, its default AWS-LC cryptographic provider, platform
certificate verification, and HTTP/2, but not native TLS, the blocking client,
cookies, automatic content decompression, SOCKS, HTTP/3, or system-proxy
support. Tokio uses only the runtime, synchronization, and time capabilities
needed by the library; macros and a multi-thread runtime are test or application
choices. Dependency features may be widened internally when required, but no
feature named after a transport, runtime, or TLS implementation becomes part of
the crate's public feature surface. Locked dependency versions must continue to
pass Rust 1.85 and the three operating-system jobs.

Rustls certificate and hostname verification are always enabled. The default
verifier uses the operating system's trust configuration through Reqwest's
Rustls platform verifier; it does not use the native-TLS/OpenSSL backend. v0.1
does not accept custom CA bundles or client identities and offers no option to
disable certificate or hostname verification. Trust-store loading is deferred
with all other client construction, so cache-only requests do not read it.

The canonical endpoint is HTTPS. Explicit custom HTTP endpoints remain useful
for public, local, and test services, but the production adapter rejects bearer
authentication over plaintext HTTP. The hermetic fixture may use a private
test-only allowance to exercise request headers over loopback. An HTTPS request
is never redirected to HTTP.

Library proxy behavior is explicit and deterministic:

- the production client calls Reqwest's `no_proxy` configuration, so it does
  not read system proxy settings or `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, or
  `NO_PROXY`;
- the v0.1 transport configuration accepts at most one explicit HTTP or HTTPS
  forward proxy, which applies to all outbound origins including redirected
  download origins;
- proxy URLs containing user information are rejected, and v0.1 does not
  support proxy authentication; any later support uses a separate redacted
  request-time secret value;
- PAC, SOCKS, per-origin bypass rules, and ambient proxy discovery are deferred;
  a future CLI may discover environment configuration and pass an explicit
  library value after its own ADR is accepted.

Reqwest automatic redirects are disabled. A private redirect loop handles only
HTTP `301`, `302`, `303`, `307`, and `308` responses for the Hub's `GET` and
`HEAD` requests, resolves relative `Location` values, rejects malformed or
non-HTTP(S) targets, detects loops, and permits at most ten hops. HTTPS-to-HTTP
downgrades are rejected. Cookies and automatic referrer headers are disabled.

Trust is evaluated for every redirect target as the exact URL origin tuple of
scheme, normalized host, and effective port. The bearer `Authorization` header
is constructed per request and attached only when that target origin equals the
configured Hub endpoint origin. It is never stored in client-wide default
headers and is never copied to another origin. Proxy authorization is distinct
from endpoint authorization. Redirect URLs, including signed query strings,
are treated as secrets for diagnostics and are never written to cache metadata,
errors, logs, or progress events. Range and validator headers may be retained
only when the transfer state machine proves they still describe the same target
identity.

Store construction records validated transport configuration but does not
construct a client. A private, per-store transport factory creates and shares a
client only after a network-enabled request reaches its first transport
operation. Cache-only resolution follows a separate branch that cannot invoke
that factory. Tests must cover both a factory that fails if constructed and a
transport that fails if called. With `network` disabled, requesting network
access fails with a safe backend-unavailable classification rather than falling
back to ambient behavior.

This ADR fixes implementation policy and behavioral boundaries. It does not
accept public builder names, method signatures, error enums, proxy types, or
certificate types; those remain private until the corresponding Phase 2
behavior is implemented and tested.

## Consequences

- Network users run fetch futures inside Tokio, while public values remain
  independent of Tokio and Reqwest types.
- The default build works against the canonical Hub without an OpenSSL or
  platform-native TLS backend and honors roots trusted by the host platform.
- Enterprise proxies require explicit configuration; proxy environment and
  operating-system proxy settings are not silently inherited.
- Private endpoints and interception proxies must chain to a root already
  trusted by the host; custom CA bundles and proxy authentication are deferred.
- Redirect behavior can be tested hop by hop and cannot leak a bearer token to
  a CDN or another origin.
- `--no-default-features` provides a smaller cache-only build, but cache-only
  correctness is still tested in the default build where a transport exists.
- Supporting another runtime, HTTP backend, native trust store, SOCKS/PAC, or
  insecure TLS mode requires a later ADR and conformance coverage; v0.1 makes no
  such backend claim.

## References

- [Reqwest 0.13.4 client documentation](https://docs.rs/reqwest/0.13.4/reqwest/)
- [Tokio runtime documentation](https://docs.rs/tokio/1/tokio/runtime/)
- [RFC 0001](../rfcs/0001-hub-store-v0.1.md)
