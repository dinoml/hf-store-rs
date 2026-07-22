# v0.1 credential and signed-URL review

Reviewed on 2026-07-22 against the v0.1 release candidate.

| Surface | Representation rule | Automated evidence |
| --- | --- | --- |
| `AuthToken` | Redacted `Debug`; no `Display` or serialization | `endpoint_and_auth::authentication_tokens_are_always_redacted` |
| Requests and stores | Request debug reports only authorization presence; store debug reports only proxy presence | `api::tests::request_debug_redacts_authorization_and_public_values_are_send` |
| Redirects | Target URLs and selected headers remain private; authorization is stripped off-origin | `transport::tests::redirects_retain_bearer_only_for_the_exact_endpoint_origin` |
| Signed locations | No URL appears in transport errors, progress, reports, plans, or cache records | `transport::tests::diagnostics_and_validation_never_expose_targets_headers_or_tokens` |
| I/O causes | Underlying error text is replaced by stable classifications | `hub_cache`, `publication`, `local_dir_bookkeeping`, and `local_dir_materialization` secret-sentinel tests |
| Progress | Contains typed path, phase, counters, reuse, and retry state only | `progress::tests::progress_values_are_send_and_contain_only_safe_typed_state` |
| Reports and GC plans | Contain validated repository/cache identities and fixed-size digests, never auth or transport state | report serialization tests and `gc::tests::executable_plan_round_trip_rejects_tampering` |
| CLI | No raw token argument; bounded no-follow token files; stable errors do not echo values | `hf-store-cli` config and black-box tests |
| Proxy | Explicit validated endpoint only; user information, query, and fragment rejected; debug records presence only | explicit-proxy API and CLI tests |

The source audit searched every Rust `Debug`, `Display`, `Error`, serialization,
authorization, token, redirect, location, proxy, and URL reference. No public
diagnostic or persisted metadata type owns a raw response URL or authorization
header. `Endpoint` is intentionally printable configuration, but its parser
rejects URL user information, query strings, and fragments before construction.

Residual boundary: a caller can deliberately place sensitive text in a legal
repository identifier, revision, repository path, or endpoint base path. Those
are explicit public identity inputs and may appear in typed results. Callers
must not encode credentials in identity fields.

