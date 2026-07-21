# ADR 0008: Initial command-line interface and output contract

- Status: Accepted
- Date: 2026-07-21

## Context

RFC 0001 defers a command-line interface until its operational scope,
credential discovery, destructive-operation safeguards, exit statuses, and
machine-readable output are fixed. The library deliberately has no ambient
authentication or configuration. A CLI, however, must interoperate with the
normal Hugging Face cache and credential locations without moving that ambient
policy into the reusable library.

The CLI also exposes operations with different safety properties. Fetch may
contact a remote endpoint or reconcile a mutable user-owned directory;
inspection and verification are read-only; and garbage collection separates a
read-only plan from destructive execution. Human progress, composable command
output, and stable automation output cannot share one undifferentiated stream.

## Decision

### Packaging and boundary

The executable is named `hf-store`. It is delivered by a separate workspace
package named `hf-store-cli`, which has a binary target and no library target.
The `hf-store` library does not depend on the CLI package, its argument parser,
or its presentation and configuration dependencies. No `cli` Cargo feature is
added to the library; the library retains the `network` capability boundary
accepted by ADR 0004.

The initial CLI package depends on the network-enabled library so one binary
can perform both online and cache-only operations. Selecting cache-only mode is
a runtime invariant: it follows a construction path that cannot create or call
the transport even though transport code is present in the binary. The CLI
uses documented library seams once those seams are accepted and implemented;
it does not make private cache or transport types public merely to reach them.

There is no persisted hf-store configuration file in v0.1. Argument parsing,
ambient discovery, presentation, process signals, and exit-status mapping live
only in the CLI package. The CLI resolves every discovered value into an
explicit library input. The library itself continues to read no environment
variables, home directories, credential files, terminal state, or process
configuration.

This ADR records a future observable contract. It does not establish that the
binary or any command is implemented or supported.

### Initial command surface

The initial surface contains only these operations:

1. `hf-store fetch` resolves and fetches one model, dataset, or Space
   repository selection. Repository kind is explicit, while an omitted
   revision means `main`. Repeated allow and ignore patterns use ADR 0005.
   Normal output is an immutable snapshot. An explicit `--local-dir` instead
   applies ADR 0006 to a user-owned destination. `--force` applies only to
   conflicting selected `local_dir` paths and cannot delete an unselected path,
   replace a non-empty directory, weaken validation, or affect GC.
2. `hf-store fetch --offline` performs the same logical request in strict
   cache-only mode. It does not resolve a mutable revision remotely, discover a
   token, initialize TLS or proxy state, or construct a transport. Missing,
   incomplete, stale, modified, corrupt, or wrong-selection content is an
   offline miss or validation failure rather than permission to contact the
   network. There is no separate `offline` command.
3. `hf-store inspect` performs a read-only cache scan and explains recognized,
   incomplete, corrupt, unknown-version, unsafe, staging, partial, compatible,
   and owned states. Findings are the result of inspection, so successfully
   producing a report exits successfully even when the report contains
   problems.
4. `hf-store verify` revalidates the selected cache, repository, snapshot, or
   `local_dir` scope. A complete verification report containing any invalid or
   incomplete finding is a negative finding rather than a failure to produce a
   report.
5. `hf-store gc plan` performs the read-only scan accepted by ADR 0007. It may
   display a report and may create a versioned plan through an explicit plan
   output path outside the selected cache root. The plan file uses create-new
   publication so an existing user file is not replaced. Planning never
   deletes, quarantines, refreshes, or otherwise mutates cache state. There are
   no implicit destructive retention defaults: without an explicit retention
   policy it reports reachability and blockers but schedules no removal.
6. `hf-store gc execute --plan <path> --yes` is the only destructive cache
   surface. It attempts only candidates already present in the supplied plan,
   after fresh coordination and revalidation under ADR 0007. `--yes` is a
   required, non-interactive confirmation and never bypasses a blocker, lease,
   fingerprint mismatch, retention rule, or containment check. A plan output
   path is created without replacing an existing user file. Deleting
   Python-visible compatible-cache state additionally requires an explicit
   exclusive-compatible-maintenance assertion; without it, execution is
   limited to eligible private sidecar, staging, and recognized trash state.

Inspect, verify, and GC never use the network and never discover credentials.
Commands may scope work to a cache root, repository identity, immutable commit,
selection, or local directory as applicable; omitting a narrower scope means
the chosen cache root. Upload, login, logout, token management, repository
creation, installation, activation, model execution, automatic quota eviction,
background maintenance, pruning of `local_dir`, and arbitrary file deletion
remain out of scope.

### Cache and operational configuration

Command-line values take precedence over documented environment values, which
take precedence over fixed defaults. No undocumented environment or config-file
fallback is allowed.

For the canonical `https://huggingface.co` endpoint, the default cache mode is
the Hub-compatible view. Its root follows the pinned Python precedence:
`--cache-dir`, `HF_HUB_CACHE`, legacy `HUGGINGFACE_HUB_CACHE`, then `hub` below
`HF_HOME`, `XDG_CACHE_HOME/huggingface`, or `~/.cache/huggingface`. This
discovery selects a path only; it does not weaken the conformance and sidecar
requirements in ADR 0002.

The owned view uses `--cache-dir`, then `HF_STORE_CACHE`, then `hf-store` below
the same resolved Hugging Face home directory. `--cache-mode compatible|owned`
overrides the endpoint-based default. A custom endpoint defaults to the owned
view so it cannot collide with canonical Hub repositories in a Python root.
Selecting the compatible view for a custom endpoint requires both
`--cache-mode compatible` and an explicit `--cache-dir`, which is the caller's
assertion that the root is dedicated to that endpoint as required by ADR 0002.

The endpoint defaults to `https://huggingface.co`; a different endpoint is a
command-line value rather than ambient configuration. Offline mode is selected
only by `--offline`; v0.1 does not silently inherit `HF_HUB_OFFLINE` or
`TRANSFORMERS_OFFLINE`. Likewise, proxy settings are accepted only through an
explicit CLI option that satisfies ADR 0004. The CLI does not read
`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`, PAC files, Git
configuration, dotenv files, or platform proxy settings. Filters,
concurrency, force, retention, and maintenance policy also have no ambient
source.

This deliberately small configuration boundary prevents the CLI from turning
library inputs into hidden process-global policy. Additional environment or
configuration sources require a later ADR and explicit precedence.

### CLI-only credential discovery

Only an online `fetch` discovers a bearer token. Exactly one source is selected
before the request; an authentication failure does not fall through to a
lower-precedence token. Precedence is:

1. `--no-token`, which disables all discovery;
2. `--token-file <path>`, where `-` means a bounded token read from standard
   input;
3. `HF_TOKEN`;
4. legacy `HUGGING_FACE_HUB_TOKEN`;
5. the file named by `HF_TOKEN_PATH`; or
6. `token` below the resolved `HF_HOME`, with the same home fallback used for
   the compatible cache.

`--no-token` and `--token-file` are mutually exclusive. A missing explicit
`--token-file` is a configuration error; a missing implicitly discovered
`HF_TOKEN_PATH` or default token file means anonymous access, matching the
pinned Python behavior. Any selected token file that exists but is unreadable,
empty, oversized, multiline, non-regular, or otherwise unsafe is a
configuration error. Token-file reads are bounded, avoid following a final
link or opening a special file, and retain only one non-empty token after
removing surrounding ASCII whitespace. Environment values follow the same
single-token and control-character validation. Users of a symlink-based or
external secret manager can pipe the secret through `--token-file -`.

There is intentionally no `--token <secret>` argument because process argument
lists and shell history commonly expose it. The CLI does not read Python's
multi-token file, refresh OAuth credentials, perform OIDC exchange, access
notebook secret stores, prompt for a secret, provide login/logout commands, or
write any token. Environment and file contents are converted to the library's
redacted request-time secret type and cleared from CLI-owned buffers as soon as
practical. ADR 0004 still governs plaintext authentication and redirect-origin
stripping.

### Non-interactive and destructive behavior

Every command is non-interactive. Missing authentication, an unsafe or
conflicting path, an existing plan output, and destructive GC without `--yes`
fail or report a blocker; none opens a prompt. Terminal attachment cannot
change cache, reconciliation, validation, confirmation, or exit-status
semantics.

`--force` and `--yes` are intentionally unrelated. `--force` grants only the
narrow selected-path replacement allowed by ADR 0006. `--yes` confirms an
already bounded GC plan but grants no additional deletion authority. Neither
option suppresses revalidation, path containment, locking, or leases.
Cooperative interruption returns cancellation and cannot activate a partial
snapshot, publish a completion record, or expand a GC plan. Operating-system
termination before the CLI can cooperate retains the atomic-visibility and
recovery guarantees of ADRs 0003, 0006, and 0007.

### Standard output, standard error, and progress

Human-readable output is the default and is not a stable parsing format.
Successful `fetch` writes the resulting snapshot or explicit `local_dir` path,
followed by one newline, to standard output. Inspect, verify, and GC write their
reports to standard output. Informational logs, warnings, retry notices, and
progress use standard error exclusively; logs never contaminate standard
output.

Interactive progress is enabled only when standard error is a terminal. It is
disabled for redirected standard error and in machine-readable mode. Terminal
detection may change presentation only. Initial v0.1 exposes no stable JSON
progress stream; adding one requires a separately versioned event schema.
Verbose diagnostics cannot relax redaction.

Broken output pipes are handled as output failures without a panic or a second
diagnostic containing sensitive state. Argument and configuration errors use
standard error. After machine-output initialization, operational results and
safe operational errors use the JSON envelope on standard output; standard
error remains reserved for failures that prevent that envelope from being
written.

### Stable machine-readable output

`--format json` emits exactly one UTF-8 JSON object followed by one newline on
standard output after successful argument and configuration parsing. It emits
no ANSI control sequences and no progress. The top-level envelope is:

```json
{
  "schema": "hf-store.cli.output",
  "version": 1,
  "command": "verify",
  "status": "ok",
  "classification": "ok",
  "exit_code": 0,
  "result": {},
  "error": null
}
```

`command` is one of `fetch`, `inspect`, `verify`, `gc-plan`, or `gc-execute`.
`status` is `ok`, `findings`, or `error`. Exactly one of `result` and `error` is
non-null. `classification` and `exit_code` match the exit-status table. A safe
error contains the same stable classification, a non-stable human message, and
only documented safe structured details such as retryability or a retry delay.
Automation must branch on the schema, version, status, classification, and exit
code rather than message text.

Each result is a command-specific version-one payload. At minimum, fetch binds
the repository identity, immutable commit, selection identity, destination
kind, destination path, and selected file digests; inspection reports
recognized entries, states, and summary counts; verification reports its
scope, validity, and findings; GC planning reports policy, blockers, stable
candidate order, logical-byte estimates, and a plan identity; and GC execution
reports that plan identity together with removed and skipped candidates and
estimated logical bytes. Exact payload fields are fixed by schema fixtures
before the corresponding command is claimed as implemented. Stable report
serialization does not make its private Rust representation public.

An executable GC plan uses a separate `hf-store.gc.plan` schema at version 1.
It contains only cache-relative identities and the observations required by
ADR 0007, not an absolute deletion path or deletion authority. Execution
requires a separately selected cache root, validates that root and plan
identity, and cannot add or substitute a target from either the plan or CLI.

Within a schema version, field meaning and JSON type do not change; required
fields are not removed. New optional fields may be added, and consumers must
ignore unknown fields. An incompatible change increments `version`. Object-key
order is not semantic, but implementations emit a fixed order for golden
tests. Arrays are sorted by canonical repository path, fixed-size object
identity, or the deterministic GC order in ADR 0007 rather than filesystem,
hash-map, or concurrent-completion order. Digests and fixed-size keys use
lowercase hexadecimal, repository paths use `/`, timestamps use UTC, and
numbers and messages are locale-independent. Given the same validated state,
policy, and supplied clock, reports and plan bytes are deterministic.

Malformed invocations that cannot establish `--format json`, failures while
reading configuration, and failures writing standard output are not guaranteed
to produce an envelope. They still follow the exit-status contract and never
emit secrets.

### Exit statuses

The following process exit statuses are stable across commands and operating
systems:

| Code | Classification | Meaning |
| ---: | --- | --- |
| 0 | `ok` | The operation completed; inspection may have reported states, verification was clean, and GC execution had no skip. |
| 1 | `findings` | A report completed with negative verification findings or a GC execution skipped at least one planned candidate. |
| 2 | `usage` | Invalid arguments, mutually exclusive options, unsafe or unreadable explicit configuration, or missing destructive confirmation. |
| 3 | `offline-miss` | The exact cache-only request is missing or incomplete, or its required cache backend is unavailable. |
| 4 | `access` | Authentication, authorization, or gated-repository access failed. |
| 5 | `not-found` | A remote repository, revision, or selected path does not exist. |
| 6 | `transport` | A network, rate-limit, redirect, HTTP protocol, retry-exhaustion, or network-backend failure occurred. |
| 7 | `validation` | Unsafe input, corrupt bytes or metadata, digest or size mismatch, unsupported format, or an unclassifiable cache state prevented the operation. |
| 8 | `conflict` | Local reconciliation or an explicit policy refused a conflicting existing object. |
| 9 | `busy` | Required coordination, an active lease, or concurrent change prevented the operation as a whole. Individual GC skips use code 1. |
| 10 | `io` | A local filesystem or process resource failure not represented by a safer classification occurred. |
| 11 | `cancelled` | Cooperative cancellation ended the operation. |
| 70 | `internal` | An internal invariant or otherwise unclassified software failure occurred. |

Inspect returns 0 when it successfully reports corrupt or unknown state because
discovering state is its purpose. Verify returns 1 for those findings. GC plan
returns 0 when it successfully reports blockers or zero candidates; GC execute
returns 1 if it safely skips any planned candidate. A command-wide inability
to acquire coordination returns 9. External signal conventions imposed by the
invoking shell or operating system are outside this table.

When an operation has multiple independent errors, its safe error records are
ordered by canonical logical path and fixed-size identity and the first record
determines the terminal classification; task scheduling or filesystem
enumeration order cannot choose the exit status. Cancellation takes precedence
once observed. Machine output repeats the matching string classification and
numeric exit status.

### Diagnostic and output security

Tokens, authorization and proxy-authorization headers, token-file contents and
paths, signed or redirected URLs, cookies, and raw HTTP header dumps never
appear in human output, JSON, plan files, progress, logs, errors, panic text, or
debug formatting. The configured endpoint may be reported only as its
validated origin, without user information, query, or fragment. Retry and
transport diagnostics use request roles and safe status classifications rather
than URLs.

Cache and verification records identify repository paths with validated POSIX
paths and filesystem entries with paths relative to the selected cache or
`local_dir` root. They do not expose home, temporary, staging, token, or cache
root absolute paths. The successful fetch destination is the deliberate
exception: it is the command's primary result and is returned so another
process can use it. A path explicitly selected as a GC plan output is not
echoed in diagnostics.

Untrusted metadata is never interpolated directly into terminal control
sequences or error text. Human rendering escapes control characters; JSON uses
normal JSON escaping. Debug or verbose modes retain the same redaction.
Credential-leak tests cover argument errors, environment and token-file
failures, redirects, retries, progress, verification findings, GC plans, JSON,
and panic boundaries before the CLI is released.

## Consequences

- Scripts can rely on one newline-terminated JSON envelope, deterministic
  ordering, and a small exit-status taxonomy without parsing human messages.
- Human users can pipe the fetched path or reports from standard output while
  progress and diagnostics remain on standard error.
- Strict `fetch --offline` works in the normal binary without reading secrets
  or touching network initialization.
- Normal Hugging Face cache and token locations remain convenient, but their
  discovery is confined to the CLI and has an explicit precedence.
- Refusing raw token arguments and interactive prompts makes automation safer
  at the cost of requiring an environment variable, a bounded token file, or
  standard input.
- GC requires an immutable plan, fresh revalidation, explicit confirmation,
  and a stronger assertion before any Python-visible deletion; convenience
  flags cannot weaken ADR 0007.
- A separate CLI package keeps parser, presentation, process, and ambient
  configuration policy out of library builds.

## References

- [RFC 0001](../rfcs/0001-hub-store-v0.1.md)
- [ADR 0001](0001-hub-transport-and-cache-boundary.md)
- [ADR 0002](0002-cache-identity-and-format.md)
- [ADR 0003](0003-cache-publication-and-coordination.md)
- [ADR 0004](0004-transport-runtime-tls-and-features.md)
- [ADR 0006](0006-local-directory-reconciliation-and-completion.md)
- [ADR 0007](0007-cache-garbage-collection.md)
- [Tracking issue #1](https://github.com/dinoml/hf-store-rs/issues/1)
- [`huggingface_hub` v1.24.0 credential discovery](https://github.com/huggingface/huggingface_hub/blob/36fd32c84d630f455a23b9a3bc4dc7b76d19cdde/src/huggingface_hub/utils/_auth.py)
- [`huggingface_hub` v1.24.0 cache and token paths](https://github.com/huggingface/huggingface_hub/blob/36fd32c84d630f455a23b9a3bc4dc7b76d19cdde/src/huggingface_hub/constants.py)
