# Security policy

## Supported versions

Security fixes are provided for the latest released minor version. Before
1.0, a fix may require a compatible migration or a documented breaking change.

## Reporting a vulnerability

Use [GitHub's private vulnerability reporting](https://github.com/dinoml/hf-store-rs/security/advisories/new).
Do not
open a public issue containing credentials, signed download URLs, private
repository names, or cache contents. Include the affected version, operating
system, cache mode, and a minimal reproduction with all secrets replaced.

## Security boundary

The library accepts bearer credentials only as explicit request-time values.
It never discovers or persists credentials. Redirect targets and response
headers are private transport state and are excluded from diagnostics,
progress, plans, and cache records. The CLI may discover a token according to
ADR 0008, but does not accept a raw token argument and clears CLI-owned token
buffers where practical.

All cache traversal is rooted below a caller-selected directory and rejects
unsafe components, links, reparse points, and unexpected file types at trust
boundaries. A validated snapshot lease coordinates cooperating hf-store
readers and garbage collection; it cannot prevent mutation by a
non-cooperating process. Never treat files from a mutable `local_dir` as
immutable after dropping its validated result.

`hf-store` downloads bytes; it does not execute model code, deserialize pickle,
or establish that downloaded content is safe to execute.
