# Changelog

All notable changes will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and releases will use [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
after the first publishable version is approved.

## Unreleased

### Added

- Initial Rust workspace and quality gates.
- Accepted v0.1 RFC plus repository-boundary, cache-format, and publication ADRs.
- Contributor and security documentation.
- Validated repository, revision, commit, path, endpoint, and redacted-token
  value types.
- Internal versioned cache keys, layouts, metadata records, and process-crash-safe
  atomic-visibility publication primitives with deterministic failure tests.
- Initial pure path models for future shared `huggingface_hub` cache and
  `local_dir` conformance work.
