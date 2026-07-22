# Changelog

All notable user-visible changes are recorded here. The project follows
[Semantic Versioning](https://semver.org/) for its public Rust and CLI
contracts.

## 0.1.0 - 2026-07-22

- Add typed asynchronous Hub planning and acquisition for model, dataset, and
  Space repositories.
- Add owned and `huggingface_hub` v1.24.0-compatible cache modes.
- Add strict transport-free offline lookup and independent `local_dir`
  materialization.
- Add resumable, validated, bounded-concurrency transfers with cancellation and
  structured progress.
- Add cache inventory, verification, immutable GC plans, conservative GC
  execution, and the thin `hf-store` CLI.
