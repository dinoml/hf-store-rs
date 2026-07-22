# Pinned Python cache conformance

This lane runs separately from the Rust test suite. It verifies the exact
`huggingface_hub` v1.24.0 source commit, validates the generated fixture
provenance, and exercises the pinned Python standard-cache and `local_dir`
readers without network access.

From a Unix host with the pinned reference checkout installed from source:

```text
python crates/hf-store/tests/fixtures/huggingface_hub-v1.24.0/generate.py \
  --output <portable-temporary-directory>
python conformance/python/portable_fixture_comparison.py \
  --checked-in crates/hf-store/tests/fixtures/huggingface_hub-v1.24.0 \
  --generated <portable-temporary-directory>

python crates/hf-store/tests/fixtures/huggingface_hub-v1.24.0/generate.py \
  --output <temporary-directory> --runtime-symlinks
python conformance/python/cache_conformance.py \
  --reference-root <huggingface_hub-checkout> \
  --inventory <temporary-directory>/inventory.json

HF_STORE_PYTHON_CONFORMANCE_INVENTORY=<temporary-directory>/inventory.json \
  cargo test -p hf-store --lib \
  cache::python_cache_conformance::python_written_cache_reuses_every_repository_without_source \
  --locked -- --ignored --exact

HF_STORE_CONFORMANCE_OUTPUT=<rust-temporary-directory> \
  cargo test -p hf-store --lib \
  cache::standard_cache::tests::emit_python_conformance_fixture \
  --locked -- --ignored --exact
python conformance/python/rust_writer_conformance.py \
  --reference-root <huggingface_hub-checkout> \
  --inventory <rust-temporary-directory>/inventory.json
```

The checkout must be at commit
`36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`, carry tag `v1.24.0`, and be
the source of the imported package. The harness verifies the package version,
source commit, source cleanliness, import path, and recorded Git blob IDs for
the upstream writer modules.

The portable comparison requires exact generated paths, entry types, symlink
targets, and file bytes. Only the source-maintained `README.md` and
`generate.py`, plus their transient root `__pycache__/`, are excluded. This
includes the complete `local-dir/` tree and `local-dir-inventory.json`.

The `local_dir` check copies the deterministic checked-in tree to a temporary
directory and sets each real file's mtime to its recorded three-line metadata
timestamp. It then exercises `read_download_metadata`, `read_tree_cache`,
`get_cached_repo_tree`, and
`snapshot_download(..., local_dir=..., local_files_only=True, token=False)`.
The repository-info method is patched to fail if that offline call attempts a
network lookup.

Passing the Python-writer lane means the pinned Python readers accept the
Python-written corpus and Rust can import its exact model, dataset, and Space
selections, reopen them offline, and republish them without opening a download
source. Passing the Rust-writer matrix means the pinned Python readers also
accept Rust-written snapshots on Linux, macOS, and Windows, including Git and
LFS identities and slash-containing refs. These lanes do not yet claim complete
shared-cache race safety, Rust import of `local_dir` state, or compatibility
with unpinned `huggingface_hub` versions; those remain separate roadmap gates.
