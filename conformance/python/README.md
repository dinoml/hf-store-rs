# Pinned Python cache conformance

This lane runs separately from the Rust test suite. It verifies the exact
`huggingface_hub` v1.24.0 source commit, validates the generated fixture
provenance, and exercises the pinned Python cache readers without network
access.

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
```

The checkout must be at commit
`36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`, carry tag `v1.24.0`, and be
the source of the imported package. The harness verifies the package version,
source commit, source cleanliness, import path, and recorded Git blob IDs for
the upstream writer modules.

The portable comparison requires exact generated paths, entry types, symlink
targets, and file bytes. Only the source-maintained `README.md` and
`generate.py` at the fixture root are excluded.

Passing this lane means the pinned Python readers accept the Python-written
corpus. It does not yet claim that Python can read Rust-written cache entries or
that Rust can completely import the Python cache; those remain separate roadmap
gates.
