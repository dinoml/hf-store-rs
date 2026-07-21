"""Regenerate the pinned upstream metadata fixtures."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import huggingface_hub
from huggingface_hub._local_folder import write_download_metadata
from huggingface_hub._tree_cache import TreeCacheEntry, write_tree_cache
from huggingface_hub.file_download import _cache_commit_hash_for_specific_revision


EXPECTED_VERSION = "1.24.0"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
FIXTURE_DIRECTORY = Path(__file__).parent


def portable_text(path: Path) -> bytes:
    """Read a Python text-mode record with deterministic LF newlines."""
    return path.read_bytes().replace(b"\r\n", b"\n")


def main() -> None:
    """Generate deterministic records using the pinned Python writers."""
    if huggingface_hub.__version__ != EXPECTED_VERSION:
        raise RuntimeError(f"expected huggingface_hub {EXPECTED_VERSION}")

    with TemporaryDirectory() as temporary_directory:
        temporary = Path(temporary_directory)
        standard = temporary / "standard"
        local_dir = temporary / "local-dir"

        _cache_commit_hash_for_specific_revision(str(standard), "main", COMMIT)
        write_tree_cache(
            str(standard),
            COMMIT,
            {
                "config.json": TreeCacheEntry(
                    size=5,
                    blob_id="1111111111111111111111111111111111111111",
                ),
                "nested/model.safetensors": TreeCacheEntry(
                    size=42,
                    blob_id="2222222222222222222222222222222222222222",
                    lfs_sha256="3333333333333333333333333333333333333333333333333333333333333333",
                    lfs_size=1024,
                    xet_hash="4444444444444444444444444444444444444444444444444444444444444444",
                ),
            },
        )
        with patch("huggingface_hub._local_folder.time.time", return_value=1_720_000_000.25):
            write_download_metadata(
                local_dir,
                filename="nested/model.safetensors",
                commit_hash=COMMIT,
                etag="3333333333333333333333333333333333333333333333333333333333333333",
            )

        (FIXTURE_DIRECTORY / "standard-ref-main").write_bytes(
            (standard / "refs" / "main").read_bytes()
        )
        (FIXTURE_DIRECTORY / "tree-v1.json").write_bytes(
            portable_text(standard / "trees" / f"{COMMIT}.json")
        )
        (FIXTURE_DIRECTORY / "local-dir-download.metadata").write_bytes(
            portable_text(
                local_dir
                / ".cache"
                / "huggingface"
                / "download"
                / "nested"
                / "model.safetensors.metadata"
            )
        )


if __name__ == "__main__":
    main()
