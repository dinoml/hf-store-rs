"""Regenerate the pinned upstream standard-cache and local_dir fixtures."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import huggingface_hub
from huggingface_hub._local_folder import (
    _create_cachedir_tag,
    get_local_download_paths,
    write_download_metadata,
)
from huggingface_hub._tree_cache import TreeCacheEntry, write_tree_cache
from huggingface_hub.file_download import (
    _cache_commit_hash_for_specific_revision,
    _create_symlink,
    repo_folder_name,
)


EXPECTED_VERSION = "1.24.0"
EXPECTED_COMMIT = "36fd32c84d630f455a23b9a3bc4dc7b76d19cdde"
EXPECTED_TAG = "v1.24.0"
LEGACY_METADATA_COMMIT = "0123456789abcdef0123456789abcdef01234567"
LOCAL_DIR_COMMIT = "4444444444444444444444444444444444444444"
LOCAL_DIR_METADATA_TIMESTAMP = 1_720_000_000.25
FIXTURE_DIRECTORY = Path(__file__).resolve().parent
WRITER_SOURCES = (
    "src/huggingface_hub/_local_folder.py",
    "src/huggingface_hub/_tree_cache.py",
    "src/huggingface_hub/file_download.py",
)


@dataclass(frozen=True)
class FileSpec:
    """A deterministic remote file represented by the standard-cache corpus."""

    path: str
    content: bytes
    snapshot_form: str
    lfs: bool = False


@dataclass(frozen=True)
class RepositorySpec:
    """A deterministic repository represented by the standard-cache corpus."""

    repo_type: str
    repo_id: str
    commit: str
    revisions: tuple[str, ...]
    files: tuple[FileSpec, ...]
    missing_paths: tuple[str, ...]


@dataclass(frozen=True)
class LocalDirFileSpec:
    """A deterministic remote file materialized into a local_dir fixture."""

    path: str
    content: bytes
    lfs: bool = False


REPOSITORIES = (
    RepositorySpec(
        repo_type="model",
        repo_id="fixture-model",
        commit="1111111111111111111111111111111111111111",
        revisions=("main",),
        files=(
            FileSpec(
                path="config.json",
                content=b'{\n  "architectures": ["FixtureModel"]\n}\n',
                snapshot_form="snapshot_only_regular",
            ),
        ),
        missing_paths=("missing/config.json",),
    ),
    RepositorySpec(
        repo_type="dataset",
        repo_id="fixture-org/fixture-dataset",
        commit="2222222222222222222222222222222222222222",
        revisions=("main", "refs/pr/7"),
        files=(
            FileSpec(
                path="data/train.jsonl",
                content=b'{"text":"first"}\n{"text":"second"}\n',
                snapshot_form="copied_regular_with_blob",
                lfs=True,
            ),
        ),
        missing_paths=("data/validation.jsonl",),
    ),
    RepositorySpec(
        repo_type="space",
        repo_id="fixture-org/fixture-space",
        commit="3333333333333333333333333333333333333333",
        revisions=("main",),
        files=(
            FileSpec(
                path="src/app.py",
                content=b'from pathlib import Path\n\nprint(Path("fixture"))\n',
                snapshot_form="relative_symlink_runtime",
            ),
        ),
        missing_paths=("assets/missing.css",),
    ),
)


LOCAL_DIR_FILES = (
    LocalDirFileSpec(
        path="config/fixture.json",
        content=b'{\n  "model_type": "fixture-local-dir"\n}\n',
    ),
    LocalDirFileSpec(
        path="weights/nested/model.safetensors",
        content=b"fixture-local-dir-lfs-bytes\x00\x01\x02\n",
        lfs=True,
    ),
)


def parse_args() -> argparse.Namespace:
    """Parse deterministic generator options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=FIXTURE_DIRECTORY,
        help="directory that receives provenance.json, inventory.json, and cache/",
    )
    parser.add_argument(
        "--runtime-symlinks",
        action="store_true",
        help="materialize the relative-symlink form (Unix only)",
    )
    return parser.parse_args()


def portable_text(path: Path) -> bytes:
    """Read a Python text-mode record with deterministic LF newlines."""
    return path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def normalize_text(path: Path) -> None:
    """Normalize a pinned upstream text writer's output to LF bytes."""
    path.write_bytes(portable_text(path))


def git_output(checkout: Path, *arguments: str) -> str:
    """Run a read-only Git query against the upstream checkout."""
    completed = subprocess.run(
        ("git", "-C", str(checkout), *arguments),
        check=True,
        capture_output=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def verify_upstream_checkout() -> tuple[Path, dict[str, object]]:
    """Verify package and source provenance before writing any fixture."""
    imported_module_root = Path(huggingface_hub.__file__).resolve().parent
    checkout = Path(
        git_output(imported_module_root, "rev-parse", "--show-toplevel")
    ).resolve()
    expected_module_root = (checkout / "src" / "huggingface_hub").resolve()
    if imported_module_root != expected_module_root:
        raise RuntimeError(
            "huggingface_hub was not imported from the pinned source tree"
        )

    if huggingface_hub.__version__ != EXPECTED_VERSION:
        raise RuntimeError(f"expected huggingface_hub {EXPECTED_VERSION}")

    git_commit = git_output(checkout, "rev-parse", "HEAD")
    if git_commit != EXPECTED_COMMIT:
        raise RuntimeError(f"expected huggingface_hub source commit {EXPECTED_COMMIT}")

    tags = git_output(checkout, "tag", "--points-at", "HEAD").splitlines()
    if EXPECTED_TAG not in tags:
        raise RuntimeError(f"expected source commit to carry tag {EXPECTED_TAG}")

    tracked_changes = git_output(
        checkout,
        "status",
        "--porcelain=v1",
        "--untracked-files=no",
        "--",
        "pyproject.toml",
        "src/huggingface_hub",
    )
    if tracked_changes:
        raise RuntimeError(
            "pinned huggingface_hub source files contain tracked changes"
        )

    writer_sources = {
        source: git_output(checkout, "rev-parse", f"HEAD:{source}")
        for source in WRITER_SOURCES
    }
    provenance = {
        "format_version": 1,
        "package": "huggingface_hub",
        "package_version": huggingface_hub.__version__,
        "git_commit": git_commit,
        "git_tag": EXPECTED_TAG,
        "writer_sources": writer_sources,
    }
    return checkout, provenance


def write_json(path: Path, value: object) -> None:
    """Write deterministic UTF-8 JSON with LF newlines."""
    path.write_bytes(
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )


def git_blob_id(content: bytes) -> str:
    """Return the Git blob identity used as a Hub ETag for non-LFS files."""
    header = f"blob {len(content)}\0".encode("ascii")
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def lfs_pointer_blob_id(content_sha256: str, size: int) -> str:
    """Return the Git blob identity of the canonical LFS pointer."""
    pointer = (
        "version https://git-lfs.github.com/spec/v1\n"
        f"oid sha256:{content_sha256}\n"
        f"size {size}\n"
    ).encode("ascii")
    return git_blob_id(pointer)


def relative_path(root: Path, posix_path: str) -> Path:
    """Join a fixture-controlled POSIX repository path to a host path."""
    return root.joinpath(*posix_path.split("/"))


def materialize_file(
    storage: Path,
    commit: str,
    spec: FileSpec,
    runtime_symlinks: bool,
) -> tuple[dict[str, object], TreeCacheEntry]:
    """Materialize one snapshot form through the pinned upstream helper."""
    content_sha256 = hashlib.sha256(spec.content).hexdigest()
    if spec.lfs:
        etag = content_sha256
        blob_id = lfs_pointer_blob_id(content_sha256, len(spec.content))
        tree_entry = TreeCacheEntry(
            size=len(spec.content),
            blob_id=blob_id,
            lfs_sha256=content_sha256,
            lfs_size=len(spec.content),
        )
    else:
        etag = git_blob_id(spec.content)
        tree_entry = TreeCacheEntry(size=len(spec.content), blob_id=etag)

    blob_path = storage / "blobs" / etag
    snapshot_path = relative_path(storage / "snapshots" / commit, spec.path)
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(spec.content)

    if spec.snapshot_form == "snapshot_only_regular":
        with patch(
            "huggingface_hub.file_download.are_symlinks_supported", return_value=False
        ):
            _create_symlink(str(blob_path), str(snapshot_path), new_blob=True)
    elif spec.snapshot_form == "copied_regular_with_blob":
        with patch(
            "huggingface_hub.file_download.are_symlinks_supported", return_value=False
        ):
            _create_symlink(str(blob_path), str(snapshot_path), new_blob=False)
    elif spec.snapshot_form == "relative_symlink_runtime":
        if runtime_symlinks:
            if os.name == "nt":
                raise RuntimeError(
                    "runtime relative symlinks are supported only on Unix"
                )
            with patch(
                "huggingface_hub.file_download.are_symlinks_supported",
                return_value=True,
            ):
                _create_symlink(str(blob_path), str(snapshot_path), new_blob=False)
    else:
        raise RuntimeError(f"unknown snapshot form {spec.snapshot_form}")

    file_inventory = {
        "path": spec.path,
        "etag": etag,
        "blob_id": tree_entry.blob_id,
        "lfs_sha256": tree_entry.lfs_sha256,
        "lfs_size": tree_entry.lfs_size,
        "size": len(spec.content),
        "content_sha256": content_sha256,
        "snapshot_form": spec.snapshot_form,
    }
    return file_inventory, tree_entry


def generate_standard_cache(output: Path, runtime_symlinks: bool) -> dict[str, object]:
    """Generate the complete portable standard-cache corpus."""
    cache_root = output / "cache"
    if cache_root.is_symlink() or (cache_root.exists() and not cache_root.is_dir()):
        raise RuntimeError(
            f"refusing to replace non-directory cache output {cache_root}"
        )
    if cache_root.exists():
        shutil.rmtree(cache_root)
    cache_root.mkdir(parents=True)
    _create_cachedir_tag(cache_root)
    normalize_text(cache_root / "CACHEDIR.TAG")

    repositories = []
    for spec in REPOSITORIES:
        cache_directory = repo_folder_name(
            repo_id=spec.repo_id, repo_type=spec.repo_type
        )
        storage = cache_root / cache_directory
        tree_entries = {}
        files = []

        for revision in spec.revisions:
            _cache_commit_hash_for_specific_revision(storage, revision, spec.commit)
            normalize_text(relative_path(storage / "refs", revision))

        for file_spec in spec.files:
            file_inventory, tree_entry = materialize_file(
                storage,
                spec.commit,
                file_spec,
                runtime_symlinks,
            )
            files.append(file_inventory)
            tree_entries[file_spec.path] = tree_entry

        write_tree_cache(str(storage), spec.commit, tree_entries)
        normalize_text(storage / "trees" / f"{spec.commit}.json")

        for missing_path in spec.missing_paths:
            marker = relative_path(storage / ".no_exist" / spec.commit, missing_path)
            marker.parent.mkdir(parents=True, exist_ok=True)
            # v1.24.0 records a known 404 with the same empty marker operation.
            marker.touch()

        repositories.append(
            {
                "repo_type": spec.repo_type,
                "repo_id": spec.repo_id,
                "cache_directory": cache_directory,
                "commit": spec.commit,
                "refs": [
                    {"revision": revision, "path": f"refs/{revision}"}
                    for revision in spec.revisions
                ],
                "tree_path": f"trees/{spec.commit}.json",
                "files": files,
                "missing_paths": list(spec.missing_paths),
            }
        )

    if not runtime_symlinks:
        prune_empty_directories(cache_root)

    return {
        "format_version": 1,
        "cache_root": "cache",
        "runtime_symlinks_materialized": runtime_symlinks,
        "repositories": repositories,
    }


def prune_empty_directories(root: Path) -> None:
    """Remove portable-output directories that Git cannot preserve."""
    directories = sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        if not any(directory.iterdir()):
            directory.rmdir()


def generate_local_dir(output: Path) -> dict[str, object]:
    """Generate a portable local_dir through the pinned upstream writers."""

    local_dir = output / "local-dir"
    if local_dir.is_symlink() or (local_dir.exists() and not local_dir.is_dir()):
        raise RuntimeError(
            f"refusing to replace non-directory local_dir output {local_dir}"
        )
    if local_dir.exists():
        shutil.rmtree(local_dir)
    local_dir.mkdir(parents=True)

    tree_entries = {}
    files = []
    for spec in LOCAL_DIR_FILES:
        content_sha256 = hashlib.sha256(spec.content).hexdigest()
        if spec.lfs:
            etag = content_sha256
            blob_id = lfs_pointer_blob_id(content_sha256, len(spec.content))
            tree_entry = TreeCacheEntry(
                size=len(spec.content),
                blob_id=blob_id,
                lfs_sha256=content_sha256,
                lfs_size=len(spec.content),
            )
        else:
            etag = git_blob_id(spec.content)
            blob_id = etag
            tree_entry = TreeCacheEntry(size=len(spec.content), blob_id=blob_id)

        file_path = relative_path(local_dir, spec.path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(spec.content)
        with patch(
            "huggingface_hub._local_folder.time.time",
            return_value=LOCAL_DIR_METADATA_TIMESTAMP,
        ):
            write_download_metadata(
                local_dir,
                filename=spec.path,
                commit_hash=LOCAL_DIR_COMMIT,
                etag=etag,
            )
        download_paths = get_local_download_paths(local_dir, spec.path)
        normalize_text(download_paths.metadata_path)
        # File-lock persistence differs by host and is not upstream cache metadata.
        if download_paths.lock_path.exists():
            download_paths.lock_path.unlink()

        tree_entries[spec.path] = tree_entry
        metadata_path = download_paths.metadata_path.relative_to(local_dir).as_posix()
        files.append(
            {
                "path": spec.path,
                "metadata_path": metadata_path,
                "etag": etag,
                "blob_id": blob_id,
                "lfs_sha256": tree_entry.lfs_sha256,
                "lfs_size": tree_entry.lfs_size,
                "size": len(spec.content),
                "content_sha256": content_sha256,
                "metadata_timestamp": LOCAL_DIR_METADATA_TIMESTAMP,
            }
        )

    cache_directory = local_dir / ".cache" / "huggingface"
    write_tree_cache(str(cache_directory), LOCAL_DIR_COMMIT, tree_entries)
    tree_path = cache_directory / "trees" / f"{LOCAL_DIR_COMMIT}.json"
    normalize_text(tree_path)
    normalize_text(cache_directory / ".gitignore")
    normalize_text(cache_directory / "CACHEDIR.TAG")

    return {
        "format_version": 1,
        "local_directories": [
            {
                "path": "local-dir",
                "repo_type": "model",
                "repo_id": "fixture-org/fixture-local-dir",
                "commit": LOCAL_DIR_COMMIT,
                "tree_path": tree_path.relative_to(local_dir).as_posix(),
                "gitignore_path": (cache_directory / ".gitignore")
                .relative_to(local_dir)
                .as_posix(),
                "cachedir_tag_path": (cache_directory / "CACHEDIR.TAG")
                .relative_to(local_dir)
                .as_posix(),
                "files": files,
            }
        ],
    }


def generate_legacy_metadata_records(output: Path) -> None:
    """Preserve the original isolated metadata-codec fixtures."""
    with TemporaryDirectory() as temporary_directory:
        temporary = Path(temporary_directory)
        standard = temporary / "standard"
        local_dir = temporary / "local-dir"

        _cache_commit_hash_for_specific_revision(
            standard, "main", LEGACY_METADATA_COMMIT
        )
        write_tree_cache(
            str(standard),
            LEGACY_METADATA_COMMIT,
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
        with patch(
            "huggingface_hub._local_folder.time.time", return_value=1_720_000_000.25
        ):
            write_download_metadata(
                local_dir,
                filename="nested/model.safetensors",
                commit_hash=LEGACY_METADATA_COMMIT,
                etag="3333333333333333333333333333333333333333333333333333333333333333",
            )

        (output / "standard-ref-main").write_bytes(
            (standard / "refs" / "main").read_bytes()
        )
        (output / "tree-v1.json").write_bytes(
            portable_text(standard / "trees" / f"{LEGACY_METADATA_COMMIT}.json")
        )
        (output / "local-dir-download.metadata").write_bytes(
            portable_text(
                local_dir
                / ".cache"
                / "huggingface"
                / "download"
                / "nested"
                / "model.safetensors.metadata"
            )
        )


def main() -> None:
    """Verify provenance and generate deterministic records."""
    arguments = parse_args()
    output = arguments.output.resolve()
    _, provenance = verify_upstream_checkout()

    output.mkdir(parents=True, exist_ok=True)
    inventory = generate_standard_cache(output, arguments.runtime_symlinks)
    local_dir_inventory = generate_local_dir(output)
    generate_legacy_metadata_records(output)
    write_json(output / "provenance.json", provenance)
    write_json(output / "inventory.json", inventory)
    write_json(output / "local-dir-inventory.json", local_dir_inventory)


if __name__ == "__main__":
    main()
