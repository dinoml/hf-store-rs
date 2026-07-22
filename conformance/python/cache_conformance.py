"""Run offline cache-reader checks against the pinned Python implementation.

This is intentionally separate from the Rust test suite.  It validates the
Python-written fixture corpus with Python's own readers; it does not establish
that the unfinished Rust compatible-cache writer is bidirectionally compatible.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import string
import subprocess
import sys
from tempfile import TemporaryDirectory
from typing import Any, Callable, Sequence
from unittest.mock import patch


EXPECTED_HUGGINGFACE_HUB_VERSION = "1.24.0"
EXPECTED_HUGGINGFACE_HUB_COMMIT = "36fd32c84d630f455a23b9a3bc4dc7b76d19cdde"
EXPECTED_HUGGINGFACE_HUB_TAG = "v1.24.0"
INVENTORY_FORMAT_VERSION = 1
PROVENANCE_FORMAT_VERSION = 1
REQUIRED_WRITER_SOURCES = frozenset(
    {
        "src/huggingface_hub/file_download.py",
        "src/huggingface_hub/_local_folder.py",
        "src/huggingface_hub/_tree_cache.py",
    }
)
SNAPSHOT_FORMS = frozenset(
    {
        "snapshot_only_regular",
        "copied_regular_with_blob",
        "relative_symlink_runtime",
    }
)


class ConformanceError(RuntimeError):
    """A deterministic conformance precondition or assertion failed."""


@dataclass(frozen=True)
class RefFixture:
    """One symbolic revision and its repository-relative ref record."""

    revision: str
    path: PurePosixPath


@dataclass(frozen=True)
class FileFixture:
    """Expected content and representation of one cached snapshot file."""

    path: PurePosixPath
    etag: str
    blob_id: str
    size: int
    content_sha256: str
    snapshot_form: str


@dataclass(frozen=True)
class RepositoryFixture:
    """A Python-written standard-cache repository fixture."""

    repo_type: str
    repo_id: str
    cache_directory: PurePosixPath
    commit: str
    refs: tuple[RefFixture, ...]
    tree_path: PurePosixPath
    files: tuple[FileFixture, ...]
    missing_paths: tuple[PurePosixPath, ...]


@dataclass(frozen=True)
class Inventory:
    """Versioned inventory for a generated standard-cache corpus."""

    cache_root: PurePosixPath
    runtime_symlinks_materialized: bool
    repositories: tuple[RepositoryFixture, ...]
    producer: str | None = None


@dataclass(frozen=True)
class LocalDirFileFixture:
    """Expected content and pinned download metadata for one local_dir file."""

    path: PurePosixPath
    metadata_path: PurePosixPath
    etag: str
    blob_id: str
    lfs_sha256: str | None
    lfs_size: int | None
    size: int
    content_sha256: str
    metadata_timestamp: float


@dataclass(frozen=True)
class LocalDirFixture:
    """One Python-written local_dir and its cached repository tree."""

    path: PurePosixPath
    repo_type: str
    repo_id: str
    commit: str
    tree_path: PurePosixPath
    gitignore_path: PurePosixPath
    cachedir_tag_path: PurePosixPath
    files: tuple[LocalDirFileFixture, ...]


@dataclass(frozen=True)
class LocalDirInventory:
    """Versioned inventory for generated Python local_dir fixtures."""

    local_directories: tuple[LocalDirFixture, ...]


@dataclass(frozen=True)
class PythonReaders:
    """Pinned upstream cache-reader entry points used by this lane."""

    try_to_load_from_cache: Callable[..., object]
    snapshot_download: Callable[..., str]
    scan_cache_dir: Callable[..., object]
    read_tree_cache: Callable[[str, str], object]
    read_download_metadata: Callable[[Path, str], object]
    get_cached_repo_tree: Callable[..., list[object]]
    cached_no_exist: object


def validate_upstream_identity(
    *, package_version: str, source_commit: str, source_is_clean: bool
) -> None:
    """Require the accepted package version and exact unmodified source commit."""

    if package_version != EXPECTED_HUGGINGFACE_HUB_VERSION:
        raise ConformanceError(
            "unexpected huggingface_hub package version: "
            f"wanted {EXPECTED_HUGGINGFACE_HUB_VERSION}, got {package_version}"
        )
    if source_commit != EXPECTED_HUGGINGFACE_HUB_COMMIT:
        raise ConformanceError(
            "unexpected huggingface_hub source commit: "
            f"wanted {EXPECTED_HUGGINGFACE_HUB_COMMIT}, got {source_commit}"
        )
    if not source_is_clean:
        raise ConformanceError("the pinned huggingface_hub sources are modified")


def validate_imported_source(reference_root: Path, imported_module: Path) -> None:
    """Require the imported package to come from the checked reference source tree."""

    source_root = (reference_root / "src" / "huggingface_hub").resolve()
    imported_module = imported_module.resolve()
    try:
        imported_module.relative_to(source_root)
    except ValueError as error:
        raise ConformanceError(
            "huggingface_hub was not imported from the pinned reference checkout: "
            f"{imported_module}"
        ) from error


def _git(reference_root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(reference_root), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def _git_output(reference_root: Path, *arguments: str) -> str:
    completed = _git(reference_root, *arguments)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ConformanceError(f"could not inspect pinned upstream checkout: {detail}")
    return completed.stdout.strip()


def verify_reference_checkout(reference_root: Path, module: Any) -> None:
    """Verify git identity, source cleanliness, version, and import provenance."""

    reference_root = reference_root.resolve()
    checkout_root = Path(
        _git_output(reference_root, "rev-parse", "--show-toplevel")
    ).resolve()
    if checkout_root != reference_root:
        raise ConformanceError(
            f"reference root is not the checkout root: {reference_root} != {checkout_root}"
        )

    source_commit = _git_output(reference_root, "rev-parse", "HEAD")
    diff = _git(
        reference_root,
        "diff",
        "--quiet",
        "HEAD",
        "--",
        "pyproject.toml",
        "src/huggingface_hub",
    )
    if diff.returncode not in {0, 1}:
        detail = diff.stderr.strip() or diff.stdout.strip()
        raise ConformanceError(f"could not inspect pinned upstream sources: {detail}")

    package_version = getattr(module, "__version__", None)
    imported_file = getattr(module, "__file__", None)
    if not isinstance(package_version, str):
        raise ConformanceError(
            "huggingface_hub did not expose a string package version"
        )
    if not isinstance(imported_file, str):
        raise ConformanceError("huggingface_hub did not expose an import source path")

    validate_upstream_identity(
        package_version=package_version,
        source_commit=source_commit,
        source_is_clean=diff.returncode == 0,
    )
    validate_imported_source(reference_root, Path(imported_file))


def _object(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ConformanceError(f"{context} must be a JSON object with string keys")
    return value


def _array(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ConformanceError(f"{context} must be a JSON array")
    return value


def _string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConformanceError(f"{context} must be a non-empty string")
    return value


def _integer(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConformanceError(f"{context} must be a non-negative integer")
    return value


def _timestamp(value: object, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConformanceError(f"{context} must be a finite non-negative number")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ConformanceError(f"{context} must be a finite non-negative number")
    return timestamp


def _boolean(value: object, context: str) -> bool:
    if not isinstance(value, bool):
        raise ConformanceError(f"{context} must be a boolean")
    return value


def _relative_path(value: object, context: str) -> PurePosixPath:
    text = _string(value, context)
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or text != path.as_posix()
        or "\\" in text
        or ":" in text
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ConformanceError(f"{context} is not a normalized relative POSIX path")
    return path


def _hex(value: object, length: int, context: str) -> str:
    text = _string(value, context)
    if len(text) != length or any(
        character not in string.hexdigits for character in text
    ):
        raise ConformanceError(
            f"{context} must contain {length} hexadecimal characters"
        )
    if text != text.lower():
        raise ConformanceError(f"{context} must use lowercase hexadecimal")
    return text


def _required(record: dict[str, object], key: str, context: str) -> object:
    try:
        return record[key]
    except KeyError as error:
        raise ConformanceError(
            f"{context} is missing required field {key!r}"
        ) from error


def load_inventory(path: Path) -> Inventory:
    """Load and strictly validate the generated fixture inventory."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConformanceError(
            f"could not read fixture inventory {path}: {error}"
        ) from error
    root = _object(raw, "fixture inventory")
    format_version = _integer(
        _required(root, "format_version", "fixture inventory"),
        "fixture inventory format_version",
    )
    if format_version != INVENTORY_FORMAT_VERSION:
        raise ConformanceError(
            f"unsupported fixture inventory version {format_version}"
        )

    repositories: list[RepositoryFixture] = []
    identities: set[tuple[str, str]] = set()
    for index, raw_repository in enumerate(
        _array(_required(root, "repositories", "fixture inventory"), "repositories")
    ):
        context = f"repositories[{index}]"
        repository = _object(raw_repository, context)
        repo_type = _string(
            _required(repository, "repo_type", context), f"{context}.repo_type"
        )
        if repo_type not in {"model", "dataset", "space"}:
            raise ConformanceError(f"{context}.repo_type is unsupported: {repo_type}")
        repo_id = _string(
            _required(repository, "repo_id", context), f"{context}.repo_id"
        )
        identity = (repo_type, repo_id)
        if identity in identities:
            raise ConformanceError(
                f"duplicate repository fixture {repo_type}/{repo_id}"
            )
        identities.add(identity)

        refs: list[RefFixture] = []
        revisions: set[str] = set()
        for ref_index, raw_ref in enumerate(
            _array(_required(repository, "refs", context), f"{context}.refs")
        ):
            ref_context = f"{context}.refs[{ref_index}]"
            ref = _object(raw_ref, ref_context)
            revision = _string(
                _required(ref, "revision", ref_context), f"{ref_context}.revision"
            )
            if revision in revisions:
                raise ConformanceError(
                    f"duplicate revision fixture {repo_type}/{repo_id}@{revision}"
                )
            revisions.add(revision)
            refs.append(
                RefFixture(
                    revision=revision,
                    path=_relative_path(
                        _required(ref, "path", ref_context), f"{ref_context}.path"
                    ),
                )
            )

        files: list[FileFixture] = []
        file_paths: set[PurePosixPath] = set()
        for file_index, raw_file in enumerate(
            _array(_required(repository, "files", context), f"{context}.files")
        ):
            file_context = f"{context}.files[{file_index}]"
            file_record = _object(raw_file, file_context)
            file_path = _relative_path(
                _required(file_record, "path", file_context), f"{file_context}.path"
            )
            if file_path in file_paths:
                raise ConformanceError(
                    f"duplicate file fixture {repo_type}/{repo_id}:{file_path}"
                )
            file_paths.add(file_path)
            snapshot_form = _string(
                _required(file_record, "snapshot_form", file_context),
                f"{file_context}.snapshot_form",
            )
            if snapshot_form not in SNAPSHOT_FORMS:
                raise ConformanceError(
                    f"{file_context}.snapshot_form is unsupported: {snapshot_form}"
                )
            files.append(
                FileFixture(
                    path=file_path,
                    etag=_string(
                        _required(file_record, "etag", file_context),
                        f"{file_context}.etag",
                    ),
                    blob_id=_string(
                        _required(file_record, "blob_id", file_context),
                        f"{file_context}.blob_id",
                    ),
                    size=_integer(
                        _required(file_record, "size", file_context),
                        f"{file_context}.size",
                    ),
                    content_sha256=_hex(
                        _required(file_record, "content_sha256", file_context),
                        64,
                        f"{file_context}.content_sha256",
                    ),
                    snapshot_form=snapshot_form,
                )
            )

        missing_paths = tuple(
            _relative_path(value, f"{context}.missing_paths[{missing_index}]")
            for missing_index, value in enumerate(
                _array(
                    _required(repository, "missing_paths", context),
                    f"{context}.missing_paths",
                )
            )
        )
        if len(set(missing_paths)) != len(missing_paths):
            raise ConformanceError(f"{context}.missing_paths contains duplicates")
        if file_paths.intersection(missing_paths):
            raise ConformanceError(f"{context} marks a cached file as missing")

        repositories.append(
            RepositoryFixture(
                repo_type=repo_type,
                repo_id=repo_id,
                cache_directory=_relative_path(
                    _required(repository, "cache_directory", context),
                    f"{context}.cache_directory",
                ),
                commit=_hex(
                    _required(repository, "commit", context), 40, f"{context}.commit"
                ),
                refs=tuple(refs),
                tree_path=_relative_path(
                    _required(repository, "tree_path", context), f"{context}.tree_path"
                ),
                files=tuple(files),
                missing_paths=missing_paths,
            )
        )

    if not repositories:
        raise ConformanceError("fixture inventory must contain at least one repository")
    return Inventory(
        cache_root=_relative_path(
            _required(root, "cache_root", "fixture inventory"),
            "fixture inventory cache_root",
        ),
        runtime_symlinks_materialized=_boolean(
            _required(root, "runtime_symlinks_materialized", "fixture inventory"),
            "fixture inventory runtime_symlinks_materialized",
        ),
        repositories=tuple(repositories),
        producer=(
            _string(root["producer"], "fixture inventory producer")
            if "producer" in root
            else None
        ),
    )


def load_local_dir_inventory(path: Path) -> LocalDirInventory:
    """Load and strictly validate the generated local_dir fixture inventory."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConformanceError(
            f"could not read local_dir fixture inventory {path}: {error}"
        ) from error
    root = _object(raw, "local_dir fixture inventory")
    format_version = _integer(
        _required(root, "format_version", "local_dir fixture inventory"),
        "local_dir fixture inventory format_version",
    )
    if format_version != INVENTORY_FORMAT_VERSION:
        raise ConformanceError(
            f"unsupported local_dir fixture inventory version {format_version}"
        )

    local_directories: list[LocalDirFixture] = []
    local_paths: set[PurePosixPath] = set()
    for index, raw_local_directory in enumerate(
        _array(
            _required(root, "local_directories", "local_dir fixture inventory"),
            "local_directories",
        )
    ):
        context = f"local_directories[{index}]"
        local_directory = _object(raw_local_directory, context)
        local_path = _relative_path(
            _required(local_directory, "path", context), f"{context}.path"
        )
        if local_path in local_paths:
            raise ConformanceError(
                f"duplicate local_dir fixture path {local_path.as_posix()}"
            )
        local_paths.add(local_path)

        repo_type = _string(
            _required(local_directory, "repo_type", context),
            f"{context}.repo_type",
        )
        if repo_type not in {"model", "dataset", "space"}:
            raise ConformanceError(f"{context}.repo_type is unsupported: {repo_type}")
        repo_id = _string(
            _required(local_directory, "repo_id", context), f"{context}.repo_id"
        )
        commit = _hex(
            _required(local_directory, "commit", context), 40, f"{context}.commit"
        )
        tree_path = _relative_path(
            _required(local_directory, "tree_path", context),
            f"{context}.tree_path",
        )
        expected_tree_path = PurePosixPath(f".cache/huggingface/trees/{commit}.json")
        if tree_path != expected_tree_path:
            raise ConformanceError(
                f"{context}.tree_path does not match its commit: {tree_path}"
            )
        gitignore_path = _relative_path(
            _required(local_directory, "gitignore_path", context),
            f"{context}.gitignore_path",
        )
        if gitignore_path != PurePosixPath(".cache/huggingface/.gitignore"):
            raise ConformanceError(f"{context}.gitignore_path is not canonical")
        cachedir_tag_path = _relative_path(
            _required(local_directory, "cachedir_tag_path", context),
            f"{context}.cachedir_tag_path",
        )
        if cachedir_tag_path != PurePosixPath(".cache/huggingface/CACHEDIR.TAG"):
            raise ConformanceError(f"{context}.cachedir_tag_path is not canonical")

        files: list[LocalDirFileFixture] = []
        file_paths: set[PurePosixPath] = set()
        for file_index, raw_file in enumerate(
            _array(_required(local_directory, "files", context), f"{context}.files")
        ):
            file_context = f"{context}.files[{file_index}]"
            file_record = _object(raw_file, file_context)
            file_path = _relative_path(
                _required(file_record, "path", file_context),
                f"{file_context}.path",
            )
            if file_path in file_paths:
                raise ConformanceError(
                    f"duplicate local_dir fixture file {local_path}/{file_path}"
                )
            file_paths.add(file_path)
            metadata_path = _relative_path(
                _required(file_record, "metadata_path", file_context),
                f"{file_context}.metadata_path",
            )
            expected_metadata_path = PurePosixPath(
                ".cache/huggingface/download"
            ) / PurePosixPath(f"{file_path.as_posix()}.metadata")
            if metadata_path != expected_metadata_path:
                raise ConformanceError(
                    f"{file_context}.metadata_path does not match its file path"
                )

            raw_lfs_sha256 = _required(file_record, "lfs_sha256", file_context)
            lfs_sha256 = (
                None
                if raw_lfs_sha256 is None
                else _hex(raw_lfs_sha256, 64, f"{file_context}.lfs_sha256")
            )
            raw_lfs_size = _required(file_record, "lfs_size", file_context)
            lfs_size = (
                None
                if raw_lfs_size is None
                else _integer(raw_lfs_size, f"{file_context}.lfs_size")
            )
            if (lfs_sha256 is None) != (lfs_size is None):
                raise ConformanceError(
                    f"{file_context} must provide LFS SHA-256 and size together"
                )
            size = _integer(
                _required(file_record, "size", file_context),
                f"{file_context}.size",
            )
            content_sha256 = _hex(
                _required(file_record, "content_sha256", file_context),
                64,
                f"{file_context}.content_sha256",
            )
            etag = _string(
                _required(file_record, "etag", file_context), f"{file_context}.etag"
            )
            blob_id = _hex(
                _required(file_record, "blob_id", file_context),
                40,
                f"{file_context}.blob_id",
            )
            if lfs_sha256 is None:
                if etag != blob_id:
                    raise ConformanceError(
                        f"{file_context} regular ETag must equal its Git blob identity"
                    )
            elif etag != lfs_sha256 or content_sha256 != lfs_sha256 or size != lfs_size:
                raise ConformanceError(
                    f"{file_context} LFS ETag, content digest, and size disagree"
                )

            files.append(
                LocalDirFileFixture(
                    path=file_path,
                    metadata_path=metadata_path,
                    etag=etag,
                    blob_id=blob_id,
                    lfs_sha256=lfs_sha256,
                    lfs_size=lfs_size,
                    size=size,
                    content_sha256=content_sha256,
                    metadata_timestamp=_timestamp(
                        _required(file_record, "metadata_timestamp", file_context),
                        f"{file_context}.metadata_timestamp",
                    ),
                )
            )
        if not files:
            raise ConformanceError(f"{context} must contain at least one file")

        local_directories.append(
            LocalDirFixture(
                path=local_path,
                repo_type=repo_type,
                repo_id=repo_id,
                commit=commit,
                tree_path=tree_path,
                gitignore_path=gitignore_path,
                cachedir_tag_path=cachedir_tag_path,
                files=tuple(files),
            )
        )

    if not local_directories:
        raise ConformanceError(
            "local_dir fixture inventory must contain at least one local directory"
        )
    return LocalDirInventory(local_directories=tuple(local_directories))


def verify_fixture_provenance(path: Path, reference_root: Path) -> None:
    """Verify the corpus baseline and the exact pinned upstream source blobs."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConformanceError(
            f"could not read fixture provenance {path}: {error}"
        ) from error
    provenance = _object(raw, "fixture provenance")
    format_version = _integer(
        _required(provenance, "format_version", "fixture provenance"),
        "fixture provenance format_version",
    )
    if format_version != PROVENANCE_FORMAT_VERSION:
        raise ConformanceError(
            f"unsupported fixture provenance version {format_version}"
        )
    package = _string(_required(provenance, "package", "fixture provenance"), "package")
    if package != "huggingface_hub":
        raise ConformanceError(f"unexpected fixture provenance package {package}")
    validate_upstream_identity(
        package_version=_string(
            _required(provenance, "package_version", "fixture provenance"),
            "package_version",
        ),
        source_commit=_string(
            _required(provenance, "git_commit", "fixture provenance"),
            "git_commit",
        ),
        source_is_clean=True,
    )
    git_tag = _string(_required(provenance, "git_tag", "fixture provenance"), "git_tag")
    if git_tag != EXPECTED_HUGGINGFACE_HUB_TAG:
        raise ConformanceError(
            f"unexpected fixture provenance git tag {git_tag}; "
            f"wanted {EXPECTED_HUGGINGFACE_HUB_TAG}"
        )

    raw_sources = _object(
        _required(provenance, "writer_sources", "fixture provenance"),
        "fixture provenance writer_sources",
    )
    source_paths = set(raw_sources)
    missing_sources = REQUIRED_WRITER_SOURCES.difference(source_paths)
    if missing_sources:
        raise ConformanceError(
            "fixture provenance omits required upstream sources: "
            + ", ".join(sorted(missing_sources))
        )
    for source_path, raw_blob_id in raw_sources.items():
        normalized_source = _relative_path(
            source_path, f"writer_sources source path {source_path!r}"
        )
        expected_blob_id = _hex(raw_blob_id, 40, f"writer_sources[{source_path!r}]")
        actual_blob_id = _git_output(
            reference_root,
            "rev-parse",
            f"{EXPECTED_HUGGINGFACE_HUB_COMMIT}:{normalized_source.as_posix()}",
        )
        if actual_blob_id != expected_blob_id:
            raise ConformanceError(
                f"fixture provenance has the wrong Git blob for {source_path}: "
                f"{expected_blob_id} != {actual_blob_id}"
            )


def load_python_readers() -> tuple[Any, PythonReaders]:
    """Import only the pinned APIs needed by the separate conformance lane."""

    import huggingface_hub
    from huggingface_hub import (
        _CACHED_NO_EXIST,
        get_cached_repo_tree,
        scan_cache_dir,
        snapshot_download,
        try_to_load_from_cache,
    )
    from huggingface_hub._local_folder import read_download_metadata
    from huggingface_hub._tree_cache import read_tree_cache

    return huggingface_hub, PythonReaders(
        try_to_load_from_cache=try_to_load_from_cache,
        snapshot_download=snapshot_download,
        scan_cache_dir=scan_cache_dir,
        read_tree_cache=read_tree_cache,
        read_download_metadata=read_download_metadata,
        get_cached_repo_tree=get_cached_repo_tree,
        cached_no_exist=_CACHED_NO_EXIST,
    )


def _host_path(root: Path, path: PurePosixPath) -> Path:
    return root.joinpath(*path.parts)


def _digest(path: Path) -> str:
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                hasher.update(chunk)
    except OSError as error:
        raise ConformanceError(f"could not hash cached file {path}: {error}") from error
    return hasher.hexdigest()


def _assert_file(path: Path, fixture: FileFixture, context: str) -> None:
    if not path.is_file():
        raise ConformanceError(
            f"{context} did not return a regular cached file: {path}"
        )
    try:
        size = path.stat().st_size
    except OSError as error:
        raise ConformanceError(
            f"could not inspect cached file {path}: {error}"
        ) from error
    if size != fixture.size:
        raise ConformanceError(
            f"{context} returned size {size}, wanted {fixture.size}: {path}"
        )
    digest = _digest(path)
    if digest != fixture.content_sha256:
        raise ConformanceError(
            f"{context} returned SHA-256 {digest}, wanted {fixture.content_sha256}: {path}"
        )


def _assert_snapshot_representation(
    repo_directory: Path, commit: str, fixture: FileFixture
) -> None:
    snapshot = _host_path(repo_directory / "snapshots" / commit, fixture.path)
    blob = repo_directory / "blobs" / fixture.etag
    if fixture.snapshot_form == "snapshot_only_regular":
        if snapshot.is_symlink() or not snapshot.is_file():
            raise ConformanceError(
                f"snapshot-only fixture is not a regular file: {snapshot}"
            )
        if blob.exists() or blob.is_symlink():
            raise ConformanceError(
                f"snapshot-only fixture unexpectedly retained its blob: {blob}"
            )
        return

    if fixture.snapshot_form == "copied_regular_with_blob":
        if snapshot.is_symlink() or blob.is_symlink():
            raise ConformanceError(
                f"copied regular fixture unexpectedly contains a symlink: {snapshot}"
            )
        _assert_file(blob, fixture, "copied regular retained blob")
        try:
            aliases_blob = os.path.samefile(snapshot, blob)
        except OSError as error:
            raise ConformanceError(
                f"could not compare copied snapshot and retained blob: {error}"
            ) from error
        if aliases_blob:
            raise ConformanceError(
                f"copied regular snapshot aliases its retained blob: {snapshot}"
            )
        return

    if fixture.snapshot_form == "relative_symlink_runtime":
        if not snapshot.is_symlink():
            raise ConformanceError(
                f"runtime symlink fixture is not a symlink: {snapshot}"
            )
        try:
            target = Path(os.readlink(snapshot))
        except OSError as error:
            raise ConformanceError(
                f"could not read fixture symlink {snapshot}: {error}"
            ) from error
        if target.is_absolute():
            raise ConformanceError(
                f"fixture symlink target is not relative: {snapshot} -> {target}"
            )
        if snapshot.resolve() != blob.resolve():
            raise ConformanceError(
                f"fixture symlink does not resolve to its inventoried blob: {snapshot} -> {target}"
            )
        _assert_file(blob, fixture, "relative symlink blob")
        return

    raise ConformanceError(f"unhandled snapshot form {fixture.snapshot_form}")


def _same_lexical_path(left: Path, right: Path) -> bool:
    try:
        return os.path.samefile(left, right)
    except OSError:
        return os.path.normcase(os.path.realpath(left)) == os.path.normcase(
            os.path.realpath(right)
        )


def exercise_python_cache_readers(
    inventory: Inventory, inventory_directory: Path, readers: PythonReaders
) -> tuple[int, int]:
    """Exercise pinned offline readers over every inventoried repository."""

    cache_root = _host_path(inventory_directory.resolve(), inventory.cache_root)
    if not cache_root.is_dir():
        raise ConformanceError(f"generated cache root does not exist: {cache_root}")
    has_runtime_symlink = any(
        fixture.snapshot_form == "relative_symlink_runtime"
        for repository in inventory.repositories
        for fixture in repository.files
    )
    if has_runtime_symlink and not inventory.runtime_symlinks_materialized:
        raise ConformanceError(
            "fixture inventory includes a runtime symlink that was not materialized"
        )

    expected_scanned_repositories: set[tuple[str, str]] = set()
    file_count = 0
    for repository in inventory.repositories:
        identity = (repository.repo_type, repository.repo_id)
        expected_scanned_repositories.add(identity)
        repo_directory = _host_path(cache_root, repository.cache_directory)
        if not repo_directory.is_dir():
            raise ConformanceError(
                f"repository cache directory does not exist: {repo_directory}"
            )

        for ref in repository.refs:
            ref_path = _host_path(repo_directory, ref.path)
            try:
                ref_commit = ref_path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as error:
                raise ConformanceError(
                    f"could not read fixture ref {ref_path}: {error}"
                ) from error
            if ref_commit != repository.commit:
                raise ConformanceError(
                    f"fixture ref {ref.revision} contains {ref_commit!r}, "
                    f"wanted {repository.commit}"
                )

        tree_path = _host_path(repo_directory, repository.tree_path)
        if not tree_path.is_file():
            raise ConformanceError(f"tree cache record does not exist: {tree_path}")
        tree = readers.read_tree_cache(str(repo_directory), repository.commit)
        if not isinstance(tree, dict):
            raise ConformanceError(
                f"pinned read_tree_cache rejected {repository.repo_type}/{repository.repo_id}"
            )
        expected_tree_paths = {fixture.path.as_posix() for fixture in repository.files}
        if set(tree) != expected_tree_paths:
            raise ConformanceError(
                f"pinned read_tree_cache returned the wrong paths for "
                f"{repository.repo_type}/{repository.repo_id}"
            )
        for fixture in repository.files:
            tree_entry = tree[fixture.path.as_posix()]
            if getattr(tree_entry, "size", None) != fixture.size:
                raise ConformanceError(
                    "pinned read_tree_cache returned the wrong size for "
                    f"{repository.repo_type}/{repository.repo_id}:"
                    f"{fixture.path.as_posix()}"
                )
            identifiers = {
                value
                for value in (
                    getattr(tree_entry, "blob_id", None),
                    getattr(tree_entry, "lfs_sha256", None),
                )
                if isinstance(value, str)
            }
            if fixture.etag not in identifiers:
                raise ConformanceError(
                    "pinned read_tree_cache did not preserve the file validator for "
                    f"{repository.repo_type}/{repository.repo_id}:"
                    f"{fixture.path.as_posix()}"
                )

        revisions = [repository.commit, *(ref.revision for ref in repository.refs)]
        expected_snapshot = repo_directory / "snapshots" / repository.commit
        for revision in revisions:
            cached_tree = readers.get_cached_repo_tree(
                repo_id=repository.repo_id,
                repo_type=repository.repo_type,
                revision=revision,
                cache_dir=cache_root,
            )
            cached_tree_by_path: dict[str, object] = {}
            for cached_entry in cached_tree:
                cached_path = getattr(cached_entry, "path", None)
                if not isinstance(cached_path, str):
                    raise ConformanceError(
                        "pinned get_cached_repo_tree returned a record without a path for "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}"
                    )
                if cached_path in cached_tree_by_path:
                    raise ConformanceError(
                        "pinned get_cached_repo_tree returned a duplicate path for "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}:"
                        f"{cached_path}"
                    )
                cached_tree_by_path[cached_path] = cached_entry

            if set(cached_tree_by_path) != expected_tree_paths:
                raise ConformanceError(
                    "pinned get_cached_repo_tree returned the wrong paths for "
                    f"{repository.repo_type}/{repository.repo_id}@{revision}"
                )
            for fixture in repository.files:
                cached_entry = cached_tree_by_path[fixture.path.as_posix()]
                if getattr(cached_entry, "size", None) != fixture.size:
                    raise ConformanceError(
                        "pinned get_cached_repo_tree returned the wrong size for "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}:"
                        f"{fixture.path.as_posix()}"
                    )
                if getattr(cached_entry, "blob_id", None) != fixture.blob_id:
                    raise ConformanceError(
                        "pinned get_cached_repo_tree returned the wrong object ID for "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}:"
                        f"{fixture.path.as_posix()}"
                    )

            for fixture in repository.files:
                cached = readers.try_to_load_from_cache(
                    repo_id=repository.repo_id,
                    filename=fixture.path.as_posix(),
                    cache_dir=cache_root,
                    revision=revision,
                    repo_type=repository.repo_type,
                )
                if not isinstance(cached, str):
                    raise ConformanceError(
                        "pinned try_to_load_from_cache missed "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}:"
                        f"{fixture.path.as_posix()}"
                    )
                cached_path = Path(cached)
                expected_path = _host_path(expected_snapshot, fixture.path)
                if not _same_lexical_path(cached_path, expected_path):
                    raise ConformanceError(
                        "pinned try_to_load_from_cache returned an unexpected path: "
                        f"{cached_path} != {expected_path}"
                    )
                _assert_file(cached_path, fixture, "try_to_load_from_cache")

            for missing_path in repository.missing_paths:
                cached = readers.try_to_load_from_cache(
                    repo_id=repository.repo_id,
                    filename=missing_path.as_posix(),
                    cache_dir=cache_root,
                    revision=revision,
                    repo_type=repository.repo_type,
                )
                if cached is not readers.cached_no_exist:
                    raise ConformanceError(
                        "pinned try_to_load_from_cache did not return _CACHED_NO_EXIST for "
                        f"{repository.repo_type}/{repository.repo_id}@{revision}:"
                        f"{missing_path.as_posix()}"
                    )

            snapshot = Path(
                readers.snapshot_download(
                    repo_id=repository.repo_id,
                    repo_type=repository.repo_type,
                    revision=revision,
                    cache_dir=cache_root,
                    local_files_only=True,
                    token=False,
                )
            )
            if not _same_lexical_path(snapshot, expected_snapshot):
                raise ConformanceError(
                    f"pinned snapshot_download returned {snapshot}, wanted {expected_snapshot}"
                )

        for fixture in repository.files:
            _assert_snapshot_representation(repo_directory, repository.commit, fixture)
            _assert_file(
                _host_path(expected_snapshot, fixture.path),
                fixture,
                "snapshot_download",
            )
            file_count += 1

    cache_info = readers.scan_cache_dir(cache_root)
    warnings = tuple(getattr(cache_info, "warnings", ()))
    if warnings:
        raise ConformanceError(
            f"pinned scan_cache_dir reported {len(warnings)} corruption warning(s): {warnings[0]}"
        )
    scanned_by_identity = {
        (str(repository.repo_type), str(repository.repo_id)): repository
        for repository in getattr(cache_info, "repos", ())
    }
    if set(scanned_by_identity) != expected_scanned_repositories:
        raise ConformanceError(
            "pinned scan_cache_dir returned the wrong repository set: "
            f"{sorted(scanned_by_identity)} != {sorted(expected_scanned_repositories)}"
        )

    for fixture in inventory.repositories:
        scanned = scanned_by_identity[(fixture.repo_type, fixture.repo_id)]
        revisions = {
            str(revision.commit_hash): revision
            for revision in getattr(scanned, "revisions", ())
        }
        if set(revisions) != {fixture.commit}:
            raise ConformanceError(
                "pinned scan_cache_dir returned the wrong commit set for "
                f"{fixture.repo_type}/{fixture.repo_id}: {sorted(revisions)}"
            )
        revision = revisions[fixture.commit]
        scanned_refs = {
            str(ref).replace("\\", "/") for ref in getattr(revision, "refs", ())
        }
        expected_refs = {ref.revision for ref in fixture.refs}
        if scanned_refs != expected_refs:
            raise ConformanceError(
                "pinned scan_cache_dir returned the wrong refs for "
                f"{fixture.repo_type}/{fixture.repo_id}: "
                f"{sorted(scanned_refs)} != {sorted(expected_refs)}"
            )
        snapshot_path = Path(revision.snapshot_path)
        scanned_paths: set[str] = set()
        for cached_file in getattr(revision, "files", ()):
            try:
                relative = Path(cached_file.file_path).relative_to(snapshot_path)
            except (AttributeError, ValueError) as error:
                raise ConformanceError(
                    "pinned scan_cache_dir returned a file outside its snapshot for "
                    f"{fixture.repo_type}/{fixture.repo_id}"
                ) from error
            scanned_paths.add(relative.as_posix())
        expected_paths = {file.path.as_posix() for file in fixture.files}
        if scanned_paths != expected_paths:
            raise ConformanceError(
                "pinned scan_cache_dir returned the wrong files for "
                f"{fixture.repo_type}/{fixture.repo_id}: "
                f"{sorted(scanned_paths)} != {sorted(expected_paths)}"
            )

    return len(inventory.repositories), file_count


def prepare_local_dir_fixture(
    inventory_directory: Path, fixture: LocalDirFixture, destination: Path
) -> Path:
    """Copy a checked-in local_dir and make its deterministic metadata fresh."""

    source = _host_path(inventory_directory.resolve(), fixture.path)
    if source.is_symlink() or not source.is_dir():
        raise ConformanceError(f"local_dir fixture does not exist: {source}")
    if destination.exists() or destination.is_symlink():
        raise ConformanceError(
            f"local_dir conformance destination already exists: {destination}"
        )
    try:
        shutil.copytree(source, destination, symlinks=True)
        for file_fixture in fixture.files:
            file_path = _host_path(destination, file_fixture.path)
            if file_path.is_symlink() or not file_path.is_file():
                raise ConformanceError(
                    f"local_dir fixture file is not a regular file: {file_path}"
                )
            os.utime(
                file_path,
                (file_fixture.metadata_timestamp, file_fixture.metadata_timestamp),
            )
    except OSError as error:
        raise ConformanceError(
            f"could not prepare local_dir fixture {source}: {error}"
        ) from error
    return destination


def _assert_local_dir_file(
    path: Path, fixture: LocalDirFileFixture, context: str
) -> None:
    if path.is_symlink() or not path.is_file():
        raise ConformanceError(f"{context} is not an independent regular file: {path}")
    try:
        size = path.stat().st_size
    except OSError as error:
        raise ConformanceError(
            f"could not inspect local_dir file {path}: {error}"
        ) from error
    if size != fixture.size:
        raise ConformanceError(
            f"{context} has size {size}, wanted {fixture.size}: {path}"
        )
    digest = _digest(path)
    if digest != fixture.content_sha256:
        raise ConformanceError(
            f"{context} has SHA-256 {digest}, wanted {fixture.content_sha256}: {path}"
        )


def exercise_python_local_dir_readers(
    inventory: LocalDirInventory,
    inventory_directory: Path,
    readers: PythonReaders,
) -> tuple[int, int]:
    """Exercise pinned offline readers over fresh copies of every local_dir."""

    file_count = 0
    with TemporaryDirectory() as temporary_directory:
        temporary = Path(temporary_directory)
        for index, fixture in enumerate(inventory.local_directories):
            local_dir = prepare_local_dir_fixture(
                inventory_directory,
                fixture,
                temporary / f"local-dir-{index}",
            )
            gitignore = _host_path(local_dir, fixture.gitignore_path)
            cachedir_tag = _host_path(local_dir, fixture.cachedir_tag_path)
            try:
                if gitignore.read_bytes() != b"*":
                    raise ConformanceError(
                        f"pinned local_dir .gitignore has unexpected bytes: {gitignore}"
                    )
                if not cachedir_tag.read_bytes().startswith(
                    b"Signature: 8a477f597d28d172789f06886806bc55"
                ):
                    raise ConformanceError(
                        f"pinned local_dir CACHEDIR.TAG has the wrong signature: {cachedir_tag}"
                    )
            except OSError as error:
                raise ConformanceError(
                    f"could not read local_dir bookkeeping files: {error}"
                ) from error

            tree_cache_directory = local_dir / ".cache" / "huggingface"
            tree = readers.read_tree_cache(str(tree_cache_directory), fixture.commit)
            if not isinstance(tree, dict):
                raise ConformanceError(
                    f"pinned read_tree_cache rejected local_dir {fixture.path}"
                )
            expected_paths = {file.path.as_posix() for file in fixture.files}
            if set(tree) != expected_paths:
                raise ConformanceError(
                    f"pinned read_tree_cache returned the wrong local_dir paths for {fixture.path}"
                )

            for file_fixture in fixture.files:
                filename = file_fixture.path.as_posix()
                tree_entry = tree[filename]
                if (
                    getattr(tree_entry, "size", None) != file_fixture.size
                    or getattr(tree_entry, "blob_id", None) != file_fixture.blob_id
                    or getattr(tree_entry, "lfs_sha256", None)
                    != file_fixture.lfs_sha256
                    or getattr(tree_entry, "lfs_size", None) != file_fixture.lfs_size
                ):
                    raise ConformanceError(
                        f"pinned read_tree_cache changed local_dir identity fields for {filename}"
                    )

                metadata = readers.read_download_metadata(local_dir, filename)
                if metadata is None:
                    raise ConformanceError(
                        f"pinned read_download_metadata rejected fresh metadata for {filename}"
                    )
                if (
                    getattr(metadata, "filename", None) != filename
                    or getattr(metadata, "commit_hash", None) != fixture.commit
                    or getattr(metadata, "etag", None) != file_fixture.etag
                    or getattr(metadata, "timestamp", None)
                    != file_fixture.metadata_timestamp
                ):
                    raise ConformanceError(
                        f"pinned read_download_metadata changed fields for {filename}"
                    )
                _assert_local_dir_file(
                    _host_path(local_dir, file_fixture.path),
                    file_fixture,
                    "local_dir fixture",
                )
                file_count += 1

            cached_tree = readers.get_cached_repo_tree(
                fixture.repo_id,
                repo_type=fixture.repo_type,
                revision=fixture.commit,
                cache_dir=temporary / "unused-standard-cache",
                local_dir=local_dir,
            )
            cached_tree_records = {
                str(getattr(entry, "path", "")): (
                    getattr(entry, "size", None),
                    getattr(entry, "blob_id", None),
                )
                for entry in cached_tree
            }
            expected_tree_records = {
                file.path.as_posix(): (file.size, file.blob_id)
                for file in fixture.files
            }
            if cached_tree_records != expected_tree_records:
                raise ConformanceError(
                    f"pinned get_cached_repo_tree changed local_dir records for {fixture.path}"
                )

            with patch(
                "huggingface_hub._snapshot_download.HfApi.repo_info",
                side_effect=ConformanceError(
                    "snapshot_download attempted a network repository lookup"
                ),
            ):
                snapshot = Path(
                    readers.snapshot_download(
                        repo_id=fixture.repo_id,
                        repo_type=fixture.repo_type,
                        revision=fixture.commit,
                        cache_dir=temporary / "unused-standard-cache",
                        local_dir=local_dir,
                        local_files_only=True,
                        token=False,
                    )
                )
            if not _same_lexical_path(snapshot, local_dir):
                raise ConformanceError(
                    f"pinned snapshot_download returned {snapshot}, wanted {local_dir}"
                )

    return len(inventory.local_directories), file_count


def _arguments(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reference-root",
        type=Path,
        required=True,
        help="exact huggingface_hub git checkout installed for this run",
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        required=True,
        help="inventory.json emitted by the pinned fixture generator",
    )
    parser.add_argument(
        "--provenance",
        type=Path,
        help="provenance.json emitted by the generator (defaults beside inventory)",
    )
    parser.add_argument(
        "--local-dir-inventory",
        type=Path,
        help=(
            "local-dir-inventory.json emitted by the generator "
            "(defaults beside inventory)"
        ),
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    """Verify the pin, load the generated corpus, and exercise offline readers."""

    parsed = _arguments(arguments)
    provenance = parsed.provenance or parsed.inventory.with_name("provenance.json")
    local_dir_inventory_path = parsed.local_dir_inventory or parsed.inventory.with_name(
        "local-dir-inventory.json"
    )

    # Set these before importing huggingface_hub so the test cannot silently use
    # network or ambient telemetry if a reader's local-only behavior regresses.
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["DO_NOT_TRACK"] = "1"

    try:
        module, readers = load_python_readers()
        verify_reference_checkout(parsed.reference_root, module)
        verify_fixture_provenance(provenance, parsed.reference_root)
        inventory = load_inventory(parsed.inventory)
        local_dir_inventory = load_local_dir_inventory(local_dir_inventory_path)
        repositories, files = exercise_python_cache_readers(
            inventory, parsed.inventory.parent, readers
        )
        local_directories, local_dir_files = exercise_python_local_dir_readers(
            local_dir_inventory, local_dir_inventory_path.parent, readers
        )
    except ConformanceError as error:
        print(f"conformance error: {error}", file=sys.stderr)
        return 1

    print(
        "verified pinned Python readers: "
        f"huggingface_hub {EXPECTED_HUGGINGFACE_HUB_VERSION} "
        f"at {EXPECTED_HUGGINGFACE_HUB_COMMIT}; "
        f"{repositories} Python-written standard-cache repositories and {files} files; "
        f"{local_directories} Python-written local_dir and {local_dir_files} files"
    )
    print(
        "scope: Python reads of Python-written fixtures; Rust-writer and hf-store "
        "offline-completeness conformance are not asserted"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
