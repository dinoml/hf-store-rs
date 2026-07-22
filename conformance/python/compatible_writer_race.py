"""Run the pinned Python half of a mixed standard-cache writer race.

The harness exercises the real ``huggingface_hub`` cache writer.  Only remote
metadata and response-body effects are replaced with deterministic local
values.  A Rust parent coordinates the process through atomic marker files.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time
from typing import Any, Iterator, Sequence
from unittest.mock import patch

from filelock import SoftFileLock

from cache_conformance import ConformanceError, verify_reference_checkout


FORMAT_VERSION = 1
CRASH_EXIT_CODE = 86
POLL_INTERVAL_SECONDS = 0.01
MODES = ("python-first", "rust-first")
CRASH_POINTS = ("lock-acquired", "body-entered", "writer-returned", "tree-written")
LOWER_HEX_40 = re.compile(r"[0-9a-f]{40}\Z")
LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")


class RaceHarnessError(RuntimeError):
    """Raised when the writer or coordination contract is violated."""


@dataclass(frozen=True)
class RaceConfig:
    """Validated inputs for one pinned Python writer process."""

    mode: str
    reference_root: Path
    cache_root: Path
    control_dir: Path
    result_path: Path
    content_path: Path
    repo_type: str
    repo_id: str
    revision: str
    commit: str
    filename: str
    etag: str
    blob_id: str
    lfs_sha256: str | None
    force_copy: bool
    timeout_seconds: float
    crash_at: str | None

    def to_arguments(self) -> list[str]:
        """Encode this configuration as command-line arguments."""

        arguments = [
            "--mode",
            self.mode,
            "--reference-root",
            str(self.reference_root),
            "--cache-root",
            str(self.cache_root),
            "--control-dir",
            str(self.control_dir),
            "--result",
            str(self.result_path),
            "--content",
            str(self.content_path),
            "--repo-type",
            self.repo_type,
            "--repo-id",
            self.repo_id,
            "--revision",
            self.revision,
            "--commit",
            self.commit,
            "--filename",
            self.filename,
            "--etag",
            self.etag,
            "--blob-id",
            self.blob_id,
            "--timeout-seconds",
            str(self.timeout_seconds),
        ]
        if self.lfs_sha256 is not None:
            arguments.extend(("--lfs-sha256", self.lfs_sha256))
        if self.force_copy:
            arguments.append("--force-copy")
        if self.crash_at is not None:
            arguments.extend(("--crash-at", self.crash_at))
        return arguments


class MarkerProtocol:
    """Create process markers atomically and wait for parent-created gates."""

    def __init__(self, directory: Path, timeout_seconds: float) -> None:
        self.directory = directory
        self.timeout_seconds = timeout_seconds

    def mark(self, name: str) -> None:
        path = self.directory / name
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as error:
            raise RaceHarnessError(f"coordination marker already exists: {name}") from error
        os.close(descriptor)

    def wait(self, name: str) -> None:
        path = self.directory / name
        abort = self.directory / "abort"
        deadline = time.monotonic() + self.timeout_seconds
        while not path.is_file():
            if abort.is_file():
                raise RaceHarnessError(f"coordination aborted while waiting for {name}")
            if time.monotonic() >= deadline:
                raise RaceHarnessError(f"timed out waiting for coordination gate {name}")
            time.sleep(POLL_INTERVAL_SECONDS)


def imported_reference_root() -> Path:
    """Return the checkout root containing the imported pinned package."""

    import huggingface_hub

    imported_file = getattr(huggingface_hub, "__file__", None)
    if not isinstance(imported_file, str):
        raise RaceHarnessError("huggingface_hub has no import path")
    package_directory = Path(imported_file).resolve().parent
    if package_directory.parent.name != "src":
        raise RaceHarnessError("huggingface_hub was not imported from a source checkout")
    return package_directory.parent.parent


def _validate(config: RaceConfig) -> None:
    if config.mode not in MODES:
        raise RaceHarnessError(f"unsupported race mode: {config.mode}")
    if config.repo_type not in {"model", "dataset", "space"}:
        raise RaceHarnessError(f"unsupported repository type: {config.repo_type}")
    if not LOWER_HEX_40.fullmatch(config.commit):
        raise RaceHarnessError("commit must be a lowercase 40-character hexadecimal value")
    if not LOWER_HEX_40.fullmatch(config.blob_id):
        raise RaceHarnessError("blob ID must be a lowercase 40-character hexadecimal value")
    if config.lfs_sha256 is not None and not LOWER_HEX_64.fullmatch(config.lfs_sha256):
        raise RaceHarnessError("LFS SHA-256 must be lowercase hexadecimal")
    if config.lfs_sha256 is None and config.etag != config.blob_id:
        raise RaceHarnessError("a non-LFS ETag must equal its Git blob ID")
    if config.lfs_sha256 is not None and config.etag != config.lfs_sha256:
        raise RaceHarnessError("an LFS ETag must equal its SHA-256")
    if not config.etag or any(separator in config.etag for separator in ("/", "\\", "\0")):
        raise RaceHarnessError("ETag must be one safe cache-path component")
    _validate_posix_path(config.filename, "filename")
    _validate_posix_path(config.revision, "revision")
    _validate_repo_id(config.repo_id)
    if config.timeout_seconds <= 0:
        raise RaceHarnessError("coordination timeout must be positive")
    if config.crash_at is not None and config.crash_at not in CRASH_POINTS:
        raise RaceHarnessError(f"unsupported crash point: {config.crash_at}")
    if not config.reference_root.is_dir():
        raise RaceHarnessError("reference root is not a directory")
    if not config.cache_root.is_dir():
        raise RaceHarnessError("cache root is not a directory")
    if not config.control_dir.is_dir():
        raise RaceHarnessError("control directory is not a directory")
    if not config.content_path.is_file():
        raise RaceHarnessError("content path is not a regular file")


def _validate_posix_path(value: str, context: str) -> None:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or path.as_posix() != value
        or any(component in {"", ".", ".."} for component in path.parts)
    ):
        raise RaceHarnessError(f"{context} must be a normalized relative POSIX path")


def _validate_repo_id(value: str) -> None:
    _validate_posix_path(value, "repository ID")
    if len(PurePosixPath(value).parts) > 2:
        raise RaceHarnessError("repository ID may contain at most one namespace separator")


def _crash_if_requested(config: RaceConfig, protocol: MarkerProtocol, point: str) -> None:
    if config.crash_at == point:
        protocol.mark(f"python.crash-{point}")
        os._exit(CRASH_EXIT_CODE)


def _normalized_absolute(path: str | Path) -> str:
    value = os.path.abspath(os.fspath(path))
    if os.name == "nt" and value.startswith("\\\\?\\"):
        value = value[4:]
    return os.path.normcase(value)


def _relative_result_path(path: str | Path, root: Path, context: str) -> str:
    normalized = Path(_normalized_absolute(path))
    normalized_root = Path(_normalized_absolute(root))
    try:
        return normalized.relative_to(normalized_root).as_posix()
    except ValueError as error:
        raise RaceHarnessError(f"{context} escaped the cache root") from error


def _pointer_form(path: Path) -> str:
    if path.is_symlink():
        return "symlink"
    if path.is_file():
        return "regular"
    raise RaceHarnessError("pinned writer did not publish a snapshot pointer")


def _write_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def run_writer(config: RaceConfig) -> dict[str, Any]:
    """Run one real pinned cache writer under deterministic coordination."""

    _validate(config)

    import huggingface_hub
    import huggingface_hub.file_download as file_download
    from huggingface_hub._tree_cache import TreeCacheEntry, write_tree_cache

    try:
        verify_reference_checkout(config.reference_root, huggingface_hub)
    except ConformanceError as error:
        raise RaceHarnessError(str(error)) from error

    content = config.content_path.read_bytes()
    content_sha256 = hashlib.sha256(content).hexdigest()
    if config.lfs_sha256 is not None and content_sha256 != config.lfs_sha256:
        raise RaceHarnessError("content does not match its configured LFS SHA-256")

    protocol = MarkerProtocol(config.control_dir, config.timeout_seconds)
    protocol.mark("python.ready")
    protocol.wait("start")

    real_weak_file_lock = file_download.WeakFileLock
    observed_lock_path: str | None = None
    observed_lock_backend: str | None = None
    body_calls = 0

    @contextmanager
    def observed_weak_file_lock(lock_path: str | Path) -> Iterator[Any]:
        nonlocal observed_lock_path, observed_lock_backend
        observed_lock_path = os.fspath(lock_path)
        protocol.mark("python.lock-attempted")
        with real_weak_file_lock(lock_path) as lock:
            observed_lock_backend = type(lock).__name__
            if isinstance(lock, SoftFileLock):
                protocol.mark("python.soft-lock")
                raise RaceHarnessError("pinned Python writer fell back to SoftFileLock")
            protocol.mark("python.lock-acquired")
            _crash_if_requested(config, protocol, "lock-acquired")
            if config.mode == "python-first":
                protocol.wait("release-python")
            yield lock

    def fixed_metadata(**_arguments: Any) -> tuple[str, str, str, int, None, None]:
        return (
            "https://fixture.invalid/content",
            config.etag,
            config.commit,
            len(content),
            None,
            None,
        )

    def fixed_body(_url: str, output: Any, **_arguments: Any) -> None:
        nonlocal body_calls
        body_calls += 1
        protocol.mark("python.body-entered")
        if config.crash_at == "body-entered":
            partial_size = max(1, len(content) // 2)
            output.write(content[:partial_size])
            output.flush()
            os.fsync(output.fileno())
            _crash_if_requested(config, protocol, "body-entered")
        output.write(content)

    symlink_patch = (
        patch.object(file_download, "are_symlinks_supported", return_value=False)
        if config.force_copy
        else _null_patch()
    )
    with (
        patch.object(file_download, "_get_metadata_or_catch_error", side_effect=fixed_metadata),
        patch.object(file_download, "http_get", side_effect=fixed_body),
        patch.object(file_download, "WeakFileLock", observed_weak_file_lock),
        symlink_patch,
    ):
        returned = file_download._hf_hub_download_to_cache_dir(
            cache_dir=str(config.cache_root),
            repo_id=config.repo_id,
            filename=config.filename,
            repo_type=config.repo_type,
            revision=config.revision,
            endpoint="https://fixture.invalid",
            etag_timeout=1.0,
            headers={},
            token=None,
            local_files_only=False,
            force_download=False,
            tqdm_class=None,
            dry_run=False,
        )

    protocol.mark("python.writer-returned")
    _crash_if_requested(config, protocol, "writer-returned")

    storage_name = file_download.repo_folder_name(
        repo_id=config.repo_id,
        repo_type=config.repo_type,
    )
    storage = config.cache_root / storage_name
    tree_entry = TreeCacheEntry(
        size=len(content),
        blob_id=config.blob_id,
        lfs_sha256=config.lfs_sha256,
        lfs_size=len(content) if config.lfs_sha256 is not None else None,
    )
    write_tree_cache(str(storage), config.commit, {config.filename: tree_entry})
    protocol.mark("python.tree-written")
    _crash_if_requested(config, protocol, "tree-written")

    expected_lock = config.cache_root / ".locks" / storage_name / f"{config.etag}.lock"
    if observed_lock_path is None or observed_lock_backend is None:
        raise RaceHarnessError("pinned writer returned without acquiring its blob lock")
    if _normalized_absolute(observed_lock_path) != _normalized_absolute(expected_lock):
        raise RaceHarnessError("pinned writer used an unexpected blob lock path")

    expected_body_calls = 1 if config.mode == "python-first" else 0
    if body_calls != expected_body_calls:
        raise RaceHarnessError(
            f"{config.mode} expected {expected_body_calls} body calls, observed {body_calls}"
        )

    pointer = Path(returned)
    blob = storage / "blobs" / config.etag
    tree = storage / "trees" / f"{config.commit}.json"
    reference = storage.joinpath("refs", *PurePosixPath(config.revision).parts)
    if pointer.read_bytes() != content:
        raise RaceHarnessError("published snapshot content does not match the fixture")
    if not tree.is_file():
        raise RaceHarnessError("real tree writer did not publish its record")

    result: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "producer": "huggingface_hub",
        "status": "ok",
        "mode": config.mode,
        "repo_type": config.repo_type,
        "repo_id": config.repo_id,
        "revision": config.revision,
        "commit": config.commit,
        "filename": config.filename,
        "etag": config.etag,
        "blob_id": config.blob_id,
        "lfs_sha256": config.lfs_sha256,
        "size": len(content),
        "content_sha256": content_sha256,
        "body_calls": body_calls,
        "lock_backend": observed_lock_backend,
        "lock_path": _relative_result_path(observed_lock_path, config.cache_root, "lock path"),
        "snapshot_path": _relative_result_path(pointer, config.cache_root, "snapshot path"),
        "pointer_form": _pointer_form(pointer),
        "blob_path": _relative_result_path(blob, config.cache_root, "blob path"),
        "blob_exists": blob.is_file(),
        "tree_path": _relative_result_path(tree, config.cache_root, "tree path"),
        "tree_exists": tree.is_file(),
        "ref_path": _relative_result_path(reference, config.cache_root, "ref path"),
        "ref_value": reference.read_text(encoding="utf-8") if reference.is_file() else None,
        "force_copy": config.force_copy,
    }
    _write_result(config.result_path, result)
    protocol.mark("python.complete")
    return result


@contextmanager
def _null_patch() -> Iterator[None]:
    yield None


def _arguments(arguments: Sequence[str] | None) -> RaceConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--control-dir", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--content", type=Path, required=True)
    parser.add_argument("--repo-type", choices=("model", "dataset", "space"), required=True)
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--etag", required=True)
    parser.add_argument("--blob-id", required=True)
    parser.add_argument("--lfs-sha256")
    parser.add_argument("--force-copy", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--crash-at", choices=CRASH_POINTS)
    parsed = parser.parse_args(arguments)
    return RaceConfig(
        mode=parsed.mode,
        reference_root=parsed.reference_root.resolve(),
        cache_root=parsed.cache_root.resolve(),
        control_dir=parsed.control_dir.resolve(),
        result_path=parsed.result.resolve(),
        content_path=parsed.content.resolve(),
        repo_type=parsed.repo_type,
        repo_id=parsed.repo_id,
        revision=parsed.revision,
        commit=parsed.commit,
        filename=parsed.filename,
        etag=parsed.etag,
        blob_id=parsed.blob_id,
        lfs_sha256=parsed.lfs_sha256,
        force_copy=parsed.force_copy,
        timeout_seconds=parsed.timeout_seconds,
        crash_at=parsed.crash_at,
    )


def _failure_result(error: Exception) -> dict[str, Any]:
    return {
        "format_version": FORMAT_VERSION,
        "producer": "huggingface_hub",
        "status": "error",
        "error": str(error),
    }


def main(arguments: Sequence[str] | None = None) -> int:
    """Run the configured writer and emit one machine-readable JSON result."""

    config = _arguments(arguments)
    try:
        result = run_writer(config)
    except (OSError, RaceHarnessError) as error:
        result = _failure_result(error)
        _write_result(config.result_path, result)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
