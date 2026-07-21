"""Compare regenerated portable cache fixtures with their checked-in bytes."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import sys
from typing import Sequence


EXCLUDED_ROOT_ENTRIES = frozenset({"README.md", "generate.py", "__pycache__"})


class ComparisonError(RuntimeError):
    """Portable generated fixtures differ from the checked-in corpus."""


@dataclass(frozen=True)
class TreeEntry:
    """One non-followed filesystem entry below a comparison root."""

    kind: str
    path: Path


def _scan_tree(
    root: Path, *, exclude_source_entries: bool
) -> dict[PurePosixPath, TreeEntry]:
    if root.is_symlink():
        raise ComparisonError(f"fixture root is not a regular directory: {root}")
    try:
        root = root.resolve(strict=True)
    except OSError as error:
        raise ComparisonError(f"fixture root does not exist: {root}") from error
    if not root.is_dir():
        raise ComparisonError(f"fixture root is not a regular directory: {root}")

    entries: dict[PurePosixPath, TreeEntry] = {}

    def scan(directory: Path, relative_directory: PurePosixPath) -> None:
        try:
            children = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError as error:
            raise ComparisonError(
                f"could not scan fixture directory {directory}: {error}"
            ) from error

        for child in children:
            relative_path = relative_directory / child.name
            if (
                exclude_source_entries
                and len(relative_path.parts) == 1
                and child.name in EXCLUDED_ROOT_ENTRIES
            ):
                continue
            absolute_path = Path(child.path)
            try:
                if child.is_symlink():
                    kind = "symlink"
                elif child.is_dir(follow_symlinks=False):
                    kind = "directory"
                elif child.is_file(follow_symlinks=False):
                    kind = "file"
                else:
                    kind = "special"
            except OSError as error:
                raise ComparisonError(
                    f"could not inspect fixture path {absolute_path}: {error}"
                ) from error
            entries[relative_path] = TreeEntry(kind=kind, path=absolute_path)
            if kind == "directory":
                scan(absolute_path, relative_path)

    scan(root, PurePosixPath())
    return entries


def _files_equal(left: Path, right: Path) -> bool:
    try:
        if left.stat().st_size != right.stat().st_size:
            return False
        with left.open("rb") as left_file, right.open("rb") as right_file:
            while True:
                left_chunk = left_file.read(1024 * 1024)
                right_chunk = right_file.read(1024 * 1024)
                if left_chunk != right_chunk:
                    return False
                if not left_chunk:
                    return True
    except OSError as error:
        raise ComparisonError(f"could not compare fixture bytes: {error}") from error


def compare_portable_fixture_trees(checked_in: Path, generated: Path) -> int:
    """Require exact generated paths, entry kinds, file bytes, and link targets."""

    checked_entries = _scan_tree(checked_in, exclude_source_entries=True)
    generated_entries = _scan_tree(generated, exclude_source_entries=False)
    checked_paths = set(checked_entries)
    generated_paths = set(generated_entries)

    missing = sorted(checked_paths - generated_paths)
    if missing:
        raise ComparisonError(
            "missing generated path(s): "
            + ", ".join(path.as_posix() for path in missing)
        )
    unexpected = sorted(generated_paths - checked_paths)
    if unexpected:
        raise ComparisonError(
            "unexpected generated path(s): "
            + ", ".join(path.as_posix() for path in unexpected)
        )

    for relative_path in sorted(checked_paths):
        checked_entry = checked_entries[relative_path]
        generated_entry = generated_entries[relative_path]
        if checked_entry.kind != generated_entry.kind:
            raise ComparisonError(
                f"entry type differs at {relative_path.as_posix()}: "
                f"checked-in {checked_entry.kind}, generated {generated_entry.kind}"
            )
        if checked_entry.kind == "file" and not _files_equal(
            checked_entry.path, generated_entry.path
        ):
            raise ComparisonError(f"file bytes differ at {relative_path.as_posix()}")
        if checked_entry.kind == "symlink":
            try:
                checked_target = os.readlink(checked_entry.path)
                generated_target = os.readlink(generated_entry.path)
            except OSError as error:
                raise ComparisonError(
                    f"could not compare symlink at {relative_path.as_posix()}: {error}"
                ) from error
            if checked_target != generated_target:
                raise ComparisonError(
                    f"symlink target differs at {relative_path.as_posix()}: "
                    f"checked-in {checked_target!r}, generated {generated_target!r}"
                )
        if checked_entry.kind == "special":
            raise ComparisonError(
                f"portable fixture contains a special file at {relative_path.as_posix()}"
            )

    return len(checked_paths)


def _arguments(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checked-in",
        type=Path,
        required=True,
        help="checked-in huggingface_hub fixture directory",
    )
    parser.add_argument(
        "--generated",
        type=Path,
        required=True,
        help="portable directory emitted by the pinned generator",
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    """Run the strict portable fixture comparison."""

    parsed = _arguments(arguments)
    try:
        entry_count = compare_portable_fixture_trees(
            parsed.checked_in, parsed.generated
        )
    except ComparisonError as error:
        print(f"portable fixture comparison error: {error}", file=sys.stderr)
        return 1
    print(f"verified {entry_count} portable fixture paths and all generated file bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
