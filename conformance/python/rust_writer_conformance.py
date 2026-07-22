"""Verify a Rust-written standard cache with the pinned Python readers."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Sequence

from cache_conformance import (
    EXPECTED_HUGGINGFACE_HUB_COMMIT,
    EXPECTED_HUGGINGFACE_HUB_VERSION,
    ConformanceError,
    Inventory,
    exercise_python_cache_readers,
    load_inventory,
    load_python_readers,
    verify_reference_checkout,
)


EXPECTED_RUST_WRITER_PRODUCER = "hf-store-rs"


def validate_rust_writer_inventory(inventory: Inventory) -> None:
    """Reject a corpus that does not explicitly identify the Rust writer."""

    if inventory.producer != EXPECTED_RUST_WRITER_PRODUCER:
        raise ConformanceError(
            "Rust-writer inventory producer must be "
            f"{EXPECTED_RUST_WRITER_PRODUCER!r}, got {inventory.producer!r}"
        )


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
        help="inventory.json emitted with the Rust-written standard cache",
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    """Verify the reader pin and exercise it over a Rust-written cache."""

    parsed = _arguments(arguments)

    # Set these before importing huggingface_hub so a local-only regression
    # cannot silently use network or ambient telemetry.
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["DO_NOT_TRACK"] = "1"

    try:
        module, readers = load_python_readers()
        verify_reference_checkout(parsed.reference_root, module)
        inventory = load_inventory(parsed.inventory)
        validate_rust_writer_inventory(inventory)
        repositories, files = exercise_python_cache_readers(
            inventory, parsed.inventory.parent, readers
        )
    except ConformanceError as error:
        print(f"conformance error: {error}", file=sys.stderr)
        return 1

    print(
        "verified Rust-written standard cache with pinned Python readers: "
        f"huggingface_hub {EXPECTED_HUGGINGFACE_HUB_VERSION} "
        f"at {EXPECTED_HUGGINGFACE_HUB_COMMIT}; "
        f"{repositories} repositories and {files} files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
