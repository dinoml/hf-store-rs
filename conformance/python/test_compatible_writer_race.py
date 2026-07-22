"""Hermetic tests for the pinned Python side of mixed cache-writer races."""

from __future__ import annotations

from contextlib import contextmanager, redirect_stdout
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from filelock import SoftFileLock

from compatible_writer_race import (
    CRASH_EXIT_CODE,
    RaceConfig,
    RaceHarnessError,
    imported_reference_root,
    main,
    run_writer,
)


CONTENT = b'{"writer":"python"}\n'
COMMIT = "4" * 40
REVISION = "main"
REPO_ID = "fixture-org/writer-race"
FILENAME = "nested/config.json"


def git_blob_id(content: bytes) -> str:
    """Return the Git object identity used by a non-LFS Hub tree entry."""

    framed = f"blob {len(content)}\0".encode() + content
    return hashlib.sha1(framed).hexdigest()  # noqa: S324 - Git requires SHA-1.


class CompatibleWriterRaceTests(unittest.TestCase):
    """Exercise the real pinned writer with only remote effects substituted."""

    def make_config(
        self,
        temporary: Path,
        *,
        mode: str,
        crash_at: str | None = None,
    ) -> RaceConfig:
        cache_root = temporary / "cache"
        control_dir = temporary / "control"
        content_path = temporary / "content.bin"
        result_path = temporary / "result.json"
        cache_root.mkdir()
        control_dir.mkdir()
        content_path.write_bytes(CONTENT)
        (control_dir / "start").touch()
        if mode == "python-first":
            (control_dir / "release-python").touch()
        return RaceConfig(
            mode=mode,
            reference_root=imported_reference_root(),
            cache_root=cache_root,
            control_dir=control_dir,
            result_path=result_path,
            content_path=content_path,
            repo_type="model",
            repo_id=REPO_ID,
            revision=REVISION,
            commit=COMMIT,
            filename=FILENAME,
            etag=git_blob_id(CONTENT),
            blob_id=git_blob_id(CONTENT),
            lfs_sha256=None,
            force_copy=True,
            timeout_seconds=5.0,
            crash_at=crash_at,
        )

    def test_python_first_uses_real_unique_move_ref_tree_and_copy_pointer(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            config = self.make_config(temporary, mode="python-first")

            result = run_writer(config)

            storage = config.cache_root / "models--fixture-org--writer-race"
            snapshot = storage / "snapshots" / COMMIT / FILENAME
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["body_calls"], 1)
            self.assertEqual(result["pointer_form"], "regular")
            self.assertFalse(result["blob_exists"])
            self.assertEqual(snapshot.read_bytes(), CONTENT)
            self.assertEqual((storage / "refs" / REVISION).read_text(), COMMIT)
            tree = json.loads((storage / "trees" / f"{COMMIT}.json").read_text())
            self.assertEqual(tree["files"][FILENAME]["blob_id"], config.blob_id)
            self.assertEqual(list(storage.rglob("*.incomplete")), [])
            self.assertTrue((config.control_dir / "python.complete").is_file())
            self.assertEqual(json.loads(config.result_path.read_text()), result)

    def test_rust_first_reuses_a_preexisting_blob_without_calling_the_body(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            config = self.make_config(temporary, mode="rust-first")
            storage = config.cache_root / "models--fixture-org--writer-race"
            blob = storage / "blobs" / config.etag
            blob.parent.mkdir(parents=True)
            blob.write_bytes(CONTENT)

            result = run_writer(config)

            snapshot = storage / "snapshots" / COMMIT / FILENAME
            self.assertEqual(result["body_calls"], 0)
            self.assertEqual(result["pointer_form"], "regular")
            self.assertTrue(result["blob_exists"])
            self.assertEqual(blob.read_bytes(), CONTENT)
            self.assertEqual(snapshot.read_bytes(), CONTENT)
            self.assertTrue((config.control_dir / "python.lock-attempted").is_file())
            self.assertTrue((config.control_dir / "python.lock-acquired").is_file())

    def test_cli_emits_the_same_versioned_machine_result(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            config = self.make_config(Path(temporary_directory), mode="rust-first")
            storage = config.cache_root / "models--fixture-org--writer-race"
            blob = storage / "blobs" / config.etag
            blob.parent.mkdir(parents=True)
            blob.write_bytes(CONTENT)
            stdout = StringIO()

            with redirect_stdout(stdout):
                exit_code = main(config.to_arguments())

            emitted = json.loads(stdout.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(emitted, json.loads(config.result_path.read_text()))
            self.assertEqual(emitted["format_version"], 1)
            self.assertEqual(emitted["status"], "ok")

    def test_rejects_the_pinned_writers_soft_lock_fallback(self) -> None:
        import huggingface_hub.file_download as file_download

        with TemporaryDirectory() as temporary_directory:
            config = self.make_config(Path(temporary_directory), mode="python-first")

            @contextmanager
            def soft_lock(lock_path: str | Path):
                yield SoftFileLock(lock_path)

            with patch.object(file_download, "WeakFileLock", soft_lock):
                with self.assertRaisesRegex(RaceHarnessError, "SoftFileLock"):
                    run_writer(config)

            self.assertTrue((config.control_dir / "python.soft-lock").is_file())

    def test_rejects_noncanonical_remote_paths_before_marking_ready(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            config = self.make_config(Path(temporary_directory), mode="python-first")
            invalid = RaceConfig(**{**config.__dict__, "filename": "nested//config.json"})

            with self.assertRaisesRegex(RaceHarnessError, "normalized relative POSIX"):
                run_writer(invalid)

            self.assertFalse((config.control_dir / "python.ready").exists())

    def test_body_crash_exits_without_running_python_cleanup(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            config = self.make_config(
                temporary,
                mode="python-first",
                crash_at="body-entered",
            )
            command = [
                sys.executable,
                str(Path(__file__).with_name("compatible_writer_race.py")),
                *config.to_arguments(),
            ]

            completed = subprocess.run(command, check=False, capture_output=True, text=True)

            self.assertEqual(completed.returncode, CRASH_EXIT_CODE)
            self.assertTrue((config.control_dir / "python.body-entered").is_file())
            storage = config.cache_root / "models--fixture-org--writer-race"
            self.assertFalse((storage / "blobs" / config.etag).exists())
            incomplete = list((storage / "blobs").glob("*.incomplete"))
            self.assertEqual(len(incomplete), 1)
            self.assertRegex(
                incomplete[0].name,
                rf"^{config.etag}\.[0-9a-f]{{8}}\.incomplete$",
            )
            self.assertGreater(incomplete[0].stat().st_size, 0)
            self.assertFalse(config.result_path.exists())


if __name__ == "__main__":
    unittest.main()
