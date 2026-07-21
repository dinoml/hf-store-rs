"""Tests for the pinned Python cache conformance runner."""

from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from cache_conformance import (
    EXPECTED_HUGGINGFACE_HUB_COMMIT,
    EXPECTED_HUGGINGFACE_HUB_VERSION,
    ConformanceError,
    load_inventory,
    load_local_dir_inventory,
    prepare_local_dir_fixture,
    validate_imported_source,
    validate_upstream_identity,
)


class UpstreamIdentityTests(unittest.TestCase):
    """Validate failures before any cache reader is exercised."""

    def test_accepts_the_pinned_version_and_commit(self) -> None:
        validate_upstream_identity(
            package_version=EXPECTED_HUGGINGFACE_HUB_VERSION,
            source_commit=EXPECTED_HUGGINGFACE_HUB_COMMIT,
            source_is_clean=True,
        )

    def test_rejects_a_different_package_version(self) -> None:
        with self.assertRaisesRegex(ConformanceError, "package version"):
            validate_upstream_identity(
                package_version="1.24.1",
                source_commit=EXPECTED_HUGGINGFACE_HUB_COMMIT,
                source_is_clean=True,
            )

    def test_rejects_a_different_source_commit(self) -> None:
        with self.assertRaisesRegex(ConformanceError, "source commit"):
            validate_upstream_identity(
                package_version=EXPECTED_HUGGINGFACE_HUB_VERSION,
                source_commit="0" * 40,
                source_is_clean=True,
            )

    def test_rejects_modified_upstream_sources(self) -> None:
        with self.assertRaisesRegex(ConformanceError, "modified"):
            validate_upstream_identity(
                package_version=EXPECTED_HUGGINGFACE_HUB_VERSION,
                source_commit=EXPECTED_HUGGINGFACE_HUB_COMMIT,
                source_is_clean=False,
            )


class ImportedSourceTests(unittest.TestCase):
    """Ensure Python actually imported the checkout whose commit was checked."""

    def test_accepts_module_below_the_reference_source_tree(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            reference_root = Path(temporary_directory) / "huggingface_hub"
            imported_module = reference_root / "src" / "huggingface_hub" / "__init__.py"
            imported_module.parent.mkdir(parents=True)
            imported_module.touch()

            validate_imported_source(reference_root, imported_module)

    def test_rejects_module_from_another_installation(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            reference_root = temporary_root / "reference"
            imported_module = (
                temporary_root / "site-packages" / "huggingface_hub" / "__init__.py"
            )
            reference_root.mkdir()
            imported_module.parent.mkdir(parents=True)
            imported_module.touch()

            with self.assertRaisesRegex(ConformanceError, "reference checkout"):
                validate_imported_source(reference_root, imported_module)


class InventoryTests(unittest.TestCase):
    """Keep the generated corpus contract versioned and path-safe."""

    def inventory(self) -> dict[str, object]:
        return {
            "format_version": 1,
            "cache_root": "cache",
            "runtime_symlinks_materialized": True,
            "repositories": [
                {
                    "repo_type": "model",
                    "repo_id": "org/model",
                    "cache_directory": "models--org--model",
                    "commit": "1" * 40,
                    "refs": [{"revision": "refs/pr/1", "path": "refs/refs/pr/1"}],
                    "tree_path": f"trees/{'1' * 40}.json",
                    "files": [
                        {
                            "path": "nested/config.json",
                            "etag": "opaque-etag",
                            "size": 2,
                            "content_sha256": "2" * 64,
                            "snapshot_form": "copied_regular_with_blob",
                        }
                    ],
                    "missing_paths": ["missing.json"],
                }
            ],
        }

    def write_inventory(self, directory: Path, inventory: dict[str, object]) -> Path:
        path = directory / "inventory.json"
        path.write_text(json.dumps(inventory), encoding="utf-8")
        return path

    def test_loads_the_version_one_inventory(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = self.write_inventory(Path(temporary_directory), self.inventory())
            inventory = load_inventory(path)

        self.assertEqual(inventory.cache_root.as_posix(), "cache")
        self.assertEqual(inventory.repositories[0].repo_id, "org/model")
        self.assertEqual(inventory.repositories[0].refs[0].revision, "refs/pr/1")

    def test_rejects_an_unknown_inventory_version(self) -> None:
        inventory = self.inventory()
        inventory["format_version"] = 2
        with TemporaryDirectory() as temporary_directory:
            path = self.write_inventory(Path(temporary_directory), inventory)
            with self.assertRaisesRegex(
                ConformanceError, "unsupported fixture inventory"
            ):
                load_inventory(path)

    def test_rejects_an_unsafe_inventory_path(self) -> None:
        for unsafe_path in ("../token", "C:/outside", "name:stream"):
            with self.subTest(unsafe_path=unsafe_path):
                inventory = self.inventory()
                repository = inventory["repositories"][0]  # type: ignore[index]
                repository["files"][0]["path"] = unsafe_path  # type: ignore[index]
                with TemporaryDirectory() as temporary_directory:
                    path = self.write_inventory(Path(temporary_directory), inventory)
                    with self.assertRaisesRegex(
                        ConformanceError, "normalized relative POSIX path"
                    ):
                        load_inventory(path)


class LocalDirInventoryTests(unittest.TestCase):
    """Keep the Python-written local_dir corpus explicit and path-safe."""

    def inventory(self) -> dict[str, object]:
        commit = "4" * 40
        return {
            "format_version": 1,
            "local_directories": [
                {
                    "path": "local-dir",
                    "repo_type": "model",
                    "repo_id": "fixture-org/fixture-local-dir",
                    "commit": commit,
                    "tree_path": f".cache/huggingface/trees/{commit}.json",
                    "gitignore_path": ".cache/huggingface/.gitignore",
                    "cachedir_tag_path": ".cache/huggingface/CACHEDIR.TAG",
                    "files": [
                        {
                            "path": "nested/config.json",
                            "metadata_path": (
                                ".cache/huggingface/download/"
                                "nested/config.json.metadata"
                            ),
                            "etag": "1" * 40,
                            "blob_id": "1" * 40,
                            "lfs_sha256": None,
                            "lfs_size": None,
                            "size": 2,
                            "content_sha256": "2" * 64,
                            "metadata_timestamp": 1_720_000_000.25,
                        }
                    ],
                }
            ],
        }

    def write_inventory(self, directory: Path, inventory: dict[str, object]) -> Path:
        path = directory / "local-dir-inventory.json"
        path.write_text(json.dumps(inventory), encoding="utf-8")
        return path

    def test_loads_the_version_one_local_dir_inventory(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = self.write_inventory(Path(temporary_directory), self.inventory())
            inventory = load_local_dir_inventory(path)

        self.assertEqual(len(inventory.local_directories), 1)
        fixture = inventory.local_directories[0]
        self.assertEqual(fixture.path.as_posix(), "local-dir")
        self.assertEqual(fixture.files[0].metadata_timestamp, 1_720_000_000.25)

    def test_rejects_an_unsafe_local_dir_metadata_path(self) -> None:
        inventory = self.inventory()
        local_directory = inventory["local_directories"][0]  # type: ignore[index]
        local_directory["files"][0]["metadata_path"] = "../token"  # type: ignore[index]
        with TemporaryDirectory() as temporary_directory:
            path = self.write_inventory(Path(temporary_directory), inventory)
            with self.assertRaisesRegex(
                ConformanceError, "normalized relative POSIX path"
            ):
                load_local_dir_inventory(path)

    def test_rejects_a_windows_drive_local_dir_path(self) -> None:
        inventory = self.inventory()
        local_directory = inventory["local_directories"][0]  # type: ignore[index]
        local_directory["path"] = "C:/outside"  # type: ignore[index]
        with TemporaryDirectory() as temporary_directory:
            path = self.write_inventory(Path(temporary_directory), inventory)
            with self.assertRaisesRegex(
                ConformanceError, "normalized relative POSIX path"
            ):
                load_local_dir_inventory(path)

    def test_prepares_an_independent_copy_with_fresh_file_metadata(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            inventory_path = self.write_inventory(temporary, self.inventory())
            fixture = load_local_dir_inventory(inventory_path).local_directories[0]
            source = temporary / "local-dir"
            file_path = source / "nested" / "config.json"
            metadata_path = (
                source
                / ".cache"
                / "huggingface"
                / "download"
                / "nested"
                / "config.json.metadata"
            )
            file_path.parent.mkdir(parents=True)
            metadata_path.parent.mkdir(parents=True)
            file_path.write_bytes(b"{}")
            metadata_path.write_text(
                f"{'4' * 40}\n{'1' * 40}\n1720000000.25\n",
                encoding="utf-8",
            )

            prepared = prepare_local_dir_fixture(
                temporary, fixture, temporary / "prepared"
            )

            prepared_file = prepared / "nested" / "config.json"
            self.assertTrue(prepared_file.is_file())
            self.assertLessEqual(prepared_file.stat().st_mtime - 1, 1_720_000_000.25)
            self.assertNotEqual(prepared, source)


if __name__ == "__main__":
    unittest.main()
