"""Tests for strict portable fixture regeneration comparison."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from portable_fixture_comparison import (
    ComparisonError,
    compare_portable_fixture_trees,
)


class PortableFixtureComparisonTests(unittest.TestCase):
    """Require matching paths, entry types, and file bytes."""

    def roots(self, temporary_directory: str) -> tuple[Path, Path]:
        temporary_root = Path(temporary_directory)
        checked_in = temporary_root / "checked-in"
        generated = temporary_root / "generated"
        checked_in.mkdir()
        generated.mkdir()
        return checked_in, generated

    def test_accepts_identical_trees_and_ignores_only_named_source_entries(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            for root in (checked_in, generated):
                (root / "cache" / "repo" / "snapshots").mkdir(parents=True)
                (root / "cache" / "repo" / "snapshots" / "config.json").write_bytes(
                    b"fixture\n"
                )
            (checked_in / "README.md").write_text("documentation\n", encoding="utf-8")
            (checked_in / "generate.py").write_text("# generator\n", encoding="utf-8")
            (checked_in / "__pycache__").mkdir()
            (checked_in / "__pycache__" / "generate.pyc").write_bytes(b"transient")

            compare_portable_fixture_trees(checked_in, generated)

    def test_does_not_exclude_named_entries_below_the_fixture_root(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (checked_in / "cache").mkdir()
            (generated / "cache").mkdir()
            (checked_in / "cache" / "README.md").write_bytes(b"expected\n")
            (generated / "cache" / "README.md").write_bytes(b"changed\n")

            with self.assertRaisesRegex(ComparisonError, "file bytes differ"):
                compare_portable_fixture_trees(checked_in, generated)

    def test_rejects_a_source_only_entry_in_generated_output(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (checked_in / "README.md").write_bytes(b"source maintained\n")
            (generated / "README.md").write_bytes(b"generator should not write this\n")

            with self.assertRaisesRegex(ComparisonError, "unexpected generated path"):
                compare_portable_fixture_trees(checked_in, generated)

    def test_rejects_a_missing_generated_path(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (checked_in / "inventory.json").write_bytes(b"{}\n")

            with self.assertRaisesRegex(ComparisonError, "missing generated path"):
                compare_portable_fixture_trees(checked_in, generated)

    def test_rejects_an_unexpected_generated_path(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (generated / "unexpected.json").write_bytes(b"{}\n")

            with self.assertRaisesRegex(ComparisonError, "unexpected generated path"):
                compare_portable_fixture_trees(checked_in, generated)

    def test_rejects_different_file_bytes(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (checked_in / "inventory.json").write_bytes(b"expected\n")
            (generated / "inventory.json").write_bytes(b"changed\n")

            with self.assertRaisesRegex(ComparisonError, "file bytes differ"):
                compare_portable_fixture_trees(checked_in, generated)

    def test_rejects_different_entry_types(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            checked_in, generated = self.roots(temporary_directory)
            (checked_in / "cache").mkdir()
            (generated / "cache").write_bytes(b"not a directory")

            with self.assertRaisesRegex(ComparisonError, "entry type differs"):
                compare_portable_fixture_trees(checked_in, generated)


if __name__ == "__main__":
    unittest.main()
