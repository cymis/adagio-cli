import tempfile
import unittest
from pathlib import Path

from adagio.executors.path_utils import resolve_output_destination


class OutputFolderTests(unittest.TestCase):
    """A per-output path ending in / is a folder, whatever exists on disk."""

    def resolve(self, outputs, name="table", suffix=".qza"):
        return resolve_output_destination(
            output_name=name,
            output_names=["table", "summary"],
            outputs=outputs,
            source_path=Path(f"/cache/x{suffix}"),
        )

    def test_trailing_slash_writes_into_the_folder(self) -> None:
        self.assertEqual(self.resolve({"table": "/data/run1/"}), "/data/run1/table.qza")
        self.assertEqual(
            self.resolve({"summary": "/data/run1/"}, name="summary", suffix=".qzv"),
            "/data/run1/summary.qzv",
        )

    def test_without_the_slash_it_is_a_file(self) -> None:
        self.assertEqual(self.resolve({"table": "/data/run1"}), "/data/run1.qza")
        self.assertEqual(self.resolve({"table": "/data/t.qza"}), "/data/t.qza")

    def test_an_existing_directory_without_the_slash_is_still_a_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(self.resolve({"table": folder}), f"{folder}.qza")

    def test_publish_paths_follow_the_same_rule(self) -> None:
        # Publish destinations resolve through the same function.
        self.assertEqual(self.resolve({"table": "relative/out/"}), "relative/out/table.qza")


if __name__ == "__main__":
    unittest.main()
