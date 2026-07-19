"""Unit tests for containerizing manifest interior paths (task_exec).

A QIIME manifest lists fastq files by absolute host path. When a plugin runs in
a container, host roots are bind-mounted under /host, so the manifest's interior
paths must be remapped or QIIME can't find the fastqs. These tests cover that
remap in isolation (no QIIME required).
"""
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from adagio.cli.task_exec import (
    _containerize_manifest_text,
    _localize_manifest_source,
)

MANIFEST = (
    "sample-id\tabsolute-filepath\n"
    "sample1\t/data/study/sample1.fastq.gz\n"
    "sample2\t/data/study/sample2.fastq.gz\n"
)


def _mounted_only(path: str) -> bool:
    """Simulate a container: only the /host-mounted copies exist."""
    return path.startswith("/host/data/study/") and path.endswith(".fastq.gz")


class ContainerizeManifestTextTests(unittest.TestCase):
    def test_remaps_interior_paths_to_host_mount(self) -> None:
        out = _containerize_manifest_text(
            text=MANIFEST, exists=_mounted_only, mount_point="/host"
        )
        self.assertIn("/host/data/study/sample1.fastq.gz", out)
        self.assertIn("/host/data/study/sample2.fastq.gz", out)
        # header + column name untouched (no leading slash)
        self.assertTrue(out.startswith("sample-id\tabsolute-filepath\n"))
        # sample ids untouched
        self.assertIn("sample1\t", out)

    def test_noop_when_original_paths_exist(self) -> None:
        # On the host, the original absolute paths resolve -> nothing rewritten.
        out = _containerize_manifest_text(
            text=MANIFEST, exists=lambda p: True, mount_point="/host"
        )
        self.assertEqual(out, MANIFEST)

    def test_idempotent(self) -> None:
        once = _containerize_manifest_text(
            text=MANIFEST, exists=_mounted_only, mount_point="/host"
        )

        def exists_after(path: str) -> bool:
            # After one pass, the already-/host paths exist; bare ones do not.
            return path.startswith("/host/data/study/")

        twice = _containerize_manifest_text(
            text=once, exists=exists_after, mount_point="/host"
        )
        self.assertEqual(once, twice)

    def test_leaves_unmounted_paths_alone(self) -> None:
        # Neither the bare path nor a /host copy exists -> unchanged (clear error later).
        out = _containerize_manifest_text(
            text=MANIFEST, exists=lambda p: False, mount_point="/host"
        )
        self.assertEqual(out, MANIFEST)


class LocalizeManifestSourceTests(unittest.TestCase):
    def test_non_manifest_format_is_passthrough(self) -> None:
        self.assertEqual(
            _localize_manifest_source(path="/x/table.biom", input_format="BIOMV210Format"),
            "/x/table.biom",
        )
        self.assertEqual(
            _localize_manifest_source(path="/x/whatever", input_format=None),
            "/x/whatever",
        )

    def test_manifest_rewritten_to_temp_when_paths_need_mount(self) -> None:
        with TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.tsv"
            manifest.write_text(MANIFEST, encoding="utf-8")
            with mock.patch(
                "adagio.cli.task_exec.os.path.exists", side_effect=_mounted_only
            ):
                out = _localize_manifest_source(
                    path=str(manifest), input_format="SingleEndFastqManifestPhred33V2"
                )
            self.assertNotEqual(out, str(manifest))
            rewritten = Path(out).read_text(encoding="utf-8")
            self.assertIn("/host/data/study/sample1.fastq.gz", rewritten)
            self.assertNotIn("\t/data/study/sample1.fastq.gz", rewritten)
            os.unlink(out)

    def test_manifest_passthrough_on_host(self) -> None:
        with TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.tsv"
            manifest.write_text(MANIFEST, encoding="utf-8")
            with mock.patch(
                "adagio.cli.task_exec.os.path.exists", return_value=True
            ):
                out = _localize_manifest_source(
                    path=str(manifest), input_format="SingleEndFastqManifestPhred33V2"
                )
            # Original paths resolve -> return the original file unchanged.
            self.assertEqual(out, str(manifest))


if __name__ == "__main__":
    unittest.main()
