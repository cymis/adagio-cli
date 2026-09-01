import sys
import tempfile
import types
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from adagio.cli.task_exec import _run_task
from adagio.executors.task_contract import read_json_file


class FakeMetadata:
    def save(self, destination: str) -> str:
        Path(destination).write_text("id\tvalue\n", encoding="utf-8")
        return destination


class FakeArtifact:
    def save(self, destination: str) -> str:
        path = f"{destination}.qza"
        Path(path).write_bytes(b"artifact")
        return path

    def view(self, view_type):  # noqa: ANN001
        if view_type is not FakeMetadata:
            raise TypeError(f"Unexpected view type: {view_type!r}")
        return FakeMetadata()


class FakeAction:
    signature = SimpleNamespace(parameters={})

    def __call__(self, **kwargs):  # noqa: ANN003
        if kwargs:
            raise TypeError(f"Unexpected action arguments: {kwargs!r}")
        return SimpleNamespace(denoising_stats=FakeArtifact())


class FakePluginManager:
    def __init__(self) -> None:
        self.plugins = {
            "dada2": SimpleNamespace(actions={"denoise_single": FakeAction()})
        }


class TaskExecMetadataTests(unittest.TestCase):
    def test_saves_requested_metadata_view_beside_archive_output(self) -> None:
        qiime2 = types.ModuleType("qiime2")
        qiime2.Artifact = FakeArtifact
        qiime2.Cache = object
        qiime2.Metadata = FakeMetadata
        qiime2_sdk = types.ModuleType("qiime2.sdk")
        qiime2_sdk.PluginManager = FakePluginManager

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive_destination = root / "stats"
            metadata_destination = root / "stats_metadata.tsv"
            manifest = root / "results.json"
            spec = {
                "plugin": "dada2",
                "action": "denoise_single",
                "archive_inputs": {},
                "archive_collection_inputs": {},
                "metadata_inputs": {},
                "params": {},
                "metadata_column_kwargs": {},
                "outputs": {"denoising_stats": str(archive_destination)},
                "metadata_outputs": {
                    "denoising_stats": str(metadata_destination)
                },
                "result_manifest": str(manifest),
                "cache_path": None,
                "recycle_pool": None,
            }

            with patch.dict(
                sys.modules,
                {"qiime2": qiime2, "qiime2.sdk": qiime2_sdk},
            ), patch(
                "adagio.cli.task_exec.action_output_context",
                side_effect=nullcontext,
            ):
                _run_task(spec)

            self.assertTrue(Path(f"{archive_destination}.qza").is_file())
            self.assertTrue(metadata_destination.is_file())
            self.assertEqual(
                read_json_file(manifest),
                {
                    "outputs": {
                        "denoising_stats": f"{archive_destination}.qza"
                    },
                    "metadata_outputs": {
                        "denoising_stats": str(metadata_destination)
                    },
                    "reused": False,
                },
            )


if __name__ == "__main__":
    unittest.main()
