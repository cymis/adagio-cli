import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rich.console import Console

from adagio.cli import runtime as runtime_cli


_SPEC = {
    "type": "pipeline",
    "signature": {"inputs": [], "parameters": [], "outputs": []},
    "graph": [],
}


class _CapturingExecutor:
    mode_label = "test"

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)


def _write(root: Path, name: str, payload) -> Path:
    path = root / name
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class RuntimeFlagsTests(unittest.TestCase):
    def _run(self, extra_argv, config_payload):
        console = Console()
        executor = _CapturingExecutor()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _write(root, "spec.json", _SPEC)
            config = _write(root, "config", config_payload)
            cache = root / "cache"
            outputs = root / "outputs"
            argv = [
                "--spec",
                str(spec),
                "--config",
                str(config),
                "--cache-dir",
                str(cache),
                "--output-dir",
                str(outputs),
                *extra_argv,
            ]
            with patch(
                "adagio.executors.select_default_executor",
                return_value=executor,
            ):
                runtime_cli.run_runtime(argv, console=console)
        self.assertEqual(len(executor.calls), 1)
        return executor.calls[0]

    def test_json_config_is_accepted_by_runtime(self) -> None:
        # A JSON config used to fail with TOMLDecodeError; now auto-detected.
        call = self._run(
            [],
            {"version": 1, "defaults": {"kind": "docker", "image": "img:1"}},
        )
        self.assertIn("cache_config", call)

    def test_toml_config_still_accepted(self) -> None:
        call = self._run(
            [],
            'version = 1\n[defaults]\nkind = "docker"\nimage = "img:1"\n',
        )
        self.assertIn("cache_config", call)

    def test_recycle_pool_threads_into_cache_config(self) -> None:
        call = self._run(
            ["--recycle-pool", "pipeline:xyz"],
            {"version": 1},
        )
        self.assertEqual(call["cache_config"].recycle_pool, "pipeline:xyz")

    def test_log_dir_and_targets_thread_through(self) -> None:
        call = self._run(
            ["--log-dir", "/tmp/adagio-logs", "--targets", "n1, n2 ,n3"],
            {"version": 1},
        )
        self.assertEqual(call["log_dir"], "/tmp/adagio-logs")
        self.assertEqual(call["target_ids"], {"n1", "n2", "n3"})

    def test_defaults_when_flags_absent(self) -> None:
        call = self._run([], {"version": 1})
        self.assertIsNone(call["target_ids"])
        self.assertIsNone(call["log_dir"])
        # Default recycle pool unchanged.
        self.assertEqual(call["cache_config"].recycle_pool, "adagio-recycle")


class ReproducibilityTests(unittest.TestCase):
    def test_reproducibility_has_adagio_version_and_no_qiime(self) -> None:
        repro = runtime_cli._build_reproducibility()
        self.assertIn("adagio_version", repro)
        self.assertIsInstance(repro["adagio_version"], str)
        # qiime2 must never be imported in the host process.
        self.assertIsNone(repro["qiime_version"])
        self.assertEqual(repro["plugin_versions"], {})
        self.assertEqual(repro["image_digests"], {})


if __name__ == "__main__":
    unittest.main()
