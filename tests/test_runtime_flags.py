import json
import tempfile
import unittest
import urllib.error
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

    def test_initial_job_status_carries_reproducibility(self) -> None:
        # The adapter captures the reproducibility header from job_status
        # events only, so the "running" post must carry it.
        console = Console()
        executor = _CapturingExecutor()
        posted: list[dict] = []

        def fake_post(*, runtime_url, job_id, payload):  # noqa: ANN001
            del runtime_url, job_id
            posted.append(payload)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _write(root, "spec.json", _SPEC)
            config = _write(root, "config", {"version": 1})
            argv = [
                "--spec",
                str(spec),
                "--config",
                str(config),
                "--cache-dir",
                str(root / "cache"),
                "--output-dir",
                str(root / "outputs"),
                "--job-id",
                "job-1",
                "--runtime-url",
                "http://127.0.0.1:9/api",
                "--connected",
            ]
            with patch(
                "adagio.executors.select_default_executor",
                return_value=executor,
            ), patch(
                "adagio.cli.runtime._post_job_event", side_effect=fake_post
            ), patch(
                "adagio.monitor.connected.urllib.request.urlopen",
                side_effect=urllib.error.URLError("no adapter"),
            ), patch("adagio.monitor.connected.time.sleep", return_value=None):
                runtime_cli.run_runtime(argv, console=console)

        running = [p for p in posted if p.get("status") == "running"]
        self.assertEqual(len(running), 1)
        self.assertEqual(running[0]["event"], "job_status")
        self.assertIn("adagio_version", running[0]["reproducibility"])


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class PostJobEventTransportTests(unittest.TestCase):
    def _post(self):
        with patch(
            "adagio.cli.runtime.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            runtime_cli._post_job_event(
                runtime_url="http://127.0.0.1:9/api",
                job_id="job-1",
                payload={"event": "job_status", "status": "running"},
            )
        return urlopen

    def test_authorization_header_from_runtime_token(self) -> None:
        with patch.dict("os.environ", {"RUNTIME_TOKEN": "tok-1"}):
            urlopen = self._post()
        req = urlopen.call_args_list[0].args[0]
        self.assertEqual(req.headers.get("Authorization"), "Bearer tok-1")

    def test_no_authorization_header_without_token(self) -> None:
        import os

        env = {k: v for k, v in os.environ.items() if k != "RUNTIME_TOKEN"}
        with patch.dict("os.environ", env, clear=True):
            urlopen = self._post()
        req = urlopen.call_args_list[0].args[0]
        self.assertIsNone(req.headers.get("Authorization"))


if __name__ == "__main__":
    unittest.main()
