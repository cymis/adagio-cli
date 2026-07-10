"""Tests for docker output cleanup: capture-to-file + single-line pre-pull."""
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from adagio.executors.container_support import record_container_output
from adagio.executors.docker import _ensure_docker_image
from adagio.executors.task_contract import container_log_path


def _completed(cmd, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(cmd, returncode, stdout, stderr)


class RecordContainerOutputTests(unittest.TestCase):
    def test_writes_combined_stdout_and_stderr(self) -> None:
        with TemporaryDirectory() as tmp:
            log = Path(tmp) / "sub" / "task_container.log"
            record_container_output(
                log_path=log, stdout_text="OUT\n", stderr_text="ERR\n"
            )
            self.assertEqual(log.read_text(), "OUT\nERR\n")

    def test_logging_failure_is_swallowed(self) -> None:
        # A directory in place of the file makes write_text raise; must not bubble.
        with TemporaryDirectory() as tmp:
            log = Path(tmp)  # a directory, not a file
            record_container_output(log_path=log, stdout_text="x", stderr_text="y")


class ContainerLogPathTests(unittest.TestCase):
    def test_path_shape(self) -> None:
        with TemporaryDirectory() as tmp:
            p = container_log_path(task_id="a/b c", work_path=Path(tmp))
            self.assertTrue(p.name.endswith("_container.log"))
            # task id separators are normalized like the other *_path helpers
            self.assertNotIn("/", p.name)
            self.assertNotIn(" ", p.name)


class EnsureDockerImageTests(unittest.TestCase):
    def test_present_image_is_not_pulled(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return _completed(cmd, returncode=0)  # inspect -> present

        with mock.patch("adagio.executors.docker.subprocess.run", side_effect=fake_run):
            _ensure_docker_image(reference="img:tag", platform=None, console=None)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:3], ["docker", "image", "inspect"])

    def test_absent_image_is_pulled_quietly_with_platform(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:3] == ["docker", "image", "inspect"]:
                return _completed(cmd, returncode=1)  # absent
            return _completed(cmd, returncode=0)  # pull ok

        with mock.patch("adagio.executors.docker.subprocess.run", side_effect=fake_run):
            _ensure_docker_image(
                reference="img:tag", platform="linux/amd64", console=None
            )

        self.assertEqual(len(calls), 2)
        pull = calls[1]
        self.assertEqual(pull[:3], ["docker", "pull", "--quiet"])
        self.assertIn("--platform", pull)
        self.assertIn("linux/amd64", pull)
        self.assertEqual(pull[-1], "img:tag")

    def test_pull_failure_raises(self) -> None:
        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["docker", "image", "inspect"]:
                return _completed(cmd, returncode=1)
            return _completed(cmd, returncode=1, stderr="denied")

        with mock.patch("adagio.executors.docker.subprocess.run", side_effect=fake_run):
            with self.assertRaises(RuntimeError) as ctx:
                _ensure_docker_image(reference="img:tag", platform=None, console=None)
        self.assertIn("denied", str(ctx.exception))

    def test_missing_docker_binary_raises_system_exit(self) -> None:
        with mock.patch(
            "adagio.executors.docker.subprocess.run", side_effect=FileNotFoundError
        ):
            with self.assertRaises(SystemExit):
                _ensure_docker_image(reference="img:tag", platform=None, console=None)

    def test_uri_reference_is_skipped(self) -> None:
        with mock.patch("adagio.executors.docker.subprocess.run") as run_mock:
            _ensure_docker_image(
                reference="docker://img:tag", platform=None, console=None
            )
        run_mock.assert_not_called()

    def test_no_pull_line_when_inline_monitor_active(self) -> None:
        printed = []
        console = mock.Mock()
        console._adagio_inline_monitor_active = True
        console.print = lambda *a, **k: printed.append(a)

        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["docker", "image", "inspect"]:
                return _completed(cmd, returncode=1)
            return _completed(cmd, returncode=0)

        with mock.patch("adagio.executors.docker.subprocess.run", side_effect=fake_run):
            _ensure_docker_image(reference="img:tag", platform=None, console=console)

        self.assertEqual(printed, [])  # nothing printed while the live table owns the console


if __name__ == "__main__":
    unittest.main()
