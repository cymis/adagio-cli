from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from adagio.backend.base import CommandResult
from adagio.backend.miniforge import (
    InstallRequest,
    _extract_wsl_runtime,
    _normalize_image_ref,
    _select_linux_runtime,
    install_compute_environment,
)


class TestMiniforgeInstaller(unittest.TestCase):
    def test_select_linux_runtime_priority(self):
        def fake_which(cmd: str) -> str | None:
            installed = {
                "nerdctl": "/usr/bin/nerdctl",
                "singularity": "/usr/bin/singularity",
            }
            return installed.get(cmd)

        self.assertEqual(_select_linux_runtime(fake_which), "nerdctl")

    def test_extract_wsl_runtime(self):
        stdout = "\n/usr/bin/podman\n"
        self.assertEqual(_extract_wsl_runtime(stdout), "podman")

    def test_normalize_image_ref(self):
        self.assertEqual(
            _normalize_image_ref("fluxrm/flux-sched:latest"),
            "docker.io/fluxrm/flux-sched:latest",
        )
        self.assertEqual(
            _normalize_image_ref("ghcr.io/org/image:tag"),
            "ghcr.io/org/image:tag",
        )

    def test_linux_apply_writes_config(self):
        calls: list[list[str]] = []

        def fake_runner(cmd: list[str]) -> CommandResult:
            calls.append(cmd)
            return CommandResult(returncode=0, stdout="", stderr="")

        def fake_which(cmd: str) -> str | None:
            if cmd == "podman":
                return "/usr/bin/podman"
            return None

        with TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "compute-environment.json"
            with patch("adagio.backend.miniforge.platform.system", return_value="Linux"):
                with patch("adagio.backend.miniforge._default_config_path", return_value=cfg):
                    report = install_compute_environment(
                        InstallRequest(apply=True, image="example/flux:1"),
                        runner=fake_runner,
                        which=fake_which,
                    )

            self.assertTrue(report.ok)
            self.assertEqual(report.runtime, "podman")
            self.assertTrue(cfg.exists())
            data = json.loads(cfg.read_text(encoding="utf-8"))
            self.assertEqual(data["runtime"]["engine"], "podman")
            self.assertEqual(data["flux"]["image"], "docker.io/example/flux:1")
            self.assertIn(["podman", "pull", "docker.io/example/flux:1"], calls)

    def test_linux_dry_run_does_not_write_config(self):
        def fake_runner(cmd: list[str]) -> CommandResult:
            return CommandResult(returncode=0, stdout="", stderr="")

        def fake_which(cmd: str) -> str | None:
            if cmd == "podman":
                return "/usr/bin/podman"
            return None

        with TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "compute-environment.json"
            with patch("adagio.backend.miniforge.platform.system", return_value="Linux"):
                with patch("adagio.backend.miniforge._default_config_path", return_value=cfg):
                    report = install_compute_environment(
                        InstallRequest(apply=False),
                        runner=fake_runner,
                        which=fake_which,
                    )

            self.assertTrue(report.ok)
            self.assertFalse(cfg.exists())
            skipped = [step for step in report.steps if step.status == "skipped"]
            self.assertGreaterEqual(len(skipped), 2)


if __name__ == "__main__":
    unittest.main()
