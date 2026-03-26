import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.container_support import (
    container_python_root,
    containerize_path,
    mount_roots,
)
from adagio.executors.docker import DockerTaskEnvironmentLauncher
from adagio.executors.task_contract import (
    build_result_manifest,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)
from adagio.model.task import PluginActionTask


def _task() -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": "task-1",
            "kind": "plugin-action",
            "plugin": "demux",
            "action": "summarize",
            "inputs": {},
            "parameters": {},
            "outputs": {"visualization": {"kind": "archive", "id": "out-1"}},
        }
    )


class DockerLauncherTests(unittest.TestCase):
    def test_launch_builds_docker_run_command(self) -> None:
        launcher = DockerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            output_path = work_path / "summary.qzv"
            input_path = cwd / "input.qza"
            input_path.write_text("input", encoding="utf-8")

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"data": str(input_path)},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = containerize_path(
                task_spec_path(task_id=task.id, work_path=work_path)
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch(
                "adagio.executors.docker.subprocess.run",
                side_effect=fake_run,
            ) as run_mock:
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="docker",
                        reference="ghcr.io/cymis/qiime2-plugin-demux:2026.1",
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        python_root = container_python_root(work_path=work_path)
        bind_targets = {
            f"{root_path}:{containerize_path(root_path)}:rw"
            for root_path in mount_roots([cwd, work_path, input_path, python_root])
        }

        self.assertEqual(command[0], "docker")
        self.assertEqual(command[1], "run")
        self.assertEqual(command[2], "--rm")
        self.assertIn("-w", command)
        self.assertIn(containerize_path(cwd), command)
        self.assertIn(
            f"PYTHONPATH={containerize_path(python_root)}",
            command,
        )
        self.assertIn("PYTHONNOUSERSITE=1", command)
        self.assertIn("python", command)
        self.assertIn("-m", command)
        self.assertIn("adagio.cli.task_exec", command)
        self.assertIn("--task", command)
        self.assertIn(expected_spec, command)
        self.assertIn("ghcr.io/cymis/qiime2-plugin-demux:2026.1", command)
        self.assertTrue(bind_targets.issubset(set(command)))
        self.assertEqual(result.outputs, {"visualization": str(output_path)})
        self.assertFalse(result.reused)
