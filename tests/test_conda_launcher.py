import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.conda import CondaTaskEnvironmentLauncher
from adagio.executors.container_support import container_python_root
from adagio.executors.task_contract import (
    build_result_manifest,
    read_json_file,
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


class CondaLauncherTests(unittest.TestCase):
    def test_launch_builds_conda_run_command(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            conda_executable = root / "bin" / "conda"
            cwd.mkdir()
            work_path.mkdir()
            conda_executable.parent.mkdir()
            conda_executable.write_text("stub", encoding="utf-8")
            output_path = work_path / "summary.qzv"
            input_path = cwd / "input.qza"
            collection_input_path = cwd / "collection-input.qza"
            input_path.write_text("input", encoding="utf-8")
            collection_input_path.write_text("collection", encoding="utf-8")

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"data": str(input_path)},
                archive_collection_inputs={"tables": [str(collection_input_path)]},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = str(task_spec_path(task_id=task.id, work_path=work_path))

            def fake_run(cmd, check, cwd, env, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": str(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch(
                "adagio.executors.conda.subprocess.run",
                side_effect=fake_run,
            ) as run_mock:
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="conda",
                        reference="qiime2-2026.1",
                        options={
                            "conda_reference_type": "environment",
                            "conda_executable": str(conda_executable),
                        },
                    ),
                    request=request,
                )

            task_spec = read_json_file(
                task_spec_path(task_id=task.id, work_path=work_path)
            )
            command = run_mock.call_args.args[0]
            kwargs = run_mock.call_args.kwargs
            python_root = container_python_root(work_path=work_path)

        self.assertEqual(
            command[:4], [str(conda_executable), "run", "-n", "qiime2-2026.1"]
        )
        self.assertIn("python", command)
        self.assertIn("-m", command)
        self.assertIn("adagio.cli.task_exec", command)
        self.assertIn("--task", command)
        self.assertIn(expected_spec, command)
        self.assertEqual(kwargs["cwd"], cwd)
        self.assertIn(str(python_root), kwargs["env"]["PYTHONPATH"])
        self.assertEqual(kwargs["env"]["PYTHONNOUSERSITE"], "1")
        self.assertEqual(task_spec["archive_inputs"], {"data": str(input_path)})
        self.assertEqual(
            task_spec["archive_collection_inputs"],
            {"tables": [str(collection_input_path)]},
        )
        self.assertEqual(result.outputs, {"visualization": str(output_path)})
        self.assertFalse(result.reused)

    def test_launch_uses_conda_prefix(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            conda_executable = root / "bin" / "conda"
            prefix = root / "envs" / "qiime2"
            cwd.mkdir()
            work_path.mkdir()
            conda_executable.parent.mkdir()
            conda_executable.write_text("stub", encoding="utf-8")
            output_path = work_path / "summary.qzv"
            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            def fake_run(cmd, check, cwd, env, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": str(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.conda.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
                patch.dict(
                    os.environ,
                    {
                        "PYTHONHOME": "/bundled/python",
                        "PYTHONPATH": "/bundled/site-packages",
                        "VIRTUAL_ENV": "/bundled/venv",
                    },
                ),
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="conda",
                        reference=str(prefix),
                        options={
                            "conda_reference_type": "prefix",
                            "conda_executable": str(conda_executable),
                        },
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        child_env = run_mock.call_args.kwargs["env"]
        python_root = container_python_root(work_path=work_path)
        self.assertEqual(command[:4], [str(conda_executable), "run", "-p", str(prefix)])
        self.assertEqual(command[4], str(prefix / "bin" / "python"))
        self.assertIn("adagio.cli.task_exec", command)
        self.assertEqual(child_env["PYTHONPATH"], str(python_root))
        self.assertNotIn("PYTHONHOME", child_env)
        self.assertNotIn("VIRTUAL_ENV", child_env)

    def test_launch_requires_environment_reference(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            request = TaskExecutionRequest(
                task=_task(),
                cwd=cwd,
                work_path=work_path,
                archive_inputs={},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(work_path / "summary.qzv")},
            )

            with self.assertRaisesRegex(RuntimeError, "environment name or prefix"):
                launcher.launch(
                    environment=TaskEnvironmentSpec(kind="conda", reference=""),
                    request=request,
                )
