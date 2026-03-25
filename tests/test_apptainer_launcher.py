import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.apptainer import ApptainerTaskEnvironmentLauncher
from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.container_support import (
    containerize_path,
    local_source_root,
    mount_roots,
)
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
            "plugin": "dada2",
            "action": "denoise_single",
            "inputs": {},
            "parameters": {},
            "outputs": {"table": {"kind": "archive", "id": "out-1"}},
        }
    )


class ApptainerLauncherTests(unittest.TestCase):
    def test_launch_builds_apptainer_exec_command(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            image_path = root / "q2-dada2.sif"
            image_path.write_text("stub", encoding="utf-8")
            output_path = work_path / "table.qza"
            input_path = cwd / "input.qza"
            input_path.write_text("input", encoding="utf-8")

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"seqs": str(input_path)},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"table": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = containerize_path(
                task_spec_path(task_id=task.id, work_path=work_path)
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"table": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.apptainer.shutil.which",
                    side_effect=["/usr/bin/apptainer", None],
                ),
                patch(
                    "adagio.executors.apptainer.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
            ):
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference=str(image_path),
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        bind_targets = {
            f"{root_path}:{containerize_path(root_path)}:rw"
            for root_path in mount_roots(
                [cwd, work_path, input_path, local_source_root()]
            )
        }

        self.assertEqual(command[0], "/usr/bin/apptainer")
        self.assertEqual(command[1], "exec")
        self.assertIn("--no-home", command)
        self.assertIn("--pwd", command)
        self.assertIn(containerize_path(cwd), command)
        self.assertIn(str(image_path), command)
        self.assertIn("env", command)
        self.assertIn(f"PYTHONPATH={containerize_path(local_source_root())}", command)
        self.assertIn("python", command)
        self.assertIn("-m", command)
        self.assertIn("adagio.cli.task_exec", command)
        self.assertIn("--task", command)
        self.assertIn(expected_spec, command)
        self.assertTrue(bind_targets.issubset(set(command)))
        self.assertEqual(result.outputs, {"table": str(output_path)})
        self.assertFalse(result.reused)

    def test_launch_falls_back_to_singularity(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            image_path = root / "q2-dada2.sif"
            image_path.write_text("stub", encoding="utf-8")
            output_path = work_path / "table.qza"
            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"table": str(output_path)},
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"table": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.apptainer.shutil.which",
                    side_effect=[None, "/usr/bin/singularity"],
                ),
                patch(
                    "adagio.executors.apptainer.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference=str(image_path),
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        self.assertEqual(command[0], "/usr/bin/singularity")

    def test_launch_rejects_non_local_image_reference(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()

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
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"table": str(work_path / "table.qza")},
            )

            with self.assertRaisesRegex(RuntimeError, "local \\.sif image paths"):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference="docker://ghcr.io/cymis/qiime2-plugin-dada2:2026.1",
                    ),
                    request=request,
                )
