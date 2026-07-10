import subprocess
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.conda import CondaTaskEnvironmentLauncher
from adagio.executors.serial_runner import (
    TaskOutcome,
    run_serial_pipeline,
)
from adagio.executors.task_contract import (
    build_result_manifest,
    container_log_path,
    result_manifest_path,
    write_json_file,
)
from adagio.model.arguments import AdagioArguments
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


class CondaContainerLogTests(unittest.TestCase):
    def test_conda_launcher_writes_container_log(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()
        task = _task()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
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
                        outputs={"visualization": str(output_path)}, reused=False
                    ),
                )
                return subprocess.CompletedProcess(
                    cmd, 0, "conda-stdout\n", "conda-stderr\n"
                )

            with patch(
                "adagio.executors.conda.subprocess.run", side_effect=fake_run
            ):
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="conda",
                        reference="q2-2026",
                        options={"conda_reference_type": "environment"},
                    ),
                    request=request,
                )

            log_path = container_log_path(task_id=task.id, work_path=work_path)
            self.assertTrue(log_path.exists())
            contents = log_path.read_text(encoding="utf-8")
            self.assertIn("conda-stdout", contents)
            self.assertIn("conda-stderr", contents)
            # Enrichment surfaces the log path + exit code.
            self.assertEqual(result.log_path, str(log_path))
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.image_ref, "q2-2026")


# --- run_serial_pipeline --log-dir copy -------------------------------------
@dataclass(frozen=True)
class _Endpoint:
    id: str


@dataclass
class _Task:
    id: str
    outputs: dict
    kind: str = "plugin-action"
    plugin: str = "p"
    action: str = "a"
    inputs: dict = field(default_factory=dict)


class _Sig:
    inputs: list = []
    parameters: list = []
    outputs: list = []

    def validate_arguments(self, arguments):  # noqa: ANN001
        del arguments

    def get_params(self, arguments):  # noqa: ANN001
        del arguments
        return {}


class _Pipeline:
    def __init__(self, tasks):
        self.signature = _Sig()
        self._tasks = tasks

    def validate_graph(self):
        return None

    def iter_tasks(self):
        return iter(self._tasks)


class LogDirCopyTests(unittest.TestCase):
    def test_log_dir_receives_task_container_log(self) -> None:
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "persisted-logs"
            captured: dict = {}

            def resolve_task(task, state, console):  # noqa: ANN001
                del console
                # Simulate a launcher writing the per-task container log.
                log_path = container_log_path(
                    task_id=task.id, work_path=state.work_path
                )
                log_path.write_text("container output\n", encoding="utf-8")
                captured["work_log"] = log_path
                return TaskOutcome(reused=False, enrichment={})

            def finish_outputs(*, sig, arguments, state, monitor, require_all):  # noqa: ANN001
                del sig, arguments, state, monitor, require_all

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                resolve_task=resolve_task,
                finish_outputs=finish_outputs,
                log_dir=str(log_dir),
            )

            persisted = log_dir / "task-1_container.log"
            self.assertTrue(persisted.exists())
            self.assertEqual(
                persisted.read_text(encoding="utf-8"), "container output\n"
            )
            # The original (temp) log is gone after teardown; the copy survives.
            self.assertFalse(captured["work_log"].exists())

    def test_no_log_dir_does_not_copy(self) -> None:
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])

        def resolve_task(task, state, console):  # noqa: ANN001
            del console
            container_log_path(
                task_id=task.id, work_path=state.work_path
            ).write_text("x", encoding="utf-8")
            return TaskOutcome(reused=False)

        def finish_outputs(*, sig, arguments, state, monitor, require_all):  # noqa: ANN001
            del sig, arguments, monitor, require_all
            self.assertEqual(state.persisted_logs, {})

        run_serial_pipeline(
            pipeline=pipeline,
            arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
            resolve_task=resolve_task,
            finish_outputs=finish_outputs,
        )


if __name__ == "__main__":
    unittest.main()
