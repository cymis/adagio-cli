import subprocess
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.conda import CondaTaskEnvironmentLauncher
from adagio.executors.serial_runner import (
    REUSED_LOG_NOTE,
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
from adagio.monitor.api import Monitor


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
            conda_executable = root / "bin" / "conda"
            conda_executable.parent.mkdir()
            conda_executable.write_text("stub", encoding="utf-8")
            prefix = root / "envs" / "q2-2026"
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
                        reference=str(prefix),
                        options={"conda_executable": str(conda_executable)},
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
            self.assertEqual(result.image_ref, str(prefix))


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


class _RecordingMonitor(Monitor):
    """Capture ``finish_task`` calls so tests can assert on relayed details."""

    def __init__(self) -> None:
        self.finished: list[dict] = []

    def finish_task(self, *, task_id, status="completed", error=None, **details):  # noqa: ANN001
        self.finished.append(
            {"task_id": task_id, "status": status, "error": error, **details}
        )


def _noop_finish_outputs(*, sig, arguments, state, monitor, require_all):  # noqa: ANN001
    del sig, arguments, state, monitor, require_all


class FailedTaskLogTests(unittest.TestCase):
    """A failed task's log is the one users most need — it must survive."""

    def test_failed_task_persists_container_log(self) -> None:
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])
        monitor = _RecordingMonitor()

        def resolve_task(task, state, console):  # noqa: ANN001
            del console
            # The launcher writes the log, *then* the task raises - which is
            # exactly the ordering that used to lose it.
            container_log_path(
                task_id=task.id, work_path=state.work_path
            ).write_text("boom traceback\n", encoding="utf-8")
            raise RuntimeError("task blew up")

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "persisted-logs"

            with self.assertRaises(RuntimeError):
                run_serial_pipeline(
                    pipeline=pipeline,
                    arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                    resolve_task=resolve_task,
                    finish_outputs=_noop_finish_outputs,
                    monitor=monitor,
                    log_dir=str(log_dir),
                )

            persisted = log_dir / "task-1_container.log"
            self.assertTrue(persisted.exists())
            self.assertEqual(
                persisted.read_text(encoding="utf-8"), "boom traceback\n"
            )

        # The adapter only relays logs when the event carries log_path.
        failed = [f for f in monitor.finished if f["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["log_path"], str(persisted))

    def test_failed_task_without_log_still_reports_failure(self) -> None:
        """No container log (e.g. the launcher never started) must not break."""
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])
        monitor = _RecordingMonitor()

        def resolve_task(task, state, console):  # noqa: ANN001
            del task, state, console
            raise RuntimeError("died before launching")

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                run_serial_pipeline(
                    pipeline=pipeline,
                    arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                    resolve_task=resolve_task,
                    finish_outputs=_noop_finish_outputs,
                    monitor=monitor,
                    log_dir=str(Path(tmp) / "persisted-logs"),
                )

        failed = [f for f in monitor.finished if f["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertNotIn("log_path", failed[0])
        self.assertEqual(failed[0]["error"], "died before launching")


class ReusedTaskLogTests(unittest.TestCase):
    """A cache hit launches no container, so it needs to say so itself."""

    def test_reused_task_persists_explanatory_note(self) -> None:
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])
        monitor = _RecordingMonitor()

        def resolve_task(task, state, console):  # noqa: ANN001
            del task, state, console
            return TaskOutcome(reused=True)

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "persisted-logs"

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                resolve_task=resolve_task,
                finish_outputs=_noop_finish_outputs,
                monitor=monitor,
                log_dir=str(log_dir),
            )

            persisted = log_dir / "task-1_container.log"
            self.assertTrue(persisted.exists())
            self.assertEqual(persisted.read_text(encoding="utf-8"), REUSED_LOG_NOTE)

        cached = [f for f in monitor.finished if f["status"] == "cached"]
        self.assertEqual(len(cached), 1)
        self.assertEqual(cached[0]["log_path"], str(persisted))

    def test_real_container_log_wins_over_the_note(self) -> None:
        """A task that did run keeps its own output even if flagged reused."""
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])

        def resolve_task(task, state, console):  # noqa: ANN001
            del console
            container_log_path(
                task_id=task.id, work_path=state.work_path
            ).write_text("real output\n", encoding="utf-8")
            return TaskOutcome(reused=True)

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "persisted-logs"

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                resolve_task=resolve_task,
                finish_outputs=_noop_finish_outputs,
                log_dir=str(log_dir),
            )

            persisted = log_dir / "task-1_container.log"
            self.assertEqual(persisted.read_text(encoding="utf-8"), "real output\n")

    def test_non_reused_task_without_log_writes_nothing(self) -> None:
        """The note is scoped to cache hits; it must not appear generally."""
        pipeline = _Pipeline([_Task(id="task-1", outputs={})])

        def resolve_task(task, state, console):  # noqa: ANN001
            del task, state, console
            return TaskOutcome(reused=False)

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "persisted-logs"

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=AdagioArguments(inputs={}, parameters={}, outputs={}),
                resolve_task=resolve_task,
                finish_outputs=_noop_finish_outputs,
                log_dir=str(log_dir),
            )

            self.assertFalse((log_dir / "task-1_container.log").exists())


if __name__ == "__main__":
    unittest.main()
