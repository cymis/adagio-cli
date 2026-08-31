import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult
from adagio.executors.common import plan_execution_order
from adagio.executors.serial_runner import resolve_pipeline_input, run_serial_pipeline
from adagio.executors.serial_runner import SerialExecutionState
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.executors.task_environments import _save_outputs
from adagio.model.arguments import AdagioArguments
from adagio.model.task import InputVal, PluginActionTask
from adagio.monitor.api import Monitor


@dataclass(frozen=True)
class FakeEndpoint:
    id: str


@dataclass(frozen=True)
class FakeOutputDef:
    id: str
    name: str


@dataclass(frozen=True)
class FakeInputDef:
    id: str
    name: str
    type: str
    required: bool


@dataclass
class FakeTask:
    id: str
    outputs: dict[str, FakeEndpoint]
    kind: str = "plugin-action"
    plugin: str = "dummy"
    action: str = "action"
    inputs: dict[str, FakeEndpoint | InputVal] = field(default_factory=dict)


class FakeSignature:
    def __init__(
        self, outputs: list[FakeOutputDef], inputs: list[FakeInputDef] | None = None
    ) -> None:
        self.inputs: list[FakeInputDef] = inputs or []
        self.parameters: list[object] = []
        self.outputs = outputs

    def validate_arguments(self, arguments: AdagioArguments) -> None:
        del arguments

    def get_params(self, arguments: AdagioArguments) -> dict[str, object]:
        del arguments
        return {}


class FakePipeline:
    def __init__(
        self,
        *,
        tasks: list[FakeTask],
        outputs: list[FakeOutputDef],
        inputs: list[FakeInputDef] | None = None,
    ) -> None:
        self.signature = FakeSignature(outputs, inputs)
        self._tasks = tasks

    def validate_graph(self) -> None:
        return None

    def iter_tasks(self):
        return iter(self._tasks)


class RecordingMonitor(Monitor):
    def __init__(self) -> None:
        self.total_tasks: list[int] = []
        self.save_start_count = 0
        self.save_finish_count = 0
        self.saved_outputs: list[tuple[str, str, str, str]] = []
        self.finished_tasks: list[dict[str, object]] = []

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        self.total_tasks.append(total_tasks)

    def finish_task(
        self,
        *,
        task_id: str,
        status: str = "completed",
        error: str | None = None,
        **details,
    ) -> None:
        self.finished_tasks.append(
            {"task_id": task_id, "status": status, "error": error, **details}
        )

    def start_save_output(self) -> None:
        self.save_start_count += 1

    def finish_output(
        self,
        *,
        output_id: str,
        output_name: str,
        destination: str,
        status: str = "succeeded",
        error: str | None = None,
    ) -> None:
        del error
        self.saved_outputs.append((output_id, output_name, destination, status))

    def finish_save_output(self) -> None:
        self.save_finish_count += 1


class RecordingResolver:
    def resolve(self, *, task):  # noqa: ANN001
        del task
        return TaskEnvironmentSpec(kind="recording", reference="recording")


class RecordingLauncher:
    kind = "recording"

    def __init__(self) -> None:
        self.request = None

    def launch(self, *, environment, request, console=None):  # noqa: ANN001
        del environment, console
        self.request = request
        return TaskExecutionResult(outputs={})


class SerialRunnerOutputTests(unittest.TestCase):
    def test_publish_copies_output_without_replacing_normal_destination(self) -> None:
        output_def = FakeOutputDef(id="out-1", name="result")
        pipeline = FakePipeline(
            tasks=[FakeTask(id="task-1", outputs={"result": FakeEndpoint("out-1")})],
            outputs=[output_def],
        )
        monitor = RecordingMonitor()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "outputs"
            publish_path = root / "published" / "named-result"
            arguments = AdagioArguments(
                inputs={},
                parameters={},
                outputs=str(output_dir),
                publish={"result": str(publish_path)},
            )

            def resolve_task(task, state, console):  # noqa: ANN001
                del task, console
                produced = state.work_path / "task-result.qza"
                produced.write_text("published", encoding="utf-8")
                state.scope["out-1"] = str(produced)
                return False

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=arguments,
                resolve_task=resolve_task,
                finish_outputs=_save_outputs,
                monitor=monitor,
            )

            normal = output_dir / "result.qza"
            published = publish_path.with_suffix(".qza")
            self.assertEqual(normal.read_text(encoding="utf-8"), "published")
            self.assertEqual(published.read_text(encoding="utf-8"), "published")
            self.assertEqual(monitor.saved_outputs[0][2], str(normal))

    def test_collection_input_manifest_expands_to_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "matrices.tsv"
            manifest.write_text(
                "key\tpath\n1\tdm-a.qza\n2\tdata/dm-b.qza\n",
                encoding="utf-8",
            )

            resolved = resolve_pipeline_input(
                source=str(manifest),
                type_name="List[DistanceMatrix]",
                cwd=root,
            )

        self.assertEqual(
            resolved,
            [
                str((root / "dm-a.qza").resolve()),
                str((root / "data" / "dm-b.qza").resolve()),
            ],
        )

    def test_collection_input_list_resolves_each_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            resolved = resolve_pipeline_input(
                source=["dm-a.qza", "nested/dm-b.qza"],
                type_name="List[DistanceMatrix]",
                cwd=root,
            )

        self.assertEqual(
            resolved,
            [
                str((root / "dm-a.qza").resolve()),
                str((root / "nested" / "dm-b.qza").resolve()),
            ],
        )

    def test_omitted_optional_input_does_not_block_task_execution(self) -> None:
        pipeline = FakePipeline(
            inputs=[
                FakeInputDef(
                    id="required-input",
                    name="seqs",
                    type="SampleData[Sequences]",
                    required=True,
                ),
                FakeInputDef(
                    id="optional-input",
                    name="tree",
                    type="Phylogeny[Rooted]",
                    required=False,
                ),
            ],
            tasks=[
                FakeTask(
                    id="task-1",
                    inputs={
                        "seqs": InputVal(kind="archive", id="required-input"),
                        "tree": InputVal(kind="archive", id="optional-input"),
                    },
                    outputs={},
                )
            ],
            outputs=[],
        )
        arguments = AdagioArguments(
            inputs={"seqs": "seqs.qza", "tree": "<fill me>"},
            parameters={},
            outputs={},
        )
        seen_scope: dict[str, object] = {}
        seen_missing_optional_ids: set[str] = set()

        def resolve_task(task, state, console):  # noqa: ANN001
            del task, console
            seen_scope.update(state.scope)
            seen_missing_optional_ids.update(state.missing_optional_ids)
            return False

        run_serial_pipeline(
            pipeline=pipeline,
            arguments=arguments,
            resolve_task=resolve_task,
            finish_outputs=_save_outputs,
        )

        self.assertIn("required-input", seen_scope)
        self.assertNotIn("optional-input", seen_scope)
        self.assertIn("optional-input", seen_missing_optional_ids)

    def test_task_environment_executor_omits_missing_optional_inputs(self) -> None:
        launcher = RecordingLauncher()
        executor = TaskEnvironmentExecutor(
            environment_resolver=RecordingResolver(),
            launchers={launcher.kind: launcher},
        )
        task = PluginActionTask.model_validate(
            {
                "id": "task-1",
                "kind": "plugin-action",
                "plugin": "feature_table",
                "action": "tabulate_seqs",
                "inputs": {
                    "data": {"kind": "archive", "id": "data-input"},
                    "taxonomy": {"kind": "archive", "id": "taxonomy-input"},
                },
                "parameters": {},
                "outputs": {},
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = SerialExecutionState(
                cwd=root,
                work_path=root,
                params={},
                scope={"data-input": "data.qza"},
                cache_config=None,
                missing_optional_ids={"taxonomy-input"},
            )

            executor._resolve_task(task, state, None)

        assert launcher.request is not None
        self.assertEqual(launcher.request.archive_inputs, {"data": "data.qza"})
        self.assertEqual(launcher.request.archive_collection_inputs, {})

    def test_preserves_completed_output_when_later_task_fails(self) -> None:
        output_def = FakeOutputDef(id="out-1", name="result")
        pipeline = FakePipeline(
            tasks=[
                FakeTask(id="task-1", outputs={"result": FakeEndpoint("out-1")}),
                FakeTask(id="task-2", outputs={"other": FakeEndpoint("out-2")}),
            ],
            outputs=[output_def],
        )
        monitor = RecordingMonitor()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "outputs"
            arguments = AdagioArguments(
                inputs={}, parameters={}, outputs=str(output_dir)
            )

            def resolve_task(task, state, console):  # noqa: ANN001
                del console
                if task.id == "task-1":
                    produced = state.work_path / "task-1_result.qza"
                    produced.write_text("done", encoding="utf-8")
                    state.scope["out-1"] = str(produced)
                    return False
                raise RuntimeError("task 2 failed")

            with self.assertRaisesRegex(RuntimeError, "task 2 failed"):
                run_serial_pipeline(
                    pipeline=pipeline,
                    arguments=arguments,
                    resolve_task=resolve_task,
                    finish_outputs=_save_outputs,
                    monitor=monitor,
                )

            saved_path = output_dir / "result.qza"
            self.assertTrue(saved_path.exists())
            self.assertEqual(saved_path.read_text(encoding="utf-8"), "done")
            self.assertEqual(monitor.save_start_count, 1)
            self.assertEqual(monitor.save_finish_count, 1)
            failed = next(
                event
                for event in monitor.finished_tasks
                if event["task_id"] == "task-2"
            )
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["error"], "task 2 failed")
            self.assertIn("Traceback (most recent call last):", failed["traceback"])
            self.assertIn('raise RuntimeError("task 2 failed")', failed["traceback"])
            self.assertIn("RuntimeError: task 2 failed", failed["traceback"])

    def test_partial_run_does_not_require_pruned_outputs(self) -> None:
        # Two independent branches; a targeted run of task-1 prunes task-2, so
        # its output (out-2) is never produced. The terminal require_all save
        # must NOT demand out-2 (that KeyError'd every editor "run node").
        out1 = FakeOutputDef(id="out-1", name="result")
        out2 = FakeOutputDef(id="out-2", name="other")
        pipeline = FakePipeline(
            tasks=[
                FakeTask(id="task-1", outputs={"result": FakeEndpoint("out-1")}),
                FakeTask(id="task-2", outputs={"other": FakeEndpoint("out-2")}),
            ],
            outputs=[out1, out2],
        )
        monitor = RecordingMonitor()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "outputs"
            arguments = AdagioArguments(inputs={}, parameters={}, outputs=str(output_dir))

            ran: list[str] = []

            def resolve_task(task, state, console):  # noqa: ANN001
                del console
                ran.append(task.id)
                produced = state.work_path / f"{task.id}.qza"
                produced.write_text("done", encoding="utf-8")
                state.scope[task.outputs["result"].id] = str(produced)
                return False

            # Must not raise despite out-2 never being produced.
            run_serial_pipeline(
                pipeline=pipeline,
                arguments=arguments,
                resolve_task=resolve_task,
                finish_outputs=_save_outputs,
                monitor=monitor,
                target_ids={"task-1"},
            )

            self.assertEqual(ran, ["task-1"])  # task-2 pruned
            self.assertTrue((output_dir / "result.qza").exists())
            self.assertFalse((output_dir / "other.qza").exists())

    def test_partial_run_ignores_unrelated_task_with_missing_dependency(self) -> None:
        selected = FakeTask(id="get-gut-to-soil-metadata-1", outputs={})
        unrelated = FakeTask(
            id="tabulate-1",
            inputs={"input": InputVal(kind="archive", id="unresolved-input")},
            outputs={},
        )
        pipeline = FakePipeline(tasks=[selected, unrelated], outputs=[])
        arguments = AdagioArguments(inputs={}, parameters={}, outputs={})
        monitor = RecordingMonitor()
        ran: list[str] = []

        def resolve_task(task, state, console):  # noqa: ANN001
            del state, console
            ran.append(task.id)
            return False

        with patch(
            "adagio.executors.serial_runner.plan_execution_order",
            wraps=plan_execution_order,
        ) as planner:
            run_serial_pipeline(
                pipeline=pipeline,
                arguments=arguments,
                resolve_task=resolve_task,
                finish_outputs=lambda **kwargs: None,
                monitor=monitor,
                target_ids={selected.id},
            )

        planned_tasks = planner.call_args.kwargs["tasks"]
        self.assertEqual([task.id for task in planned_tasks], [selected.id])
        self.assertEqual(ran, [selected.id])
        self.assertEqual(monitor.total_tasks, [1])

    def test_full_run_reports_unrelated_task_missing_dependency(self) -> None:
        selected = FakeTask(id="get-gut-to-soil-metadata-1", outputs={})
        unrelated = FakeTask(
            id="tabulate-1",
            inputs={"input": InputVal(kind="archive", id="unresolved-input")},
            outputs={},
        )
        pipeline = FakePipeline(tasks=[selected, unrelated], outputs=[])
        arguments = AdagioArguments(inputs={}, parameters={}, outputs={})
        ran: list[str] = []

        def resolve_task(task, state, console):  # noqa: ANN001
            del state, console
            ran.append(task.id)
            return False

        with self.assertRaisesRegex(
            RuntimeError, r"tabulate-1: missing \[unresolved-input\]"
        ):
            run_serial_pipeline(
                pipeline=pipeline,
                arguments=arguments,
                resolve_task=resolve_task,
                finish_outputs=lambda **kwargs: None,
            )

        self.assertEqual(ran, [])

    def test_full_run_still_requires_every_output(self) -> None:
        # A full run where a task fails to produce its declared output must
        # still surface the missing-output KeyError (regression guard for the
        # partial-run relaxation above).
        out1 = FakeOutputDef(id="out-1", name="result")
        pipeline = FakePipeline(
            tasks=[FakeTask(id="task-1", outputs={"result": FakeEndpoint("out-1")})],
            outputs=[out1],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            arguments = AdagioArguments(
                inputs={}, parameters={}, outputs=str(Path(tmpdir) / "outputs")
            )

            def resolve_task(task, state, console):  # noqa: ANN001
                del task, state, console  # never populates scope["out-1"]
                return False

            with self.assertRaisesRegex(KeyError, "Missing output value for 'result'"):
                run_serial_pipeline(
                    pipeline=pipeline,
                    arguments=arguments,
                    resolve_task=resolve_task,
                    finish_outputs=_save_outputs,
                )

    def test_saves_each_output_only_once_across_multiple_tasks(self) -> None:
        output_def = FakeOutputDef(id="out-1", name="result")
        pipeline = FakePipeline(
            tasks=[
                FakeTask(id="task-1", outputs={"result": FakeEndpoint("out-1")}),
                FakeTask(id="task-2", outputs={"other": FakeEndpoint("out-2")}),
            ],
            outputs=[output_def],
        )
        monitor = RecordingMonitor()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "outputs"
            arguments = AdagioArguments(
                inputs={}, parameters={}, outputs=str(output_dir)
            )

            def resolve_task(task, state, console):  # noqa: ANN001
                del console
                if task.id == "task-1":
                    produced = state.work_path / "task-1_result.qza"
                    produced.write_text("done", encoding="utf-8")
                    state.scope["out-1"] = str(produced)
                    return False
                produced = state.work_path / "task-2_other.qza"
                produced.write_text("other", encoding="utf-8")
                state.scope["out-2"] = str(produced)
                return False

            run_serial_pipeline(
                pipeline=pipeline,
                arguments=arguments,
                resolve_task=resolve_task,
                finish_outputs=_save_outputs,
                monitor=monitor,
            )

            self.assertEqual(
                monitor.saved_outputs,
                [
                    (
                        output_def.id,
                        output_def.name,
                        str(output_dir / "result.qza"),
                        "succeeded",
                    )
                ],
            )
            self.assertEqual(monitor.save_start_count, 1)
            self.assertEqual(monitor.save_finish_count, 1)
