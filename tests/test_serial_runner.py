import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

from adagio.executors.serial_runner import resolve_pipeline_input, run_serial_pipeline
from adagio.executors.task_environments import _save_outputs
from adagio.model.arguments import AdagioArguments
from adagio.monitor.api import Monitor


@dataclass(frozen=True)
class FakeEndpoint:
    id: str


@dataclass(frozen=True)
class FakeOutputDef:
    id: str
    name: str


@dataclass
class FakeTask:
    id: str
    outputs: dict[str, FakeEndpoint]
    kind: str = "plugin-action"
    plugin: str = "dummy"
    action: str = "action"
    inputs: dict[str, FakeEndpoint] = field(default_factory=dict)


class FakeSignature:
    def __init__(self, outputs: list[FakeOutputDef]) -> None:
        self.inputs: list[object] = []
        self.parameters: list[object] = []
        self.outputs = outputs

    def validate_arguments(self, arguments: AdagioArguments) -> None:
        del arguments

    def get_params(self, arguments: AdagioArguments) -> dict[str, object]:
        del arguments
        return {}


class FakePipeline:
    def __init__(self, *, tasks: list[FakeTask], outputs: list[FakeOutputDef]) -> None:
        self.signature = FakeSignature(outputs)
        self._tasks = tasks

    def validate_graph(self) -> None:
        return None

    def iter_tasks(self):
        return iter(self._tasks)


class RecordingMonitor(Monitor):
    def __init__(self) -> None:
        self.save_start_count = 0
        self.save_finish_count = 0
        self.saved_outputs: list[tuple[str, str, str, str]] = []

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


class SerialRunnerOutputTests(unittest.TestCase):
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
