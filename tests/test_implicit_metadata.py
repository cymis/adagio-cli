import tempfile
import unittest
from pathlib import Path

from adagio.executors.base import (
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline


class PluginResolver:
    def resolve(self, *, task):  # noqa: ANN001
        return TaskEnvironmentSpec(kind="test", reference=f"{task.plugin}:test")


class MetadataRecordingLauncher:
    kind = "test"

    def __init__(self) -> None:
        self.requests: list[TaskExecutionRequest] = []

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console=None,  # noqa: ANN001
        monitor=None,  # noqa: ANN001
        task_id=None,  # noqa: ANN001
    ) -> TaskExecutionResult:
        del environment, console, monitor, task_id
        self.requests.append(request)

        outputs: dict[str, str] = {}
        for name, destination in request.outputs.items():
            path = f"{destination}.qza"
            Path(path).write_bytes(b"artifact")
            outputs[name] = path

        metadata_outputs: dict[str, str] = {}
        for name, destination in (request.metadata_outputs or {}).items():
            Path(destination).write_text("id\tvalue\n", encoding="utf-8")
            metadata_outputs[name] = destination

        return TaskExecutionResult(
            outputs=outputs,
            metadata_outputs=metadata_outputs,
        )


def _action(
    *,
    node_id: str,
    plugin: str,
    action: str,
    inputs: dict,
    outputs: dict,
) -> dict:
    return {
        "id": node_id,
        "kind": "plugin-action",
        "plugin": plugin,
        "action": action,
        "inputs": inputs,
        "parameters": {},
        "outputs": outputs,
    }


def _pipeline(*, legacy_conversion: bool) -> AdagioPipeline:
    producer_output_id = "stats-artifact"
    consumer_input_id = producer_output_id
    graph = [
        _action(
            node_id="producer",
            plugin="dada2",
            action="denoise_single",
            inputs={},
            outputs={"denoising_stats": {"kind": "archive", "id": producer_output_id}},
        )
    ]

    if legacy_conversion:
        consumer_input_id = "stats-metadata"
        graph.append(
            {
                "id": "convert",
                "kind": "built-in",
                "name": "convert-to-metadata",
                "inputs": {"data": {"kind": "archive", "id": producer_output_id}},
                "parameters": {},
                "outputs": {
                    "metadata": {"kind": "archive", "id": consumer_input_id}
                },
            }
        )

    graph.append(
        _action(
            node_id="consumer",
            plugin="metadata",
            action="tabulate",
            inputs={"input": {"kind": "metadata", "id": consumer_input_id}},
            outputs={},
        )
    )

    return AdagioPipeline.model_validate(
        {
            "type": "pipeline",
            "signature": {"inputs": [], "parameters": [], "outputs": []},
            "graph": graph,
        }
    )


class ImplicitMetadataTests(unittest.TestCase):
    def _run(self, *, legacy_conversion: bool) -> MetadataRecordingLauncher:
        launcher = MetadataRecordingLauncher()
        executor = TaskEnvironmentExecutor(
            environment_resolver=PluginResolver(),
            launchers={"test": launcher},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            executor.execute(
                pipeline=_pipeline(legacy_conversion=legacy_conversion),
                arguments=AdagioArguments(
                    inputs={},
                    parameters={},
                    outputs=str(Path(tmpdir) / "results"),
                ),
            )
        return launcher

    def test_materializes_direct_metadata_input_in_producer_environment(self) -> None:
        launcher = self._run(legacy_conversion=False)
        producer, consumer = launcher.requests

        metadata_path = producer.metadata_outputs["denoising_stats"]  # type: ignore[index]
        self.assertTrue(metadata_path.endswith("_metadata.tsv"))
        self.assertEqual(consumer.metadata_inputs, {"input": metadata_path})
        self.assertNotEqual(
            consumer.metadata_inputs["input"],
            producer.outputs["denoising_stats"],
        )

    def test_legacy_conversion_node_propagates_producer_metadata_view(self) -> None:
        launcher = self._run(legacy_conversion=True)
        producer, consumer = launcher.requests

        metadata_path = producer.metadata_outputs["denoising_stats"]  # type: ignore[index]
        self.assertEqual(consumer.metadata_inputs, {"input": metadata_path})


if __name__ == "__main__":
    unittest.main()
