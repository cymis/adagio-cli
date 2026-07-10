import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from adagio.cli.task_exec import _load_archive_input
from adagio.executors.base import (
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from adagio.executors.task_contract import DATA_IMPORT_PLUGIN
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline


AST = {
    "type": "expression",
    "builtin": False,
    "name": "SampleData",
    "predicate": None,
    "fields": [
        {
            "type": "expression",
            "builtin": False,
            "name": "SequencesWithQuality",
            "predicate": None,
            "fields": [],
        }
    ],
}


class StaticResolver:
    def resolve(self, *, task):  # noqa: ANN001
        del task
        return TaskEnvironmentSpec(kind="docker", reference="q2-demux:test")


class CapturingLauncher:
    kind = "docker"

    def __init__(self) -> None:
        self.requests: list[TaskExecutionRequest] = []

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console=None,  # noqa: ANN001
    ) -> TaskExecutionResult:
        del environment, console
        self.requests.append(request)
        return TaskExecutionResult(outputs={}, reused=False)


class MaterializingLauncher:
    """Launcher that fakes the import materialization (writes a placeholder qza)."""

    kind = "docker"

    def __init__(self) -> None:
        self.requests: list[TaskExecutionRequest] = []

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console=None,  # noqa: ANN001
    ) -> TaskExecutionResult:
        del environment, console
        self.requests.append(request)
        if request.task.plugin == DATA_IMPORT_PLUGIN:
            dest = request.outputs["artifact"]
            path = dest if dest.endswith(".qza") else f"{dest}.qza"
            Path(path).write_bytes(b"placeholder-qza")
            return TaskExecutionResult(outputs={"artifact": path}, reused=False)
        return TaskExecutionResult(outputs={}, reused=False)


class MaterializationTests(unittest.TestCase):
    def test_data_import_artifact_as_pipeline_output_is_materialized_and_saved(
        self,
    ) -> None:
        pipeline = AdagioPipeline.model_validate(
            {
                "type": "pipeline",
                "signature": {
                    "inputs": [
                        {
                            "id": "input-1",
                            "name": "source",
                            "type": "RawData",
                            "ast": AST,
                            "description": None,
                            "required": True,
                        }
                    ],
                    "parameters": [],
                    "outputs": [
                        {
                            "id": "import-out",
                            "name": "artifact",
                            "type": "EMPSingleEndSequences",
                            "ast": AST,
                            "description": None,
                        }
                    ],
                },
                "graph": [
                    {
                        "id": "import-1",
                        "kind": "built-in",
                        "name": "data-import",
                        "inputs": {"source": {"kind": "archive", "id": "input-1"}},
                        "parameters": {
                            "semantic_type": {
                                "kind": "literal",
                                "value": "EMPSingleEndSequences",
                            },
                            "validate_level": {"kind": "literal", "value": "max"},
                        },
                        "outputs": {
                            "artifact": {"kind": "archive", "id": "import-out"}
                        },
                    },
                    {
                        "id": "task-1",
                        "kind": "plugin-action",
                        "plugin": "demux",
                        "action": "emp_single",
                        "inputs": {"seqs": {"kind": "archive", "id": "import-out"}},
                        "parameters": {},
                        "outputs": {},
                    },
                ],
            }
        )
        launcher = MaterializingLauncher()
        executor = TaskEnvironmentExecutor(
            environment_resolver=StaticResolver(),
            launchers={"docker": launcher},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "emp-single-end-sequences"
            source.mkdir()
            (source / "sequences.fastq.gz").write_bytes(b"")
            results = Path(tmpdir) / "results"

            executor.execute(
                pipeline=pipeline,
                arguments=AdagioArguments(
                    inputs={"source": str(source)},
                    parameters={},
                    outputs=str(results),
                ),
            )

            # The exposed data-import artifact was imported (not copied as a raw
            # directory) and written to the results directory as a .qza.
            self.assertTrue((results / "artifact.qza").exists())

        import_requests = [
            request
            for request in launcher.requests
            if request.task.plugin == DATA_IMPORT_PLUGIN
        ]
        self.assertEqual(len(import_requests), 1)
        self.assertEqual(
            import_requests[0].archive_input_materializations,
            {
                "source": {
                    "mode": "raw",
                    "semantic_type": "EMPSingleEndSequences",
                    "validate_level": "max",
                }
            },
        )
        # The downstream consumer then loads the imported artifact directly
        # (no per-input materialization remaining).
        consumer_requests = [
            request
            for request in launcher.requests
            if request.task.plugin != DATA_IMPORT_PLUGIN
        ]
        self.assertEqual(len(consumer_requests), 1)
        self.assertEqual(consumer_requests[0].archive_input_materializations, {})


    def test_task_environment_passes_data_import_materialization_to_consumer(
        self,
    ) -> None:
        pipeline = AdagioPipeline.model_validate(
            {
                "type": "pipeline",
                "signature": {
                    "inputs": [
                        {
                            "id": "input-1",
                            "name": "sequences_source",
                            "type": "RawData",
                            "ast": AST,
                            "description": None,
                            "required": True,
                        }
                    ],
                    "parameters": [],
                    "outputs": [],
                },
                "graph": [
                    {
                        "id": "import-1",
                        "kind": "built-in",
                        "name": "data-import",
                        "inputs": {"source": {"kind": "archive", "id": "input-1"}},
                        "parameters": {
                            "semantic_type": {
                                "kind": "literal",
                                "value": "SampleData[SequencesWithQuality]",
                            },
                            "validate_level": {"kind": "literal", "value": "max"},
                        },
                        "outputs": {
                            "artifact": {"kind": "archive", "id": "import-out"}
                        },
                    },
                    {
                        "id": "task-1",
                        "kind": "plugin-action",
                        "plugin": "demux",
                        "action": "summarize",
                        "inputs": {"data": {"kind": "archive", "id": "import-out"}},
                        "parameters": {},
                        "outputs": {},
                    },
                ],
            }
        )
        launcher = CapturingLauncher()
        executor = TaskEnvironmentExecutor(
            environment_resolver=StaticResolver(),
            launchers={"docker": launcher},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "manifest.tsv"
            source.write_text("sample-id\tabsolute-filepath\n", encoding="utf-8")

            executor.execute(
                pipeline=pipeline,
                arguments=AdagioArguments(
                    inputs={"sequences_source": str(source)},
                    parameters={},
                    outputs={},
                ),
            )

        self.assertEqual(len(launcher.requests), 1)
        self.assertEqual(
            launcher.requests[0].archive_input_materializations,
            {
                "data": {
                    "mode": "raw",
                    "semantic_type": "SampleData[SequencesWithQuality]",
                    "validate_level": "max",
                }
            },
        )

    def test_task_exec_imports_raw_archive_inputs_with_action_semantic_type(
        self,
    ) -> None:
        artifact = SimpleNamespace(
            load=Mock(), import_data=Mock(return_value="artifact")
        )
        action = SimpleNamespace(
            signature=SimpleNamespace(
                inputs={
                    "data": SimpleNamespace(
                        qiime_type=(
                            "SampleData[SequencesWithQuality | "
                            "PairedEndSequencesWithQuality]"
                        )
                    )
                }
            )
        )

        with patch.dict(sys.modules, {"qiime2": SimpleNamespace(Artifact=artifact)}):
            loaded = _load_archive_input(
                action=action,
                input_name="data",
                path="/data/manifest.tsv",
                materialization={
                    "mode": "raw",
                    "semantic_type": "SampleData[SequencesWithQuality]",
                    "input_format": "SingleEndFastqManifestPhred33V2",
                    "validate_level": "min",
                },
            )

        self.assertEqual(loaded, "artifact")
        artifact.import_data.assert_called_once_with(
            "SampleData[SequencesWithQuality]",
            "/data/manifest.tsv",
            view_type="SingleEndFastqManifestPhred33V2",
            validate_level="min",
        )
        artifact.load.assert_not_called()

    def test_task_exec_imports_raw_archive_inputs_with_default_format(
        self,
    ) -> None:
        artifact = SimpleNamespace(
            load=Mock(), import_data=Mock(return_value="artifact")
        )
        action = SimpleNamespace(
            signature=SimpleNamespace(
                inputs={"seqs": SimpleNamespace(qiime_type="EMPSingleEndSequences")}
            )
        )

        with patch.dict(sys.modules, {"qiime2": SimpleNamespace(Artifact=artifact)}):
            loaded = _load_archive_input(
                action=action,
                input_name="seqs",
                path="/data/emp-single-end-sequences",
                materialization={
                    "mode": "raw",
                    "semantic_type": "EMPSingleEndSequences",
                    "validate_level": "max",
                },
            )

        self.assertEqual(loaded, "artifact")
        artifact.import_data.assert_called_once_with(
            "EMPSingleEndSequences",
            "/data/emp-single-end-sequences",
            view_type=None,
            validate_level="max",
        )
        artifact.load.assert_not_called()
