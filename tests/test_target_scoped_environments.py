"""Environment resolution is scoped to the nodes that actually run.

Running one node used to fail when *any* downstream node lacked a configured
environment: the consumer-environment scan resolved every action in the
pipeline before the plan was pruned to the target's closure. These tests pin
the scoping (a selected node ignores downstream configuration), the case that
scoping alone would have broken (a lone import materialized by an out-of-closure
consumer), and the fail-fast a full run still gets.
"""

import shutil
import tempfile
import unittest
from pathlib import Path

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
    "fields": [],
}


class ConfiguredPluginResolver:
    """Resolves only plugins with a configured image, mirroring the real resolver.

    ``ConfigurableTaskEnvironmentResolver`` raises ``ValueError`` for a plugin
    with no image and no global default -- exactly the editor's situation, where
    per-plugin catalog defaults are emitted but the ``defaults`` section is empty.
    """

    def __init__(self, images: dict[str, str]) -> None:
        self._images = images
        self.resolved: list[str] = []

    def resolve(self, *, task):  # noqa: ANN001
        self.resolved.append(task.id)
        reference = self._images.get(task.plugin)
        if reference is None:
            raise ValueError(
                "No execution environment is configured for plugin "
                f'"{task.plugin}". Provide a plugin or task environment in the '
                "run configuration."
            )
        return TaskEnvironmentSpec(kind="docker", reference=reference)


class RecordingLauncher:
    """Records launches and fakes data-import materialization."""

    kind = "docker"

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
        if request.task.plugin == DATA_IMPORT_PLUGIN:
            dest = request.outputs["artifact"]
            path = dest if dest.endswith(".qza") else f"{dest}.qza"
            Path(path).write_bytes(b"placeholder-qza")
            return TaskExecutionResult(outputs={"artifact": path}, reused=False)
        return TaskExecutionResult(
            outputs={name: path for name, path in request.outputs.items()},
            reused=False,
        )

    @property
    def launched_task_ids(self) -> list[str]:
        return [request.task.id for request in self.requests]


def _import_node(*, node_id: str, source_id: str, output_id: str) -> dict:
    return {
        "id": node_id,
        "kind": "built-in",
        "name": "data-import",
        "inputs": {"source": {"kind": "archive", "id": source_id}},
        "parameters": {
            "semantic_type": {"kind": "literal", "value": "EMPSingleEndSequences"},
            "validate_level": {"kind": "literal", "value": "max"},
        },
        "outputs": {"artifact": {"kind": "archive", "id": output_id}},
    }


def _action_node(
    *,
    node_id: str,
    plugin: str,
    action: str,
    input_id: str,
    output_id: str | None,
) -> dict:
    return {
        "id": node_id,
        "kind": "plugin-action",
        "plugin": plugin,
        "action": action,
        "inputs": {"data": {"kind": "archive", "id": input_id}},
        "parameters": {},
        "outputs": (
            {"out": {"kind": "archive", "id": output_id}} if output_id else {}
        ),
    }


def _pipeline(*, outputs: list[dict], graph: list[dict]) -> AdagioPipeline:
    return AdagioPipeline.model_validate(
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
                "outputs": outputs,
            },
            "graph": graph,
        }
    )


class TargetScopedEnvironmentTests(unittest.TestCase):
    def _run(
        self,
        *,
        pipeline: AdagioPipeline,
        images: dict[str, str],
        target_ids: set[str] | None = None,
        outputs_arg: object = None,
    ) -> tuple[RecordingLauncher, ConfiguredPluginResolver, Path]:
        # Exposed on self so a test that expects execute() to raise can still
        # assert on what was (not) launched.
        launcher = self.launcher = RecordingLauncher()
        resolver = self.resolver = ConfiguredPluginResolver(images)
        executor = TaskEnvironmentExecutor(
            environment_resolver=resolver,
            launchers={"docker": launcher},
        )
        tmpdir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        source = tmpdir / "emp-single-end-sequences"
        source.mkdir()
        (source / "sequences.fastq.gz").write_bytes(b"")
        results = tmpdir / "results"

        executor.execute(
            pipeline=pipeline,
            arguments=AdagioArguments(
                inputs={"source": str(source)},
                parameters={},
                outputs=str(results) if outputs_arg is None else outputs_arg,
            ),
            target_ids=target_ids,
        )
        return launcher, resolver, results

    def test_selected_node_runs_when_downstream_node_has_no_environment(self) -> None:
        """The reported bug: run demux, feature_table unconfigured downstream."""
        pipeline = _pipeline(
            outputs=[],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
                _action_node(
                    node_id="feature-table-1",
                    plugin="feature_table",
                    action="summarize",
                    input_id="demux-out",
                    output_id="ft-out",
                ),
            ],
        )

        # Only demux has an image; feature_table is unconfigured.
        launcher, resolver, _ = self._run(
            pipeline=pipeline,
            images={"demux": "q2-demux:test"},
            target_ids={"demux-1"},
        )

        self.assertEqual(launcher.launched_task_ids, ["demux-1"])
        self.assertNotIn("feature-table-1", resolver.resolved)

    def test_selected_node_run_ignores_unconfigured_parallel_branch(self) -> None:
        """A sibling branch that never runs must not be resolved either."""
        pipeline = _pipeline(
            outputs=[],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
                _action_node(
                    node_id="other-1",
                    plugin="dada2",
                    action="denoise",
                    input_id="import-out",
                    output_id="other-out",
                ),
            ],
        )

        launcher, resolver, _ = self._run(
            pipeline=pipeline,
            images={"demux": "q2-demux:test"},
            target_ids={"demux-1"},
        )

        self.assertEqual(launcher.launched_task_ids, ["demux-1"])
        self.assertNotIn("other-1", resolver.resolved)

    def test_lone_import_materializes_via_out_of_closure_consumer(self) -> None:
        """Strict closure-scoping would break this: the only consumer is downstream.

        Targeting just the import means no plugin action is in the closure, yet
        the raw artifact still has to be imported inside *some* action's
        environment to be saved as a .qza.
        """
        pipeline = _pipeline(
            outputs=[
                {
                    "id": "import-out",
                    "name": "artifact",
                    "type": "EMPSingleEndSequences",
                    "ast": AST,
                    "description": None,
                }
            ],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
            ],
        )

        launcher, _, results = self._run(
            pipeline=pipeline,
            images={"demux": "q2-demux:test"},
            target_ids={"import-1"},
        )

        # The import ran (borrowing demux's environment) and produced a .qza,
        # but demux itself was never executed.
        self.assertTrue((results / "artifact.qza").exists())
        self.assertEqual(
            [
                request.task.id
                for request in launcher.requests
                if request.task.plugin == DATA_IMPORT_PLUGIN
            ],
            ["data-import-import-out"],
        )
        self.assertNotIn("demux-1", launcher.launched_task_ids)

    def test_materialization_falls_back_to_a_configured_consumer(self) -> None:
        """An unconfigured first consumer no longer kills a resolvable import."""
        pipeline = _pipeline(
            outputs=[
                {
                    "id": "import-out",
                    "name": "artifact",
                    "type": "EMPSingleEndSequences",
                    "ast": AST,
                    "description": None,
                }
            ],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="unconfigured-1",
                    plugin="feature_table",
                    action="summarize",
                    input_id="import-out",
                    output_id="ft-out",
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
            ],
        )

        _, _, results = self._run(
            pipeline=pipeline,
            images={"demux": "q2-demux:test"},
            target_ids={"import-1"},
        )

        self.assertTrue((results / "artifact.qza").exists())

    def test_unconfigured_upstream_of_target_fails_before_wasting_work(self) -> None:
        """The other half of the contract: the closure must still be validated.

        Scoping the preflight to the *literal* target ids instead of the target
        closure would leave every other test in this file green, so this pins
        the ``prune_to_targets`` call itself. The chain puts a configured,
        expensive action in front of the unconfigured one: fail-fast means that
        action never launches, which a run that merely dies mid-flight would.
        """
        pipeline = _pipeline(
            outputs=[],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="expensive-1",
                    plugin="dada2",
                    action="denoise",
                    input_id="import-out",
                    output_id="exp-out",
                ),
                _action_node(
                    node_id="upstream-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="exp-out",
                    output_id="up-out",
                ),
                _action_node(
                    node_id="target-1",
                    plugin="feature_table",
                    action="summarize",
                    input_id="up-out",
                    output_id="ft-out",
                ),
            ],
        )

        # The target itself is configured; an action in its upstream closure is not.
        with self.assertRaises(RuntimeError) as caught:
            self._run(
                pipeline=pipeline,
                images={"dada2": "q2-dada2:test", "feature_table": "q2-ft:test"},
                target_ids={"target-1"},
            )

        self.assertIn("upstream-1", str(caught.exception))
        # Nothing ran -- not even the configured action ahead of the bad one.
        self.assertEqual(self.launcher.launched_task_ids, [])

    def test_full_run_fails_fast_before_launching_anything(self) -> None:
        """A full run still refuses to start when a node is unconfigured."""
        pipeline = _pipeline(
            outputs=[],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
                _action_node(
                    node_id="feature-table-1",
                    plugin="feature_table",
                    action="summarize",
                    input_id="demux-out",
                    output_id="ft-out",
                ),
            ],
        )

        with self.assertRaises(RuntimeError) as caught:
            self._run(
                pipeline=pipeline,
                images={"demux": "q2-demux:test"},
                target_ids=None,
            )

        message = str(caught.exception)
        self.assertIn("feature-table-1", message)
        self.assertIn("feature_table", message)
        # Fail-fast: the configured upstream demux node must not have run first.
        self.assertEqual(self.launcher.launched_task_ids, [])

    def test_preflight_reports_every_unconfigured_node_at_once(self) -> None:
        pipeline = _pipeline(
            outputs=[],
            graph=[
                _import_node(
                    node_id="import-1", source_id="input-1", output_id="import-out"
                ),
                _action_node(
                    node_id="demux-1",
                    plugin="demux",
                    action="emp_single",
                    input_id="import-out",
                    output_id="demux-out",
                ),
                _action_node(
                    node_id="feature-table-1",
                    plugin="feature_table",
                    action="summarize",
                    input_id="demux-out",
                    output_id="ft-out",
                ),
            ],
        )

        with self.assertRaises(RuntimeError) as caught:
            self._run(pipeline=pipeline, images={}, target_ids=None)

        message = str(caught.exception)
        self.assertIn("demux-1", message)
        self.assertIn("feature-table-1", message)


if __name__ == "__main__":
    unittest.main()
