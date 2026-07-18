import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import (
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from adagio.executors.cache_support import ExecutionCacheConfig
from adagio.executors.serial_runner import SerialExecutionState, TaskOutcome
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.executors.task_contract import (
    build_result_manifest,
    result_manifest_path,
    write_json_file,
)
from adagio.executors.container_support import containerize_path
from adagio.model.task import PluginActionTask
from adagio.monitor.api import Monitor


class _RecordingMonitor(Monitor):
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def pulling_image(self, *, task_id, image_ref=None):  # noqa: ANN001
        self.events.append(("pulling_image", {"task_id": task_id, "image_ref": image_ref}))

    def starting_container(self, *, task_id, image_ref=None):  # noqa: ANN001
        self.events.append(
            ("starting_container", {"task_id": task_id, "image_ref": image_ref})
        )


class _Resolver:
    def resolve(self, *, task):  # noqa: ANN001
        del task
        return TaskEnvironmentSpec(
            kind="docker", reference="ghcr.io/x/qiime2-plugin-demux:2026.1"
        )


class _CapturingLauncher:
    kind = "docker"

    def __init__(self) -> None:
        self.requests: list[TaskExecutionRequest] = []

    def launch(self, *, environment, request, console=None):  # noqa: ANN001
        del environment, console
        self.requests.append(request)
        return TaskExecutionResult(outputs=request.outputs)


def _task(task_id: str = "task-1") -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": task_id,
            "kind": "plugin-action",
            "plugin": "demux",
            "action": "summarize",
            "inputs": {},
            "parameters": {"n": {"kind": "literal", "value": 10}},
            "outputs": {"visualization": {"kind": "archive", "id": "out-1"}},
        }
    )


class ExecutorEnrichmentTests(unittest.TestCase):
    def test_selective_no_reuse_assigns_no_pool_only_to_matching_tasks(self) -> None:
        launcher = _CapturingLauncher()
        executor = TaskEnvironmentExecutor(
            environment_resolver=_Resolver(),
            launchers={"docker": launcher},
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            work_path = root / "work"
            work_path.mkdir()
            state = SerialExecutionState(
                cwd=root,
                work_path=work_path,
                params={},
                scope={},
                cache_config=ExecutionCacheConfig(
                    cache_dir=root / "cache",
                    recycle_pool="pipeline:test",
                    no_reuse_nodes=frozenset({"local-task"}),
                ),
                monitor=None,
            )

            executor._resolve_task(_task("official-upstream"), state, None)
            executor._resolve_task(_task("local-task"), state, None)
            executor._resolve_task(_task("official-downstream"), state, None)

        pools = {
            request.task.id: request.recycle_pool for request in launcher.requests
        }
        self.assertEqual(pools["official-upstream"], "pipeline:test")
        self.assertIsNone(pools["local-task"])
        self.assertEqual(pools["official-downstream"], "pipeline:test")

    def test_plugin_action_returns_enriched_outcome_and_emits_phase_events(self) -> None:
        from adagio.executors.docker import DockerTaskEnvironmentLauncher

        executor = TaskEnvironmentExecutor(
            environment_resolver=_Resolver(),
            launchers={"docker": DockerTaskEnvironmentLauncher()},
        )
        task = _task()
        monitor = _RecordingMonitor()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            work_path = root / "work"
            work_path.mkdir()
            output_path = work_path / "summary.qzv"
            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)

            state = SerialExecutionState(
                cwd=root,
                work_path=work_path,
                params={},
                scope={},
                cache_config=None,
                monitor=monitor,
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                # The image-inspect (present check), digest lookup, and the run
                # all funnel through subprocess.run. Distinguish by argv.
                if "inspect" in cmd and "--format" in cmd:
                    return subprocess.CompletedProcess(
                        cmd, 0, "ghcr.io/x/demux@sha256:abc123\n", ""
                    )
                if cmd[:3] == ["docker", "image", "inspect"]:
                    return subprocess.CompletedProcess(cmd, 0, "", "")
                # docker run
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={
                            "visualization": containerize_path(output_path)
                        },
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "ran\n", "")

            with patch(
                "adagio.executors.docker.subprocess.run", side_effect=fake_run
            ):
                outcome = executor._resolve_task(task, state, None)

        self.assertIsInstance(outcome, TaskOutcome)
        enrichment = outcome.enrichment
        self.assertEqual(enrichment["exit_code"], 0)
        self.assertEqual(
            enrichment["image_ref"], "ghcr.io/x/qiime2-plugin-demux:2026.1"
        )
        self.assertEqual(enrichment["image_digest"], "ghcr.io/x/demux@sha256:abc123")
        self.assertFalse(enrichment["reused"])
        self.assertIn("input_signature", enrichment)
        self.assertTrue(enrichment["input_signature"].startswith("sha256:"))
        self.assertIn("timings", enrichment)
        self.assertIsInstance(enrichment["command"], list)
        # Minimal NodeResources block derived from the run timing.
        self.assertEqual(
            enrichment["resources"],
            {"wall_seconds": enrichment["timings"]["run_seconds"]},
        )

        emitted = [name for name, _ in monitor.events]
        self.assertEqual(emitted, ["pulling_image", "starting_container"])


if __name__ == "__main__":
    unittest.main()
