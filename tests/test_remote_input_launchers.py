"""The launchers preserve remote sources; only the worker resolves them."""

import json
import subprocess
from types import SimpleNamespace

import pytest

from adagio.executors.apptainer import ApptainerTaskEnvironmentLauncher
from adagio.executors.base import (
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from adagio.executors.conda import CondaTaskEnvironmentLauncher
from adagio.executors.docker import DockerTaskEnvironmentLauncher
from adagio.executors.serial_runner import (
    SerialExecutionState,
    _persist_task_log,
)
from adagio.executors.task_contract import result_manifest_path, task_spec_path
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.model.task import PluginActionTask

URL = "https://example.org/table.qza"
RECORD = {"url": URL, "uuid": "source-uuid", "sha256": "a" * 64, "source_id": "b" * 64}


def task():
    return PluginActionTask.model_validate(
        {
            "id": "task",
            "kind": "plugin-action",
            "plugin": "feature_table",
            "action": "summarize",
            "inputs": {"table": {"kind": "archive", "id": "input"}},
            "outputs": {},
            "parameters": {},
        }
    )


@pytest.mark.parametrize(
    "kind,launcher",
    [
        ("docker", DockerTaskEnvironmentLauncher()),
        ("apptainer", ApptainerTaskEnvironmentLauncher()),
        ("conda", CondaTaskEnvironmentLauncher()),
    ],
)
def test_remote_source_and_receipt_cross_launcher_boundary(
    monkeypatch, tmp_path, kind, launcher
):
    image = tmp_path / "image.sif"
    image.touch()
    monkeypatch.setattr(
        "adagio.executors.apptainer._resolve_runtime_executable", lambda: "apptainer"
    )
    monkeypatch.setattr(
        "adagio.executors.conda._resolve_conda_executable", lambda **kw: "conda"
    )
    request = TaskExecutionRequest(
        task=task(),
        cwd=tmp_path,
        work_path=tmp_path,
        archive_inputs={"table": URL},
        archive_collection_inputs={"others": [URL]},
        metadata_inputs={},
        params={},
        metadata_column_kwargs={},
        outputs={},
        remote_input_types={"table": "FeatureTable[Frequency]"},
    )
    manifest = result_manifest_path(task_id="task", work_path=tmp_path)

    def run(command, **kwargs):
        manifest.write_text(
            json.dumps({"outputs": {}, "reused": False, "input_downloads": [RECORD]})
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("subprocess.run", run)
    result = launcher.launch(
        environment=TaskEnvironmentSpec(kind=kind, reference=str(image)),
        request=request,
    )
    spec = json.loads(task_spec_path(task_id="task", work_path=tmp_path).read_text())
    assert spec["archive_inputs"]["table"] == URL
    assert spec["archive_collection_inputs"]["others"] == [URL]
    assert spec["remote_input_types"]["table"] == "FeatureTable[Frequency]"
    assert result.input_downloads == [RECORD]


def test_receipts_flow_into_events_and_persist_without_container_log(tmp_path):
    def launch(**kw):
        return TaskExecutionResult(outputs={}, input_downloads=[RECORD])

    executor = TaskEnvironmentExecutor(
        environment_resolver=SimpleNamespace(
            resolve=lambda **kw: TaskEnvironmentSpec(kind="test", reference="env")
        ),
        launchers={"test": SimpleNamespace(launch=launch)},
    )
    state = SerialExecutionState(
        cwd=tmp_path,
        work_path=tmp_path,
        params={},
        scope={"input": URL},
        cache_config=None,
        log_dir=tmp_path,
    )
    outcome = executor._resolve_task(task(), state, None)
    assert outcome.enrichment["input_downloads"] == [RECORD]
    _persist_task_log(
        state=state, task_id="task", downloads=outcome.enrichment["input_downloads"]
    )
    assert json.loads((tmp_path / "task_inputs.json").read_text()) == {
        "input_downloads": [RECORD]
    }


def test_unwritable_receipt_does_not_fail_successful_task(tmp_path):
    state = SerialExecutionState(
        cwd=tmp_path,
        work_path=tmp_path,
        params={},
        scope={},
        cache_config=None,
        log_dir=tmp_path / "missing",
    )
    _persist_task_log(
        state=state,
        task_id="task",
        downloads=[RECORD],
    )
