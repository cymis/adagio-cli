"""Cache hits never execute an action or announce that one is running."""

import json
import subprocess
import sys
import threading
from contextlib import contextmanager, nullcontext
from types import ModuleType, SimpleNamespace

import pytest

from adagio.cli import task_exec
from adagio.executors.telemetry import (
    TaskTelemetry,
    execution_timings,
    relay_task_progress,
)
from adagio.monitor.api import Monitor


@pytest.fixture
def worker(tmp_path, monkeypatch):
    clock = [0.0]
    calls = []

    def spend(seconds):
        clock[0] += seconds

    class Artifact:
        @staticmethod
        def load(path):
            spend(2)
            return Artifact()

        def save(self, path):
            spend(5)
            return path + ".qza"

    class Action:
        signature = SimpleNamespace(
            parameters={}, outputs={"result": SimpleNamespace(qiime_type="Artifact")}
        )

        def __call__(self, **kwargs):
            calls.append(kwargs)
            spend(6)
            return SimpleNamespace(result=Artifact())

    class Pool:
        index = {"invocation": {"result": "artifact-id"}}

        def create_index(self):
            spend(3)

        def load(self, key):
            spend(4)
            return Artifact()

    pool = Pool()

    class Cache:
        def __init__(self, path):
            self.named_pool = None
            self.process_pool = SimpleNamespace(save=lambda value: value)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        @contextmanager
        def create_pool(self, **kwargs):
            self.named_pool = pool
            try:
                yield pool
            finally:
                self.named_pool = None

    def manager():
        spend(1)
        return SimpleNamespace(
            plugins={"test": SimpleNamespace(actions={"action": Action()})}
        )

    qiime = ModuleType("qiime2")
    qiime.Artifact, qiime.Cache, qiime.Metadata = Artifact, Cache, object
    sdk = ModuleType("qiime2.sdk")
    sdk.PluginManager = manager
    sdk.Results = lambda names, values: SimpleNamespace(**dict(zip(names, values)))
    sdk.ResultCollection = dict
    util = ModuleType("qiime2.core.type.util")
    util.is_collection_type = lambda value: False
    monkeypatch.setitem(sys.modules, "qiime2", qiime)
    monkeypatch.setitem(sys.modules, "qiime2.sdk", sdk)
    monkeypatch.setitem(sys.modules, "qiime2.core.type.util", util)
    monkeypatch.setattr(task_exec, "_build_invocation", lambda **kwargs: "invocation")
    monkeypatch.setattr(task_exec, "action_output_context", nullcontext)
    monkeypatch.setattr(task_exec.time, "monotonic", lambda: clock[0])
    spec = {
        "plugin": "test",
        "action": "action",
        "archive_inputs": {"input": "input.qza"},
        "outputs": {"result": str(tmp_path / "result")},
        "cache_path": str(tmp_path / "cache"),
        "recycle_pool": "pool",
        "result_manifest": str(tmp_path / "manifest.json"),
        "progress_path": str(tmp_path / "progress.jsonl"),
    }
    return spec, calls, pool


def execute(spec):
    from pathlib import Path

    task_exec._run_task(spec)
    phases = [
        json.loads(line)["phase"]
        for line in Path(spec["progress_path"]).read_text().splitlines()
    ]
    return phases, json.loads(Path(spec["result_manifest"]).read_text())


def test_hit_skips_computation_and_measures_cache_and_io(worker):
    spec, calls, _ = worker
    phases, result = execute(spec)
    assert phases == ["preparing_cache", "checking_cache", "using_cache"]
    assert calls == []
    assert result["reused"] is True
    timings = result["timings"]
    assert timings["plugin_setup_seconds"] == 1
    assert timings["input_loading_seconds"] == 2
    assert timings["cache_index_seconds"] == 3
    assert timings["cache_load_seconds"] == 4
    assert timings["cache_lookup_seconds"] == 7
    assert timings["action_seconds"] == 0
    assert timings["output_save_seconds"] == 5
    assert timings["worker_seconds"] == 15


def test_miss_announces_running_only_after_lookup(worker):
    spec, calls, pool = worker
    pool.index = {}
    phases, result = execute(spec)
    assert phases == ["preparing_cache", "checking_cache", "running", "saving"]
    assert len(calls) == 1
    assert result["reused"] is False
    assert result["timings"]["action_seconds"] == 6


@pytest.mark.parametrize("cache_enabled", [True, False])
def test_disabled_reuse_does_not_claim_to_check_cache(worker, cache_enabled):
    spec, calls, _ = worker
    spec["recycle_pool"] = None
    if not cache_enabled:
        spec["cache_path"] = None
    phases, result = execute(spec)
    assert phases == ["preparing", "running", "saving"]
    assert len(calls) == 1
    assert result["reused"] is False
    assert not any(name.startswith("cache_") for name in result["timings"])


def test_missing_cached_output_falls_back_to_action(worker):
    spec, calls, pool = worker
    pool.index = {"invocation": {}}
    phases, result = execute(spec)
    assert phases[-2:] == ["running", "saving"]
    assert len(calls) == 1
    assert result["reused"] is False


def test_failed_lookup_never_announces_computation(worker):
    spec, calls, pool = worker

    def fail():
        raise OSError("unreadable cache")

    pool.create_index = fail
    with pytest.raises(OSError, match="unreadable cache"):
        execute(spec)
    from pathlib import Path

    assert "running" not in Path(spec["progress_path"]).read_text()
    assert calls == []


def test_progress_arrives_while_real_subprocess_is_still_running(tmp_path):
    path = tmp_path / "phases.jsonl"
    released = tmp_path / "released"
    seen = []

    class RecordingMonitor(Monitor):
        def update_task_phase(self, *, task_id, phase):
            seen.append((task_id, phase))
            released.touch()

    code = """
import pathlib, sys, time
path, released = map(pathlib.Path, sys.argv[1:])
with path.open('a') as stream:
    stream.write('{"phase":"checking_cache"}\\n')
deadline = time.monotonic() + 5
while not released.exists():
    if time.monotonic() > deadline:
        raise RuntimeError('progress was held until exit')
    time.sleep(0.01)
print('scientific stdout')
"""
    with relay_task_progress(path=path, monitor=RecordingMonitor(), task_id="node"):
        result = subprocess.run(
            [sys.executable, "-c", code, str(path), str(released)],
            capture_output=True,
            text=True,
            timeout=10,
        )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "scientific stdout\n"
    assert seen == [("node", "checking_cache")]


def test_relay_ignores_partial_lines_and_drains_on_error(tmp_path):
    path = tmp_path / "phases.jsonl"
    seen = []
    observed = threading.Event()

    class RecordingMonitor(Monitor):
        def update_task_phase(self, *, task_id, phase):
            seen.append(phase)
            observed.set()

    with (
        pytest.raises(RuntimeError),
        relay_task_progress(path=path, monitor=RecordingMonitor(), task_id="node"),
    ):
        with path.open("a") as stream:
            stream.write('{"phase":"checking_')
            stream.flush()
            assert not observed.wait(0.2)
            stream.write('cache"}\nnot json\n{"phase":"unknown"}\n')
        assert observed.wait(2)
        TaskTelemetry(str(path)).phase("using_cache")
        raise RuntimeError("worker failed")
    assert seen == ["checking_cache", "using_cache"]


def test_worker_and_environment_times_are_separate_and_legacy_manifests_work():
    assert execution_timings({}, run_seconds=9) == {"run_seconds": 9}
    timings = execution_timings(
        {"timings": {"worker_seconds": 5, "action_seconds": 0}},
        run_seconds=9,
        pull_seconds=2,
    )
    assert timings["environment_overhead_seconds"] == 4
    assert timings["run_seconds"] == 9
    assert timings["pull_seconds"] == 2


def test_relay_drains_a_final_write_after_reader_reaches_eof(tmp_path, monkeypatch):
    from pathlib import Path

    from adagio.executors import telemetry

    path = tmp_path / "phases.jsonl"
    at_eof = threading.Event()
    stopped = threading.Event()
    seen = []
    original_open = Path.open

    class RecordingMonitor(Monitor):
        def update_task_phase(self, *, task_id, phase):
            seen.append(phase)

    @contextmanager
    def pause_at_eof(self, mode="r", *args, **kwargs):
        with original_open(self, mode, *args, **kwargs) as stream:
            if self != path or mode != "r":
                yield stream
                return

            def readline():
                line = stream.readline()
                if not line and not at_eof.is_set():
                    at_eof.set()
                    assert stopped.wait(2)
                return line

            yield SimpleNamespace(tell=stream.tell, seek=stream.seek, readline=readline)

    monkeypatch.setattr(Path, "open", pause_at_eof)
    monkeypatch.setattr(
        telemetry,
        "threading",
        SimpleNamespace(Event=lambda: stopped, Thread=threading.Thread),
    )
    with relay_task_progress(path=path, monitor=RecordingMonitor(), task_id="node"):
        assert at_eof.wait(2)
        TaskTelemetry(str(path)).phase("using_cache")
    assert seen == ["using_cache"]


@pytest.mark.parametrize("kind", ["docker", "conda", "apptainer"])
def test_launchers_relay_cache_hit_and_preserve_worker_timings(
    tmp_path, monkeypatch, kind
):
    from pathlib import Path

    from adagio.executors.apptainer import ApptainerTaskEnvironmentLauncher
    from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
    from adagio.executors.conda import CondaTaskEnvironmentLauncher
    from adagio.executors.container_support import host_path_from_container
    from adagio.executors.docker import DockerTaskEnvironmentLauncher
    from adagio.model.task import PluginActionTask

    task = PluginActionTask.model_validate(
        {
            "id": "node",
            "kind": "plugin-action",
            "plugin": "test",
            "action": "test",
            "inputs": {},
            "parameters": {},
            "outputs": {"result": {"kind": "archive", "id": "output"}},
        }
    )
    phases = []

    class RecordingMonitor(Monitor):
        def update_task_phase(self, *, task_id, phase):
            phases.append(phase)

    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0, "digest", "")
        spec_path = host_path_from_container(command[command.index("--task") + 1])
        spec = json.loads(spec_path.read_text())
        progress_path = host_path_from_container(spec["progress_path"])
        telemetry = TaskTelemetry(str(progress_path))
        for phase in ("preparing_cache", "checking_cache", "using_cache"):
            telemetry.phase(phase)
        Path(host_path_from_container(spec["result_manifest"])).write_text(
            json.dumps(
                {
                    "outputs": spec["outputs"],
                    "reused": True,
                    "timings": {
                        "action_seconds": 0.0,
                        "cache_index_seconds": 0.25,
                        "worker_seconds": 0.5,
                    },
                }
            )
        )
        return subprocess.CompletedProcess(command, 0, "worker output", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    executable = tmp_path / "conda"
    executable.touch()
    image = tmp_path / "plugin.sif"
    image.touch()
    monkeypatch.setattr(
        "adagio.executors.apptainer._resolve_runtime_executable", lambda: "apptainer"
    )
    launcher = {
        "docker": DockerTaskEnvironmentLauncher,
        "conda": CondaTaskEnvironmentLauncher,
        "apptainer": ApptainerTaskEnvironmentLauncher,
    }[kind]()
    reference = str(image) if kind == "apptainer" else str(tmp_path / "environment")
    result = launcher.launch(
        environment=TaskEnvironmentSpec(
            kind=kind,
            reference=reference,
            options={"conda_executable": str(executable)},
        ),
        request=TaskExecutionRequest(
            task=task,
            cwd=tmp_path,
            work_path=tmp_path,
            archive_inputs={},
            archive_collection_inputs={},
            metadata_inputs={},
            params={},
            metadata_column_kwargs={},
            outputs={"result": str(tmp_path / "result")},
            cache_path=str(tmp_path / "cache"),
            recycle_pool="pool",
        ),
        monitor=RecordingMonitor(),
    )
    assert phases == ["preparing_cache", "checking_cache", "using_cache"]
    assert result.reused is True
    assert result.timings["action_seconds"] == 0
    assert result.timings["cache_index_seconds"] == 0.25
    assert result.outputs["result"] == str(tmp_path / "result")
