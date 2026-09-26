"""The batch backend against a real Slurm controller, without scientific imports.

Run inside a bootstrapped test host (see bootstrap.sh). Each case uses a tiny
shell worker, so it checks scheduler behavior only: submission, accounting,
failures, a missing result manifest, cancellation and ``adagio cleanup``.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

from adagio.execution.backends import clean_up_run, create_backend
from adagio.execution.backends.base import JobState, TaskInvocation
from adagio.execution.backends.slurm import SlurmExecutorConfig
from adagio.execution.resources import TaskResourceRequirements
from adagio.executors.base import TaskExecutionRequest
from adagio.executors.container_support import container_python_root
from adagio.executors.prepared import PreparedInvocation
from adagio.executors.task_contract import write_json_file

root = Path("/workspace/acceptance-output/scheduler-edges")
root.mkdir(parents=True, exist_ok=True)
report = {}

WORKER = """
import json, sys, time
spec = json.load(open(sys.argv[1]))
time.sleep(spec.get("sleep", 0))
if spec.get("exit"):
    sys.exit(spec["exit"])
if spec.get("manifest", True):
    for path in spec["outputs"].values():
        open(path, "w").write("result")
    json.dump({"outputs": spec["outputs"], "attempt_id": spec["attempt_id"]},
              open(spec["result_manifest"], "w"))
"""


class Launcher:
    def __init__(self, **behavior):
        self.behavior = behavior

    def prepare(self, *, environment, request, shared):
        spec = request.work_path / "spec.json"
        manifest = request.work_path / "results.json"
        write_json_file(
            spec,
            {
                "outputs": dict(request.outputs),
                "result_manifest": str(manifest),
                **self.behavior,
            },
        )
        container_python_root(work_path=request.work_path, force_stage=shared)
        return PreparedInvocation(
            command=[sys.executable, "-c", WORKER, str(spec)],
            env=None,
            cwd=request.cwd,
            spec_path=spec,
            manifest_path=manifest,
            log_path=request.work_path / "task.log",
            request=request,
            image_ref=sys.executable,
        )


def backend(name, run_record=None, **options):
    config = SlurmExecutorConfig(
        work_dir=str(root / name), slurm={"partition": "test", **options}
    )
    created = create_backend(config, run_record=run_record)
    created.check_available()
    return created


def submit(backend_, workspace, node, **behavior):
    work = backend_.attempt_directory(workspace)
    request = TaskExecutionRequest(
        task=SimpleNamespace(id=node),
        cwd=workspace.cwd,
        work_path=work,
        archive_inputs={},
        archive_collection_inputs={},
        metadata_inputs={},
        params={},
        metadata_column_kwargs={},
        outputs={"out": str(work / "output")},
    )
    return backend_.submit(
        TaskInvocation(
            launcher=Launcher(**behavior),
            environment=SimpleNamespace(kind="conda", reference=sys.executable),
            request=request,
            task_id=node,
        ),
        TaskResourceRequirements(cpus=1, memory="32 MiB"),
    )


def settle(backend_, handles, timeout=120):
    deadline = time.monotonic() + timeout
    while not all(h.state.terminal for h in handles):
        if time.monotonic() > deadline:
            raise AssertionError("jobs did not finish")
        backend_.wait(handles)


def outcome(backend_, handle):
    try:
        return {"collected": sorted(backend_.collect(handle).outputs)}
    except RuntimeError as error:
        return {"error": str(error)}


def case_results():
    b = backend("results")
    with b.workspace() as workspace:
        handles = {
            "ok": submit(b, workspace, "ok"),
            "exit": submit(b, workspace, "exit", exit=3),
            "no-manifest": submit(b, workspace, "no-manifest", manifest=False),
        }
        settle(b, list(handles.values()))
        results = {name: outcome(b, h) for name, h in handles.items()}
    assert results["ok"] == {"collected": ["out"]}, results
    assert "ended FAILED (exit 3:0)" in results["exit"]["error"], results
    assert "did not write an output manifest" in results["no-manifest"]["error"], results
    assert not Path(workspace.root).exists()
    report["results"] = results


def case_rejected():
    """A submission Slurm refuses is reported plainly and leaves nothing behind."""
    b = backend("rejected")
    b.config.slurm.partition = "no-such-partition"
    with b.workspace() as workspace:
        try:
            submit(b, workspace, "rejected")
        except RuntimeError as error:
            message = str(error)
        else:
            raise AssertionError("Slurm accepted an unknown partition")
        assert b.cancel() == []
    assert "invalid partition" in message.lower(), message
    report["rejected"] = message


def case_cancel():
    b = backend("cancel")
    workspace_context = b.workspace()
    workspace = workspace_context.__enter__()
    handle = submit(b, workspace, "long", sleep=300)
    while handle.state is JobState.QUEUED:
        b.wait([handle])
    errors = b.cancel()
    assert errors == [], errors
    registry = json.loads(b.registry.path.read_text())
    (entry,) = registry["attempts"].values()
    assert entry["scheduler_state"] == "CANCELLED", entry
    report["cancel"] = {"job_id": handle.job_id, "entry": entry}


def case_cleanup_after_kill():
    """A killed driver leaves jobs; `adagio cleanup` cancels exactly those."""
    record = root / "killed-run-record.json"
    record.unlink(missing_ok=True)
    driver = subprocess.Popen(
        [sys.executable, __file__, "--driver", str(record)],
        env={**os.environ},
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if record.exists():
            registry = json.loads(
                Path(json.loads(record.read_text())["registry"]).read_text()
            )
            if len([a for a in registry["attempts"].values() if a.get("job_id")]) == 2:
                break
        time.sleep(0.5)
    driver.send_signal(signal.SIGKILL)
    driver.wait()
    cleanup = subprocess.run(
        [sys.executable, "-m", "adagio.cli.main", "cleanup", str(record)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert cleanup.returncode == 0, cleanup.stdout + cleanup.stderr
    assert json.loads(cleanup.stdout) == {"complete": True, "errors": []}
    assert not record.exists()
    registry_states = [a["scheduler_state"] for a in registry_after(registry)]
    assert registry_states == ["CANCELLED", "CANCELLED"], registry_states
    assert clean_up_run(record) == []
    report["cleanup_after_kill"] = registry_states


def registry_after(registry):
    path = Path(registry["run_dir"]) / "submissions.json"
    return list(json.loads(path.read_text())["attempts"].values())


def drive(record):
    b = backend("killed", run_record=Path(record))
    with b.workspace() as workspace:
        handles = [submit(b, workspace, f"long-{i}", sleep=300) for i in range(2)]
        settle(b, handles, timeout=600)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--driver"]:
        drive(sys.argv[2])
        sys.exit(0)
    case_results()
    case_rejected()
    case_cancel()
    case_cleanup_after_kill()
    (root / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
