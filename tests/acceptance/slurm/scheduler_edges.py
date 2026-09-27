"""The batch backend against a real Slurm controller, without scientific imports.

Run inside a bootstrapped test host (see bootstrap.sh). Each case uses a tiny
shell worker, so it checks scheduler behavior only: submission, accounting,
failures, a missing result manifest, outages, cancellation and ``adagio
cleanup``, including who may clean up and what it may cancel.
"""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from adagio.execution.backends import clean_up_run, create_backend
from adagio.execution.backends.base import JobState, TaskInvocation
from adagio.execution.backends.batch import SchedulerStatus
from adagio.execution.backends.run_record import write_run_record
from adagio.execution.backends.slurm import SlurmExecutorConfig
from adagio.execution.backends.submissions import SubmissionRegistry
from adagio.execution.coordinator import WorkItem, coordinate
from adagio.execution.resources import TaskResourceRequirements
from adagio.executors.base import TaskExecutionRequest
from adagio.executors.container_support import container_python_root
from adagio.executors.prepared import PreparedInvocation
from adagio.executors.task_contract import write_json_file

root = Path("/workspace/acceptance-output/scheduler-edges")
root.mkdir(parents=True, exist_ok=True)
# Run records are locked, so they live on local disk; /workspace is a bind
# mount that accepts locks without enforcing them.
local = Path("/tmp/adagio-acceptance/scheduler-edges")
local.mkdir(parents=True, exist_ok=True)
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


def invocation(backend_, workspace, node, **behavior):
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
    return TaskInvocation(
        launcher=Launcher(**behavior),
        environment=SimpleNamespace(kind="conda", reference=sys.executable),
        request=request,
        task_id=node,
    )


RESOURCES = TaskResourceRequirements(cpus=1, memory="32 MiB")


def submit(backend_, workspace, node, **behavior):
    return backend_.submit(invocation(backend_, workspace, node, **behavior), RESOURCES)


def adagio_cleanup(record):
    result = subprocess.run(
        [sys.executable, "-m", "adagio.cli.main", "cleanup", str(record)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, json.loads(result.stdout)


def squeue_state(job_id):
    return subprocess.run(
        ["squeue", "--noheader", "--states=all", f"--jobs={job_id}", "--format=%T"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()


def sbatch(name, seconds=300):
    return subprocess.run(
        [
            "sbatch",
            "--parsable",
            "--partition=test",
            "--mem=32M",
            f"--job-name={name}",
            f"--output={root}/{name}.out",
            f"--wrap=sleep {seconds}",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def left_behind(name, *entries):
    """A run record whose registry holds ``(job_name, job_id)`` submissions."""
    run_dir = root / name
    run_dir.mkdir(exist_ok=True)
    registry = SubmissionRegistry.create(run_dir, executor="slurm")
    for index, (job_name, job_id) in enumerate(entries):
        registry.record_intent(str(index), job_name=job_name)
        if job_id:
            registry.record_submitted(str(index), job_id=job_id, cluster=None)
        else:
            registry.record_unconfirmed(str(index), "reply lost")
    registry.save()
    record = local / f"{name}-run-record.json"
    write_run_record(record, executor="slurm", registry=registry.path)
    return record, registry


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
    # Confirmed from the queue; accounting may not have recorded it yet.
    assert entry["scheduler_state"].startswith("CANCELLED"), entry
    report["cancel"] = {"job_id": handle.job_id, "entry": entry}


def case_unknown_state_is_still_cancelled():
    """A job the scheduler stops accounting for fails its task but is cancelled."""
    b = backend("unknown")
    b._accounting_timeout = 0
    real_statuses = b.scheduler.statuses
    blind = [True]
    b.scheduler.statuses = lambda jobs, run: (
        {job: SchedulerStatus(None, "no record", active=False) for job in jobs}
        if blind[0]
        else real_statuses(jobs, run)
    )
    workspace = b.workspace().__enter__()
    handle = submit(b, workspace, "lost", sleep=300)
    b.wait([handle])
    assert handle.state is JobState.FAILED, handle
    blind[0] = False
    assert b.cancel() == []
    (entry,) = json.loads(b.registry.path.read_text())["attempts"].values()
    assert entry["scheduler_state"].startswith("CANCELLED"), entry
    report["unknown_state"] = {"job_id": handle.job_id, "entry": entry["scheduler_state"]}


def case_outage_is_waited_out():
    """While Slurm cannot be asked, a running task is neither failed nor lost."""
    b = backend("outage")
    b._accounting_timeout = 0  # were an outage a loss, the task would fail at once
    real_statuses = b.scheduler.statuses
    started = time.monotonic()
    b.scheduler.statuses = lambda jobs, run: (
        {job: SchedulerStatus(None, "controller down", answered=False) for job in jobs}
        if time.monotonic() - started < 15
        else real_statuses(jobs, run)
    )
    with b.workspace() as workspace:
        handle = submit(b, workspace, "outage", sleep=5)
        settle(b, [handle])
        result = outcome(b, handle)
    assert result == {"collected": ["out"]}, result
    report["outage"] = result


def start_driver(record, *, supervised=False):
    record.unlink(missing_ok=True)
    driver = subprocess.Popen(
        [sys.executable, __file__, "--driver", str(record)]
        + (["--exit-with-stdin"] if supervised else []),
        env={**os.environ},
        stdin=subprocess.PIPE if supervised else subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if record.exists():
            registry = json.loads(
                Path(json.loads(record.read_text())["registry"]).read_text()
            )
            if len([a for a in registry["attempts"].values() if a.get("job_id")]) == 2:
                return driver, registry
        time.sleep(0.5)
    raise AssertionError("the driver did not submit its jobs")


def case_cleanup_after_kill():
    """A killed driver leaves jobs; `adagio cleanup` cancels exactly those."""
    record = local / "killed-run-record.json"
    driver, registry = start_driver(record)
    driver.send_signal(signal.SIGKILL)
    driver.wait()
    assert adagio_cleanup(record) == (0, {"complete": True, "errors": []})
    assert not record.exists()
    assert not record.with_name(record.name + ".lock").exists()
    registry_states = [a["scheduler_state"] for a in registry_after(registry)]
    assert all(s.startswith("CANCELLED") for s in registry_states), registry_states
    assert len(registry_states) == 2
    assert clean_up_run(record) == []
    report["cleanup_after_kill"] = registry_states


def case_live_owner_then_stdin_closes():
    """Cleanup leaves a live run alone; the run cancels its own jobs once its
    supervisor's end of stdin closes."""
    record = local / "supervised-run-record.json"
    driver, registry = start_driver(record, supervised=True)
    owned = adagio_cleanup(record)
    assert owned[0] == 75 and owned[1]["owner_alive"], owned
    job_ids = [a["job_id"] for a in registry_after(registry)]
    assert all(squeue_state(i) in ("PENDING", "RUNNING") for i in job_ids), job_ids
    driver.stdin.close()  # as when the supervisor dies
    assert driver.wait(timeout=60) == 3
    entries = registry_after(registry)
    assert all(e["scheduler_state"].startswith("CANCELLED") for e in entries), entries
    # The run kept its record (it did not succeed); its owner is gone now.
    assert adagio_cleanup(record) == (0, {"complete": True, "errors": []})
    assert not record.exists()
    report["live_owner"] = {"refused": owned[1], "then": [e["state"] for e in entries]}


def case_reused_id_spares_the_other_job():
    """A recorded id that now belongs to another job is never cancelled."""
    foreign = sbatch("someone-else")
    name = f"adagio-{uuid.uuid4().hex}"
    record, registry = left_behind("reused-id", (name, foreign))
    assert clean_up_run(record) == []
    try:
        assert squeue_state(foreign) in ("PENDING", "RUNNING"), squeue_state(foreign)
        (entry,) = json.loads(registry.path.read_text())["attempts"].values()
        assert entry["state"] == "ended", entry
    finally:
        subprocess.run(["scancel", foreign], check=True)
    report["reused_id"] = {"foreign_job": foreign, "entry": entry["state"]}


def lost_replies(name):
    """A registry with two lost replies: one became a job, one never did."""
    appeared = f"adagio-{uuid.uuid4().hex}"
    job_id = sbatch(appeared)
    never = f"adagio-{uuid.uuid4().hex}"
    record, registry = left_behind(name, (appeared, None), (never, None))
    data = json.loads(registry.path.read_text())
    # Requested longer ago than an explicit five-minute lifetime allows.
    data["attempts"]["1"]["intent_at"] = time.time() - 400
    registry.path.write_text(json.dumps(data))
    return record, registry, job_id


def case_lost_replies_without_a_known_lifetime():
    """This cluster sets no AuthInfo ttl, so a lost reply that never became a
    job is kept until someone who has checked settles it; one that did become
    a job is cancelled by name either way."""
    record, registry, job_id = lost_replies("lost-replies-kept")
    (error,) = clean_up_run(record)
    assert "credential lifetime could not be verified" in error, error
    assert f"adagio cleanup --settle-unconfirmed {record}" in error, error
    entries = json.loads(registry.path.read_text())["attempts"]
    assert entries["0"]["job_id"] == job_id, entries
    assert squeue_state(job_id) == "CANCELLED", squeue_state(job_id)
    assert entries["1"]["state"] == "unconfirmed" and record.exists(), entries
    # After checking the queue, the operator settles it from the command line.
    cleanup = subprocess.run(
        [sys.executable, "-m", "adagio.cli.main", "cleanup", "--settle-unconfirmed", str(record)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert cleanup.returncode == 0, cleanup.stdout + cleanup.stderr
    entries = json.loads(registry.path.read_text())["attempts"]
    assert entries["1"]["state"] == "ended" and not record.exists(), entries
    report["lost_replies_kept"] = {k: (e["state"], e["job_id"]) for k, e in entries.items()}


def case_lost_replies_with_a_known_lifetime():
    """With an explicit AuthInfo ttl, a lost reply that never became a job is
    settled once that lifetime has passed."""
    from adagio.execution.backends.slurm import SlurmScheduler

    record, registry, job_id = lost_replies("lost-replies-settled")
    known = SlurmScheduler.lost_reply_deadline
    SlurmScheduler.lost_reply_deadline = lambda self, run: 360.0  # AuthInfo=ttl=300
    try:
        assert clean_up_run(record) == []
    finally:
        SlurmScheduler.lost_reply_deadline = known
    entries = json.loads(registry.path.read_text())["attempts"]
    assert entries["0"]["job_id"] == job_id, entries
    assert squeue_state(job_id) == "CANCELLED", squeue_state(job_id)
    assert entries["1"]["state"] == "ended" and entries["1"]["job_id"] is None
    report["lost_replies_settled"] = {k: (e["state"], e["job_id"]) for k, e in entries.items()}


AS_USER = """
import json, subprocess, sys
from adagio.execution.backends.batch import JobRef, make_command_runner
from adagio.execution.backends.slurm import SlurmScheduler

name = sys.argv[1]
job_id = subprocess.run(
    ["sbatch", "--parsable", "--partition=adagio-hidden", "--mem=32M",
     f"--job-name={name}", "--output=/dev/null", "--wrap=sleep 300"],
    capture_output=True, text=True, check=True,
).stdout.strip()
scheduler = SlurmScheduler()
run = make_command_runner(SlurmScheduler)
status = scheduler.statuses([JobRef(name, job_id)], run)[JobRef(name, job_id)]
subprocess.run(["scancel", f"--name={name}", job_id], check=True)
print(json.dumps({"job_id": job_id, "active": status.active, "detail": status.detail}))
"""


def case_hidden_partition_as_a_user():
    """A normal user's job in a hidden partition is still seen, not taken as gone."""
    subprocess.run(
        ["scontrol", "create", "PartitionName=adagio-hidden", "Nodes=ALL", "Hidden=YES"],
        capture_output=True,
        check=False,  # already there from an earlier run
    )
    try:
        result = subprocess.run(
            [
                "runuser", "-u", "ubuntu", "--", "env", "PYTHONPATH=/workspace/src",
                sys.executable, "-c", AS_USER, f"adagio-{uuid.uuid4().hex}",
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd="/tmp",
        )
    finally:
        subprocess.run(
            ["scontrol", "delete", "PartitionName=adagio-hidden"], check=False
        )
    seen = json.loads(result.stdout)
    assert seen["active"] is True, seen
    report["hidden_partition"] = seen


def registry_after(registry):
    path = Path(registry["run_dir"]) / "submissions.json"
    return list(json.loads(path.read_text())["attempts"].values())


class Quiet:
    def started(self, item, handle):
        pass

    def succeeded(self, item, outcome):
        pass

    def stopped(self, error, **details):
        pass


def drive(record, exit_with_stdin):
    """Run two long jobs through the coordinator, as ``adagio runtime`` does."""
    if exit_with_stdin:
        from adagio.cli.runtime import _terminate_when_stdin_closes

        _terminate_when_stdin_closes()
    b = backend("driven", run_record=Path(record))

    def program(workspace, node):
        def start():
            return (yield invocation(b, workspace, node, sleep=300))

        return start

    with b.workspace() as workspace:
        coordinate(
            items=[
                WorkItem(f"long-{i}", frozenset(), RESOURCES, program(workspace, f"long-{i}"))
                for i in range(2)
            ],
            backend=b,
            listener=Quiet(),
        )


if __name__ == "__main__":
    if sys.argv[1:2] == ["--driver"]:
        try:
            drive(sys.argv[2], "--exit-with-stdin" in sys.argv)
        except KeyboardInterrupt:
            sys.exit(3)
        sys.exit(0)
    case_results()
    case_rejected()
    case_cancel()
    case_unknown_state_is_still_cancelled()
    case_outage_is_waited_out()
    case_cleanup_after_kill()
    case_live_owner_then_stdin_closes()
    case_reused_id_spares_the_other_job()
    case_lost_replies_without_a_known_lifetime()
    case_lost_replies_with_a_known_lifetime()
    case_hidden_partition_as_a_user()
    (root / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
