"""Batch backend guarantees, exercised through the Slurm adapter with scripted commands."""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from adagio.cli.config import AdagioRunConfig, load_run_config
from adagio.execution.backends import batch, clean_up_run
from adagio.execution.backends.base import JobState, TaskInvocation
from adagio.execution.backends.batch import BatchBackend, JobRef
from adagio.execution.backends.run_record import RunOwned, owning_run
from adagio.execution.backends.slurm import (
    SlurmExecutorConfig,
    SlurmScheduler,
    memory_megabytes,
)
from adagio.execution.backends.submissions import SubmissionRegistry
from adagio.execution.resources import ConfiguredResourcePolicy, TaskResourceRequirements
from adagio.executors.base import TaskExecutionRequest
from adagio.executors.prepared import PreparedInvocation
from adagio.executors.task_contract import write_json_file


A = "adagio-" + "a" * 32
B = "adagio-" + "b" * 32
C = "adagio-" + "c" * 32


class Commands:
    """Stand-in for ``subprocess.run`` that replays scheduler output in order.

    ``NAME`` in a response stands for the job name the command asked about.
    Once the responses run out, ``default`` answers every further command.
    """

    def __init__(self, responses, default=None):
        self.responses = iter(responses)
        self.default = default
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        response = next(self.responses, self.default)
        if response is None:
            raise AssertionError(f"Unexpected command: {argv}")
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, tuple):  # (returncode, stdout, stderr)
            return subprocess.CompletedProcess(argv, *response)
        names = [a.removeprefix("--name=") for a in argv if a.startswith("--name=")]
        if names:
            response = response.replace("NAME", names[0])
        return subprocess.CompletedProcess(argv, 0, response, "")


class Clock:
    """Monotonic time that advances only when the code under test sleeps."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def registry_with(tmp_path, *entries):
    """A registry holding ``(attempt, name, job_id)`` submissions."""
    registry = SubmissionRegistry.create(tmp_path, executor="slurm")
    for attempt, name, job_id in entries:
        registry.record_intent(attempt, job_name=name)
        if job_id:
            registry.record_submitted(attempt, job_id=job_id, cluster=None)
    registry.save()
    return registry


def cancel(registry, commands, **kwargs):
    return batch.cancel_outstanding(
        registry,
        SlurmScheduler(),
        batch.make_command_runner(SlurmScheduler, runner=commands),
        sleep=kwargs.pop("sleep", lambda _: None),
        **kwargs,
    )


def attempts(registry):
    return json.loads(registry.path.read_text())["attempts"]


class Launcher:
    def __init__(self, prepared):
        self.prepared = prepared

    def prepare(self, *, environment, request, shared):
        assert shared
        return self.prepared


def prepared_invocation(work: Path) -> PreparedInvocation:
    work.mkdir(parents=True, exist_ok=True)
    staged = work / ".adagio-container-python/adagio/cli/task_exec.py"
    staged.parent.mkdir(parents=True)
    staged.write_text("")
    environment = work.parent / "env"
    environment.mkdir(exist_ok=True)
    spec = work / "spec.json"
    spec.write_text("{}")
    request = TaskExecutionRequest(
        task=SimpleNamespace(id="node"),
        cwd=work.parent,
        work_path=work,
        archive_inputs={},
        archive_collection_inputs={},
        metadata_inputs={},
        params={},
        metadata_column_kwargs={},
        outputs={"out": str(work / "output")},
    )
    return PreparedInvocation(
        command=["/bin/echo", "a; $(unsafe)"],
        env={"RUNTIME_TOKEN": "never-export"},
        cwd=work.parent,
        spec_path=spec,
        manifest_path=work / "result.json",
        log_path=work / "log",
        request=request,
        image_ref=str(environment),
    )


def batch_backend(tmp_path, commands, **kwargs):
    backend = BatchBackend(
        config=SlurmExecutorConfig(work_dir=str(tmp_path / "work")),
        scheduler=SlurmScheduler(),
        command_runner=commands,
        sleep=kwargs.pop("sleep", lambda _: None),
        **kwargs,
    )
    workspace = backend.workspace().__enter__()
    return backend, workspace


def submit(backend, workspace, resources=None):
    prepared = prepared_invocation(backend.attempt_directory(workspace))
    handle = backend.submit(
        TaskInvocation(
            launcher=Launcher(prepared),
            environment=SimpleNamespace(kind="apptainer"),
            request=prepared.request,
            task_id="node",
        ),
        resources or TaskResourceRequirements(cpus=1),
    )
    return handle, prepared


@pytest.mark.parametrize(
    "value, expected",
    [("1 B", 1), ("1 GB", 954), ("1 GiB", 1024), ("1.1 MiB", 2), ("512 KB", 1)],
)
def test_memory_rounds_up_total_allocation(value, expected):
    assert memory_megabytes(value) == expected


def test_submission_and_accounting_allocation_not_steps(tmp_path):
    commands = Commands(
        ["123;lab", "123|NAME|PENDING", "", "123.batch|FAILED|1:0\n123|COMPLETED|0:0"]
    )
    backend, workspace = batch_backend(tmp_path, commands)
    handle, prepared = submit(
        backend, workspace, TaskResourceRequirements(cpus=7, memory="1 GB")
    )
    assert (handle.job_id, handle.job.cluster) == ("123", "lab")
    sbatch = commands.calls[0]
    assert "--cpus-per-task=7" in sbatch and "--mem=954M" in sbatch
    assert "--no-requeue" in sbatch and "--export=NONE" in sbatch
    script = (prepared.request.work_path / "job.sh").read_text()
    assert "never-export" not in script and "'a; $(unsafe)'" in script
    assert json.loads(prepared.spec_path.read_text())["attempt_id"] == prepared.attempt_id

    backend.wait([handle])
    assert handle.state is JobState.QUEUED
    # The queue is asked by our name only, in every partition including
    # hidden ones, and including finished jobs.
    squeue = commands.calls[1]
    assert f"--name={handle.job.name}" in squeue and "--states=all" in squeue
    assert "--all" in squeue
    assert "--clusters=lab" in squeue and not any("--jobs" in a for a in squeue)
    backend.wait([handle])
    assert handle.state is JobState.SUCCEEDED

    Path(prepared.request.outputs["out"]).write_text("scientific output")
    write_json_file(
        prepared.manifest_path,
        {"outputs": dict(prepared.request.outputs), "attempt_id": prepared.attempt_id},
    )
    assert backend.collect(handle).outputs["out"] == prepared.request.outputs["out"]


def test_disappearance_is_not_success(tmp_path):
    now = [0]
    backend, workspace = batch_backend(
        tmp_path,
        Commands(["123", "", "", "", ""]),
        clock=lambda: now[0],
        accounting_timeout=5,
    )
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    assert handle.state is JobState.QUEUED and handle.unknown_since == 0
    now[0] = 6
    backend.wait([handle])
    assert handle.state is JobState.FAILED
    with pytest.raises(RuntimeError, match="state is unknown"):
        backend.collect(handle)


def test_unconfirmed_submission_is_recorded_and_never_retried(tmp_path):
    commands = Commands([subprocess.TimeoutExpired("sbatch", 30)])
    backend, workspace = batch_backend(tmp_path, commands)
    with pytest.raises(RuntimeError, match="not retried"):
        submit(backend, workspace)
    assert len(commands.calls) == 1
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["state"] == "unconfirmed" and entry["job_id"] is None
    with pytest.raises(RuntimeError, match="stopped"):
        submit(backend, workspace)


@pytest.mark.parametrize("accounting", ["", "123|COMPLETED|", "123||0:0"])
def test_terminal_queue_state_waits_for_accounting_exit_code(tmp_path, accounting):
    backend, workspace = batch_backend(
        tmp_path,
        Commands(["123", "123|NAME|COMPLETED", accounting, "", "123|COMPLETED|0:0"]),
    )
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    assert handle.state is JobState.QUEUED and handle.unknown_since is not None
    backend.wait([handle])
    assert handle.state is JobState.SUCCEEDED and handle.exit_code == "0:0"
    assert handle.unknown_since is None


@pytest.mark.parametrize(
    "state,code",
    [
        ("OUT_OF_MEMORY", "0:125"),
        ("TIMEOUT", "0:0"),
        ("CANCELLED", "0:15"),
        ("FAILED", "1:0"),
        ("COMPLETED", "1:0"),
    ],
)
def test_scheduler_failure_is_not_worker_success(tmp_path, state, code):
    backend, workspace = batch_backend(tmp_path, Commands(["123", "", f"123|{state}|{code}"]))
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    assert handle.state is JobState.FAILED
    with pytest.raises(RuntimeError, match=state):
        backend.collect(handle)


def test_polling_backs_off_until_something_changes(tmp_path):
    sleeps = []
    backend, workspace = batch_backend(
        tmp_path,
        Commands(["123"] + ["123|NAME|PENDING"] * 6 + ["123|NAME|RUNNING"] * 2),
        sleep=sleeps.append,
    )
    handle, _ = submit(backend, workspace)
    for _ in range(8):
        backend.wait([handle])
    # The first poll records PENDING, a change; then nothing moves until RUNNING.
    assert sleeps == [2.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 2.0]


def test_missing_shared_paths_fail_before_submission(tmp_path):
    commands = Commands([])
    backend, workspace = batch_backend(tmp_path, commands)
    prepared = prepared_invocation(backend.attempt_directory(workspace))
    prepared.request = TaskExecutionRequest(
        **{**prepared.request.__dict__, "archive_inputs": {"table": "/missing/table.qza"}}
    )
    with pytest.raises(ValueError, match="/missing/table.qza"):
        backend.submit(
            TaskInvocation(
                launcher=Launcher(prepared),
                environment=SimpleNamespace(kind="conda"),
                request=prepared.request,
            ),
            TaskResourceRequirements(cpus=1),
        )
    assert commands.calls == []


def test_only_shared_environments_are_placed(tmp_path):
    backend, _ = batch_backend(tmp_path, Commands([]))
    for kind in ("apptainer", "conda"):
        assert backend.unsupported_reason(SimpleNamespace(kind=kind)) is None
    assert backend.unsupported_reason(SimpleNamespace(kind="docker")) == (
        "Slurm supports Apptainer or shared Conda environments; Docker is unsupported."
    )


def test_workspace_is_kept_after_failure_and_removed_after_success(tmp_path):
    record = tmp_path / "run-record.json"
    backend = BatchBackend(
        config=SlurmExecutorConfig(work_dir=str(tmp_path / "work")),
        scheduler=SlurmScheduler(),
        run_record=record,
    )
    lock = tmp_path / "run-record.json.lock"
    with pytest.raises(RuntimeError):
        with backend.workspace() as failed:
            assert json.loads(record.read_text())["registry"] == str(
                backend.registry.path
            )
            # The run's own process owns it: cleanup must not act meanwhile.
            with pytest.raises(RunOwned):
                clean_up_run(record)
            raise RuntimeError("task failed")
    assert failed.root.is_dir() and record.exists() and lock.exists()
    # Once the owner is gone, cleanup owns the run; nothing was submitted.
    assert clean_up_run(record) == []
    assert not record.exists() and not lock.exists()

    with backend.workspace() as succeeded:
        pass
    assert not succeeded.root.exists() and not record.exists() and not lock.exists()


def test_a_run_record_has_one_owner(tmp_path):
    record = tmp_path / "run-record.json"
    backend = BatchBackend(
        config=SlurmExecutorConfig(work_dir=str(tmp_path / "work")),
        scheduler=SlurmScheduler(),
        run_record=record,
    )
    with owning_run(record):
        with pytest.raises(RunOwned, match="pid"):
            backend.workspace().__enter__()
    assert not (tmp_path / "work").exists() and not record.exists()


def test_a_filesystem_that_ignores_locks_is_refused(tmp_path, monkeypatch):
    import fcntl

    # As on network and container bind mounts: every lock is granted.
    monkeypatch.setattr(fcntl, "flock", lambda descriptor, operation: None)
    with pytest.raises(RuntimeError, match="does not enforce file locks"):
        with owning_run(tmp_path / "run-record.json"):
            pass


def test_cancel_touches_only_recorded_jobs_by_name(tmp_path):
    registry = registry_with(tmp_path, ("a", A, "123"), ("b", B, "999"), ("c", C, None))
    registry.record_status(
        "b", state=JobState.SUCCEEDED, scheduler_state="COMPLETED", exit_code="0:0"
    )
    registry.record_unconfirmed("c", "timed out")
    registry.save()
    record = tmp_path / "run-record.json"
    write_json_file(
        record, {"version": 1, "executor": "slurm", "registry": str(registry.path)}
    )
    commands = Commands(
        [
            "",  # scancel a
            "",  # scancel c, whose reply was lost: by name alone
            f"123|{A}|CANCELLED\n456|{C}|CANCELLED",  # squeue
            "123|CANCELLED|0:15\n456|CANCELLED|0:15",  # sacct
        ]
    )
    assert clean_up_run(record, command_runner=commands) == []
    assert commands.calls[:2] == [
        ["scancel", f"--name={A}", "123"],
        ["scancel", f"--name={C}"],
    ]
    assert "999" not in str(commands.calls) and B not in str(commands.calls)
    assert not record.exists()
    entries = attempts(registry)
    assert {a: e["state"] for a, e in entries.items()} == {
        "a": "failed",
        "b": "succeeded",
        "c": "failed",
    }
    assert entries["c"]["job_id"] == "456"


def test_a_reused_job_id_is_never_taken_for_ours(tmp_path):
    registry = registry_with(tmp_path, ("a", A, "123"))
    # Our job ended long ago; Slurm reused 123 for someone else's job.
    commands = Commands(["", f"123|{B}|RUNNING", ""])
    assert cancel(registry, commands) == []
    assert commands.calls[0] == ["scancel", f"--name={A}", "123"]
    (entry,) = attempts(registry).values()
    assert entry["state"] == "ended"
    assert entry["scheduler_state"] == "not in the queue; no accounting record"


def test_a_lost_reply_is_settled_once_no_job_can_appear(tmp_path):
    registry = registry_with(tmp_path, ("c", C, None))
    registry.record_unconfirmed("c", "timed out")
    registry.save()
    intent = attempts(registry)["c"]["intent_at"]
    clock = Clock()
    commands = Commands([], default="")
    errors = cancel(
        registry, commands, clock=clock, sleep=clock.sleep, now=lambda: intent + clock.now
    )
    assert errors == []
    # Slurm acts on a delivered request until its credential expires.
    assert SlurmScheduler.lost_reply_deadline >= 300
    assert SlurmScheduler.lost_reply_deadline <= clock.now < 366
    # Waiting backs off rather than asking the scheduler twice a second.
    assert len(commands.calls) < 100
    (entry,) = attempts(registry).values()
    assert entry["state"] == "ended" and entry["job_id"] is None


def test_a_lost_reply_that_becomes_a_job_is_cancelled_by_name(tmp_path):
    registry = registry_with(tmp_path, ("c", C, None))
    intent = attempts(registry)["c"]["intent_at"]
    clock = Clock()
    commands = Commands(
        [
            "",  # scancel: nothing has that name yet
            "",  # squeue: still nothing
            f"456|{C}|PENDING",  # squeue: the job appears
            "",  # scancel it again, by name
            f"456|{C}|CANCELLED",  # squeue
            "456|CANCELLED|0:15",  # sacct
        ]
    )
    errors = cancel(
        registry, commands, clock=clock, sleep=clock.sleep, now=lambda: intent + 1
    )
    assert errors == []
    assert commands.calls[3] == ["scancel", f"--name={C}"]
    (entry,) = attempts(registry).values()
    assert (entry["state"], entry["job_id"]) == ("failed", "456")


def test_a_job_whose_state_is_unknown_is_still_cancelled(tmp_path):
    now = [0]
    commands = Commands(
        [
            "123",  # sbatch
            "",  # squeue: not queued
            "",  # sacct: no record yet
            "",  # squeue
            "",  # sacct: still nothing after the accounting timeout
            "",  # scancel: the job may still be alive, so it is cancelled
            "123|NAME|CANCELLED",  # squeue
            "123|CANCELLED|0:15",  # sacct
        ]
    )
    backend, workspace = batch_backend(
        tmp_path, commands, clock=lambda: now[0], accounting_timeout=5
    )
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    now[0] = 6
    backend.wait([handle])
    assert handle.state is JobState.FAILED
    assert backend.cancel() == []
    assert ["scancel", f"--name={handle.job.name}", "123"] in commands.calls
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["scheduler_state"] == "CANCELLED"


def test_a_scheduler_outage_is_waited_out(tmp_path):
    now = [0]
    down = (1, "", "slurm_load_jobs error: Unable to contact slurm controller")
    commands = Commands(["123"], default=down)
    backend, workspace = batch_backend(
        tmp_path,
        commands,
        clock=lambda: now[0],
        accounting_timeout=5,
        outage_timeout=600,
    )
    handle, _ = submit(backend, workspace)
    for now[0] in (0, 300, 599):
        backend.wait([handle])
        assert handle.state is JobState.QUEUED
    assert handle.unknown_since is None
    now[0] = 600
    backend.wait([handle])
    assert handle.state is JobState.FAILED
    with pytest.raises(RuntimeError, match="could not be asked for 600s"):
        backend.collect(handle)


def test_accounting_that_cannot_be_asked_is_an_outage_not_a_loss(tmp_path):
    now = [0]
    commands = Commands(
        ["123"], default=(1, "", "sacct: error: slurmdbd: Connection refused")
    )
    backend, workspace = batch_backend(
        tmp_path, commands, clock=lambda: now[0], accounting_timeout=5
    )
    handle, _ = submit(backend, workspace)
    # squeue answers (the job left the queue) but accounting cannot be asked.
    commands.responses = iter(["", commands.default, "", commands.default])
    backend.wait([handle])
    now[0] = 60
    backend.wait([handle])
    assert handle.state is JobState.QUEUED and handle.unknown_since is None


def test_an_unconfirmed_submission_stays_uncertain_within_its_deadline(tmp_path):
    clock = Clock()
    commands = Commands([subprocess.TimeoutExpired("sbatch", 30)], default="")
    backend, workspace = batch_backend(
        tmp_path, commands, clock=clock, sleep=clock.sleep
    )
    with pytest.raises(RuntimeError, match="not retried"):
        submit(backend, workspace)
    (error,) = backend.cancel()
    assert "not confirmed" in error and "id unknown" in error
    assert commands.calls[1] == ["scancel", f"--name={backend_job_name(backend)}"]
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["state"] == "unconfirmed"


def backend_job_name(backend):
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    return entry["job_name"]


def test_a_rejected_submission_is_reported_plainly_and_leaves_nothing(tmp_path):
    rejection = (
        "sbatch: error: invalid partition specified: nope\n"
        "sbatch: error: Batch job submission failed: Invalid partition name specified"
    )
    commands = Commands([(1, "", rejection)])
    backend, workspace = batch_backend(tmp_path, commands)
    with pytest.raises(RuntimeError, match="Slurm rejected") as raised:
        submit(backend, workspace)
    assert "Invalid partition name specified" in str(raised.value)
    assert backend.cancel() == []
    assert len(commands.calls) == 1
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["state"] == "rejected"


@pytest.mark.parametrize(
    "stderr",
    [
        "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation",
        "sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)",
        "sbatch: error: Batch job submission failed: Zero Bytes were transmitted or received",
        "sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying",
        # Failures reading or verifying the controller's reply: the job may exist.
        "sbatch: error: Batch job submission failed: Message receive failure",
        "sbatch: error: Batch job submission failed: Header lengths are longer than data received",
        "sbatch: error: Batch job submission failed: Unexpected message received",
        "sbatch: error: Batch job submission failed: Insane message length",
        "sbatch: error: Batch job submission failed: Protocol authentication error",
        "sbatch: error: Batch job submission failed: Invalid authentication credential",
        "sbatch: error: Batch job submission failed: Interrupted system call",
        # A refusal and a lost reply together are still uncertain.
        "sbatch: error: Batch job submission failed: Invalid qos specification\n"
        "sbatch: error: Batch job submission failed: Message receive failure",
        "sbatch: error: some local problem",
    ],
)
def test_anything_but_an_explicit_refusal_stays_unconfirmed(tmp_path, stderr):
    clock = Clock()
    commands = Commands([(1, "", stderr)], default="")
    backend, workspace = batch_backend(
        tmp_path, commands, clock=clock, sleep=clock.sleep
    )
    with pytest.raises(RuntimeError, match="not retried"):
        submit(backend, workspace)
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["state"] == "unconfirmed"
    # Cancellation names the job, and keeps reporting it until it can be sure.
    (error,) = backend.cancel()
    assert commands.calls[1] == ["scancel", f"--name={entry['job_name']}"]
    assert "not confirmed" in error


@pytest.mark.parametrize(
    "reason",
    [
        "Invalid partition name specified",
        "Invalid account or account/partition combination specified",
        "Invalid qos specification",
        "Requested time limit is invalid (missing or exceeds some limit)",
        "Requested node configuration is not available",
        "Memory required by task is not available",
        "Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)",
        "Access/permission denied",
    ],
)
def test_explicit_refusals_are_rejections(tmp_path, reason):
    stderr = f"sbatch: error: Batch job submission failed: {reason}"
    backend, workspace = batch_backend(tmp_path, Commands([(1, "", stderr)]))
    with pytest.raises(RuntimeError, match="Slurm rejected"):
        submit(backend, workspace)
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["state"] == "rejected"


def test_accounting_is_asked_even_when_the_queue_lookup_fails(tmp_path):
    invalid = (1, "", "slurm_load_jobs error: Socket timed out on send/recv")
    commands = Commands(["123", invalid, "123|COMPLETED|0:0"])
    backend, workspace = batch_backend(tmp_path, commands)
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    assert handle.state is JobState.SUCCEEDED
    assert commands.calls[2][0] == "sacct"
    # Only our job: an older job that reused the id has another name.
    assert f"--name={handle.job.name}" in commands.calls[2]


def test_a_failed_scancel_is_harmless_once_the_end_is_confirmed(tmp_path):
    registry = registry_with(tmp_path, ("a", A, "123"))
    commands = Commands(
        [
            (1, "", "scancel: error: Kill job error on job id 123: Job/step already completed"),
            "",  # squeue
            "123|COMPLETED|0:0",  # sacct confirms the job ended
        ]
    )
    assert cancel(registry, commands) == []


def test_cleanup_reports_what_it_cannot_confirm(tmp_path):
    registry = registry_with(tmp_path, ("a", A, "123"))
    now = [0.0]

    def commands(argv, **kwargs):
        now[0] += 6
        if argv[0] == "scancel":
            return subprocess.CompletedProcess(argv, 1, "", "Invalid job id")
        return subprocess.CompletedProcess(argv, 0, f"123|{A}|RUNNING", "")

    errors = cancel(SubmissionRegistry.load(registry.path), commands, clock=lambda: now[0])
    assert any("Invalid job id" in error for error in errors)
    assert any(f"not confirmed for Slurm jobs {A} (123)" in error for error in errors)
    assert clean_up_run(tmp_path / "absent.json") == []


def test_a_job_the_queue_still_holds_is_never_confirmed_gone(tmp_path):
    # STOPPED keeps its CPUs; a state this code does not know may as well.
    registry = registry_with(tmp_path, ("a", A, "1"), ("b", B, "2"))
    clock = Clock()
    commands = Commands([], default=f"1|{A}|STOPPED\n2|{B}|SOME_FUTURE_STATE")
    (error,) = cancel(registry, commands, clock=clock, sleep=clock.sleep)
    assert "not confirmed" in error and A in error and B in error
    # Cancellation was sent again while the jobs stayed in the queue.
    scancels = [argv for argv in commands.calls if argv[0] == "scancel"]
    assert len(scancels) > 2
    assert {e["state"] for e in attempts(registry).values()} == {"queued"}


def test_a_listed_state_that_is_not_an_end_keeps_a_task_waiting(tmp_path):
    backend, workspace = batch_backend(
        tmp_path, Commands(["123", "123|NAME|SOME_FUTURE_STATE"])
    )
    handle, _ = submit(backend, workspace)
    backend.wait([handle])
    assert handle.state is JobState.QUEUED and handle.unknown_since is None


def test_the_detail_says_what_accounting_shows(tmp_path):
    now = [0]
    commands = Commands(["123"], default="")
    backend, workspace = batch_backend(
        tmp_path, commands, clock=lambda: now[0], accounting_timeout=5
    )
    handle, _ = submit(backend, workspace)
    commands.responses = iter(["", "123|RUNNING|0:0", "", "123|RUNNING|0:0"])
    backend.wait([handle])
    now[0] = 6
    backend.wait([handle])
    with pytest.raises(RuntimeError, match="accounting shows RUNNING, exit code 0:0"):
        backend.collect(handle)


def test_an_outage_restarts_the_grace_for_a_missing_job(tmp_path):
    now = [0]
    down = (1, "", "slurm_load_jobs error: Unable to contact slurm controller")
    commands = Commands(["123"], default="")
    backend, workspace = batch_backend(
        tmp_path, commands, clock=lambda: now[0], accounting_timeout=5
    )
    handle, _ = submit(backend, workspace)
    commands.responses = iter(["", "", down, down, "", ""])
    backend.wait([handle])  # gone, no outcome yet: the grace starts
    now[0] = 3
    backend.wait([handle])  # an outage: says nothing about the job
    now[0] = 60
    backend.wait([handle])  # answered again: a fresh grace, not a failure
    assert handle.state is JobState.QUEUED and handle.unknown_since == 60


def test_scheduler_filters_in_the_environment_are_dropped(monkeypatch):
    for name in ("SBATCH_PARTITION", "SQUEUE_PARTITION", "SACCT_FEDERATION", "SCANCEL_STATE"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("SLURM_CONF", "/etc/slurm/slurm.conf")
    seen = {}

    def runner(argv, **kwargs):
        seen.update(kwargs["env"])
        return subprocess.CompletedProcess(argv, 0, "", "")

    batch.make_command_runner(SlurmScheduler, runner=runner)(["squeue"])
    assert not any(k.startswith(("SBATCH_", "SQUEUE_", "SACCT_", "SCANCEL_")) for k in seen)
    assert seen["SLURM_CONF"] == "/etc/slurm/slurm.conf"


def test_a_run_never_replaces_an_unsettled_record(tmp_path):
    record = tmp_path / "run-record.json"
    record.write_text('{"earlier": "run"}')
    backend = BatchBackend(
        config=SlurmExecutorConfig(work_dir=str(tmp_path / "work")),
        scheduler=SlurmScheduler(),
        run_record=record,
    )
    with pytest.raises(RuntimeError, match="adagio cleanup"):
        backend.workspace().__enter__()
    assert record.read_text() == '{"earlier": "run"}'


def test_a_successful_run_drops_its_record_before_its_directory(tmp_path, monkeypatch):
    record = tmp_path / "run-record.json"
    backend = BatchBackend(
        config=SlurmExecutorConfig(work_dir=str(tmp_path / "work")),
        scheduler=SlurmScheduler(),
        run_record=record,
    )

    def interrupted(path, ignore_errors=False):
        assert not record.exists()
        raise KeyboardInterrupt

    monkeypatch.setattr(batch.shutil, "rmtree", interrupted)
    with pytest.raises(KeyboardInterrupt):
        with backend.workspace():
            pass
    assert not record.exists()


def test_cancel_commands_name_every_job_and_refuse_foreign_names():
    commands = SlurmScheduler().cancel_commands(
        [JobRef(A, "1", "lab"), JobRef(B), JobRef(C, "3")]
    )
    assert commands == [
        ["scancel", "--clusters=lab", f"--name={A}", "1"],
        ["scancel", f"--name={B}"],
        ["scancel", f"--name={C}", "3"],
    ]
    with pytest.raises(ValueError, match="Not an Adagio job name"):
        SlurmScheduler().cancel_commands([JobRef(f"{A},{B}", "1")])


def test_results_must_be_current_complete_and_present(tmp_path):
    prepared = prepared_invocation(tmp_path / "attempt")
    with pytest.raises(RuntimeError, match="did not write"):
        prepared.collect()
    write_json_file(prepared.manifest_path, {"outputs": {}, "attempt_id": "stale"})
    with pytest.raises(RuntimeError, match="stale"):
        prepared.collect()
    write_json_file(
        prepared.manifest_path,
        {"outputs": dict(prepared.request.outputs), "attempt_id": prepared.attempt_id},
    )
    with pytest.raises(RuntimeError, match="missing"):
        prepared.collect()


@pytest.mark.parametrize(
    "payload", [[], {"outputs": []}, {"outputs": {}, "reused": "false"}, "broken"]
)
def test_malformed_results_are_rejected(tmp_path, payload):
    prepared = prepared_invocation(tmp_path / "attempt")
    prepared.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(RuntimeError, match="malformed"):
        prepared.collect()


@pytest.mark.parametrize(
    "argument",
    [
        "--mem=5G",
        "-c8",
        "--wrap=ls",
        "--dependency=after:1",
        "--array=1-2",
        "--out=x",
        "--requeue",
        "--export=ALL",
    ],
)
def test_managed_and_abbreviated_options_rejected(argument, tmp_path):
    with pytest.raises(ValueError):
        SlurmExecutorConfig(work_dir=str(tmp_path), slurm={"extra_args": [argument]})


def test_batch_requires_an_absolute_shared_work_directory():
    for work_dir in ("relative/work", "/shared/\nwork"):
        with pytest.raises(ValueError):
            SlurmExecutorConfig(work_dir=work_dir)
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"executor": {"kind": "slurm"}})


def test_resource_fields_resolve_independently():
    config = AdagioRunConfig.model_validate(
        {
            "resources": {
                "defaults": {"cpus": 2, "memory": "3 GB"},
                "tasks": {"a": {"cpus": 7}},
            }
        }
    )
    policy = ConfiguredResourcePolicy(config.resources)
    assert policy.for_task(SimpleNamespace(id="a")).model_dump() == {
        "cpus": 7,
        "memory": "3 GB",
    }
    assert policy.for_task(SimpleNamespace(id="b")).cpus == 2
    assert ConfiguredResourcePolicy().for_task(SimpleNamespace(id="b")).cpus == 1
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"executor": {"kind": "typo"}})
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"version": 2})
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"exector": {"kind": "slurm"}})


def test_shared_json_and_toml_fixture_and_strict_version():
    fixture = Path(__file__).parent / "fixtures/slurm-run-v1"
    left = load_run_config(fixture.with_suffix(".json"))
    assert left == load_run_config(fixture.with_suffix(".toml"))
    assert left.executor.kind == "slurm"
    policy = ConfiguredResourcePolicy(left.resources)
    assert policy.for_task(SimpleNamespace(id="node.with.dots")).model_dump() == {
        "cpus": 7,
        "memory": "4 GiB",
    }
    assert policy.for_task(SimpleNamespace(id="memory-only")).model_dump() == {
        "cpus": 2,
        "memory": "1 GB",
    }
    for value in [True, 1.0, "1", 2]:
        with pytest.raises(ValueError):
            AdagioRunConfig(version=value)
