"""Batch backend guarantees, exercised through the Slurm adapter with scripted commands."""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from adagio.cli.config import AdagioRunConfig, load_run_config
from adagio.execution.backends import clean_up_run
from adagio.execution.backends.base import JobState, TaskInvocation
from adagio.execution.backends.batch import BatchBackend
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


class Commands:
    """Stand-in for ``subprocess.run`` that replays scheduler output in order."""

    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        return subprocess.CompletedProcess(argv, 0, response, "")


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
        ["123;lab", "123|PENDING", "", "123.batch|FAILED|1:0\n123|COMPLETED|0:0"]
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
        Commands(["123", "123|COMPLETED", accounting, "", "123|COMPLETED|0:0"]),
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
        Commands(["123"] + ["123|PENDING"] * 6 + ["123|RUNNING", "123|RUNNING"]),
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
    with pytest.raises(RuntimeError):
        with backend.workspace() as failed:
            assert json.loads(record.read_text())["registry"] == str(
                backend.registry.path
            )
            raise RuntimeError("task failed")
    assert failed.root.is_dir() and record.exists()

    with backend.workspace() as succeeded:
        pass
    assert not succeeded.root.exists() and not record.exists()


def test_cancel_touches_only_recorded_jobs(tmp_path):
    registry = SubmissionRegistry.create(tmp_path, executor="slurm")
    for attempt, job_id, name in [
        ("a", "123", "adagio-" + "a" * 32),
        ("b", "999", "adagio-" + "b" * 32),
        ("c", None, "adagio-" + "c" * 32),
    ]:
        registry.record_intent(attempt, job_name=name)
        if job_id:
            registry.record_submitted(attempt, job_id=job_id, cluster=None)
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
            f"456|adagio-{'c' * 32}",  # squeue --name for the unconfirmed submission
            "",  # scancel
            "",  # squeue
            "123|CANCELLED|0:15\n456|CANCELLED|0:15",  # sacct
        ]
    )
    assert clean_up_run(record, command_runner=commands) == []
    assert commands.calls[1] == ["scancel", "123", "456"]
    assert "999" not in str(commands.calls)
    assert not record.exists()
    states = {
        attempt: entry["state"]
        for attempt, entry in json.loads(registry.path.read_text())["attempts"].items()
    }
    assert states == {"a": "failed", "b": "succeeded", "c": "failed"}


def test_a_rejected_submission_leaves_nothing_to_cancel(tmp_path):
    commands = Commands(
        [
            subprocess.CalledProcessError(1, "sbatch"),  # sbatch rejects the job
            "",  # squeue --name finds nothing under the job's unique name
        ]
    )
    backend, workspace = batch_backend(tmp_path, commands)
    with pytest.raises(RuntimeError, match="not retried"):
        submit(backend, workspace)
    assert backend.cancel() == []
    (entry,) = json.loads(backend.registry.path.read_text())["attempts"].values()
    assert entry["scheduler_state"] == "NOT_QUEUED" and entry["state"] == "failed"


def test_cleanup_reports_what_it_cannot_confirm(tmp_path):
    registry = SubmissionRegistry.create(tmp_path, executor="slurm")
    registry.record_intent("a", job_name="adagio-" + "a" * 32)
    registry.record_submitted("a", job_id="123", cluster="lab")
    record = tmp_path / "run-record.json"
    write_json_file(
        record, {"version": 1, "executor": "slurm", "registry": str(registry.path)}
    )
    now = [0.0]

    def commands(argv, **kwargs):
        now[0] += 6
        if argv[0] == "scancel":
            return subprocess.CompletedProcess(argv, 1, "", "Invalid job id")
        return subprocess.CompletedProcess(argv, 0, "123|RUNNING", "")

    from adagio.execution.backends import batch

    errors = batch.cancel_outstanding(
        SubmissionRegistry.load(registry.path),
        SlurmScheduler(),
        batch.make_command_runner(SlurmScheduler, runner=commands),
        clock=lambda: now[0],
        sleep=lambda _: None,
    )
    assert any("Invalid job id" in error for error in errors)
    assert any("not confirmed for Slurm jobs 123" in error for error in errors)
    assert clean_up_run(tmp_path / "absent.json") == []


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
