import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from adagio.cli.config import AdagioRunConfig, ExecutorConfig, TaskResourceRequirements
from adagio.execution.slurm import SlurmBackend, memory_megabytes
from adagio.executors.base import TaskExecutionRequest
from adagio.executors.prepared import PreparedInvocation
from adagio.executors.task_contract import write_json_file


def invocation(tmp_path):
    work = tmp_path / "attempt"
    work.mkdir()
    source = work / ".adagio-container-python/adagio/cli/task_exec.py"
    source.parent.mkdir(parents=True)
    source.write_text("")
    env = tmp_path / "env"
    env.mkdir()
    spec = work / "spec.json"
    spec.write_text("{}")
    request = TaskExecutionRequest(
        task=SimpleNamespace(id="node"),
        cwd=tmp_path,
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
        cwd=tmp_path,
        spec_path=spec,
        manifest_path=work / "result.json",
        log_path=work / "log",
        request=request,
        image_ref=str(env),
    )


class Commands:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return subprocess.CompletedProcess(argv, 0, response, "")


def backend(tmp_path, commands, **kwargs):
    return SlurmBackend(
        config=ExecutorConfig(kind="slurm", work_dir=str(tmp_path)),
        run_dir=tmp_path / "run",
        command_runner=commands,
        **kwargs,
    )


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
    b = backend(tmp_path, commands)
    p = invocation(tmp_path)
    h = b.submit(p, TaskResourceRequirements(cpus=7, memory="1 GB"))
    assert h.job_id == "123" and h.cluster == "lab"
    assert "--cpus-per-task=7" in commands.calls[0]
    assert "--mem=954M" in commands.calls[0]
    assert "--no-requeue" in commands.calls[0]
    script = (p.request.work_path / "submit.sh").read_text()
    assert "never-export" not in script and "'a; $(unsafe)'" in script
    b.poll([h])
    assert h.state == "PENDING"
    b.poll([h])
    assert h.state == "COMPLETED"
    Path(p.request.outputs["out"]).write_text("scientific output")
    write_json_file(
        p.manifest_path,
        {"outputs": dict(p.request.outputs), "attempt_id": p.attempt_id},
    )
    assert b.collect(h).outputs["out"] == p.request.outputs["out"]


def test_disappearance_is_not_success_and_accounting_is_batched(tmp_path):
    commands = Commands(["123", "", "", "", ""])
    now = [0]
    b = backend(tmp_path, commands, clock=lambda: now[0], accounting_timeout=5)
    h = b.submit(invocation(tmp_path), TaskResourceRequirements(cpus=1))
    b.poll([h])
    assert h.state == "PENDING"
    now[0] = 6
    with pytest.raises(RuntimeError, match="state is unknown"):
        b.poll([h])


def test_ambiguous_submission_is_recorded_never_retried(tmp_path):
    commands = Commands([subprocess.TimeoutExpired("sbatch", 30)])
    b = backend(tmp_path, commands)
    with pytest.raises(RuntimeError, match="not retried"):
        b.submit(invocation(tmp_path), TaskResourceRequirements(cpus=1))
    assert len(commands.calls) == 1
    assert (
        next(iter(json.loads(b.registry_path.read_text())["attempts"].values()))[
            "state"
        ]
        == "submission-uncertain"
    )


@pytest.mark.parametrize("accounting", ["", "123|COMPLETED|", "123||0:0"])
def test_terminal_queue_state_waits_for_accounting_exit_code(tmp_path, accounting):
    commands = Commands(["123", "123|COMPLETED", accounting, "", "123|COMPLETED|0:0"])
    b = backend(tmp_path, commands)
    h = b.submit(invocation(tmp_path), TaskResourceRequirements(cpus=1))
    b.poll([h])
    assert h.state == "PENDING"
    assert h.unknown_since is not None
    b.poll([h])
    assert h.state == "COMPLETED" and h.exit_code == "0:0"
    assert h.unknown_since is None


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
    b = backend(tmp_path, Commands(["123", "", f"123|{state}|{code}"]))
    h = b.submit(invocation(tmp_path), TaskResourceRequirements(cpus=1))
    b.poll([h])
    with pytest.raises(RuntimeError, match=state):
        b.collect(h)


def test_results_must_be_current_complete_and_present(tmp_path):
    p = invocation(tmp_path)
    with pytest.raises(RuntimeError, match="did not write"):
        p.collect()
    write_json_file(p.manifest_path, {"outputs": {}, "attempt_id": "stale"})
    with pytest.raises(RuntimeError, match="stale"):
        p.collect()
    write_json_file(
        p.manifest_path,
        {"outputs": dict(p.request.outputs), "attempt_id": p.attempt_id},
    )
    with pytest.raises(RuntimeError, match="missing"):
        p.collect()


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
        ExecutorConfig(
            kind="slurm", work_dir=str(tmp_path), slurm={"extra_args": [argument]}
        )


def test_resource_fields_resolve_independently():
    config = AdagioRunConfig.model_validate(
        {
            "resources": {
                "defaults": {"cpus": 2, "memory": "3 GB"},
                "tasks": {"a": {"cpus": 7}},
            }
        }
    )
    assert config.resources.for_task("a").model_dump() == {"cpus": 7, "memory": "3 GB"}
    assert config.resources.for_task("b").cpus == 2
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"executor": {"kind": "typo"}})
    with pytest.raises(ValueError):
        AdagioRunConfig.model_validate({"version": 2})


def test_shared_json_and_toml_fixture_and_strict_version():
    from adagio.cli.config import load_run_config

    fixture = Path(__file__).parent / "fixtures/slurm-run-v1"
    left = load_run_config(fixture.with_suffix(".json"))
    assert left == load_run_config(fixture.with_suffix(".toml"))
    assert left.resources.for_task("node.with.dots").model_dump() == {
        "cpus": 7,
        "memory": "4 GiB",
    }
    assert left.resources.for_task("memory-only").model_dump() == {
        "cpus": 2,
        "memory": "1 GB",
    }
    for value in [True, 1.0, "1", 2]:
        with pytest.raises(ValueError):
            AdagioRunConfig(version=value)


@pytest.mark.parametrize(
    "payload", [[], {"outputs": []}, {"outputs": {}, "reused": "false"}, "broken"]
)
def test_malformed_results_are_rejected(tmp_path, payload):
    prepared = invocation(tmp_path)
    prepared.manifest_path.write_text(json.dumps(payload))
    with pytest.raises(RuntimeError, match="malformed"):
        prepared.collect()
