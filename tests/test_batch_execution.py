"""A whole pipeline through the batch backend, with a scheduler that runs jobs here.

The scheduler adapter is the only fake: planning, attempt directories, shared
staging, job scripts, the submission registry and result collection are the
real code paths a Slurm run takes.
"""

import json
import sys
from pathlib import Path


from adagio.execution.backends.base import JobState
from adagio.execution.backends.batch import (
    BatchBackend,
    BatchExecutorConfig,
    JobRef,
    SchedulerStatus,
)
from adagio.executors.base import TaskEnvironmentOverride
from adagio.executors.container_support import container_python_root
from adagio.executors.defaults import ConfigurableTaskEnvironmentResolver
from adagio.executors.prepared import PreparedInvocation
from adagio.executors.task_contract import (
    container_log_path,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor

PIPELINE = Path(__file__).parent / "fixtures/slurm-branch.adg"

WORKER = """
import json, sys
spec = json.load(open(sys.argv[1]))
print("working on", sys.argv[1])
if spec["fail"]:
    sys.exit(3)
for path in spec["outputs"].values():
    open(path, "w").write("result")
json.dump(
    {"outputs": spec["outputs"], "reused": False, "attempt_id": spec["attempt_id"]},
    open(spec["result_manifest"], "w"),
)
"""

RUN_JOB = """
import pathlib, subprocess, sys
script, log, job = sys.argv[1:]
with open(log, "w") as out:
    code = subprocess.call(["sh", script], stdout=out, stderr=subprocess.STDOUT)
pathlib.Path(script).with_suffix(".exit").write_text(str(code))
print(job)
"""


class HereScheduler:
    """Runs each job to completion at submission; reports it on the next poll."""

    kind = "here"
    name = "Here"
    commands = ("sh",)
    scrubbed_environment = ()

    def __init__(self):
        self.scripts = {}
        self.resources = {}

    def submit_argv(self, *, script, job_name, cwd, log_path, resources):
        job_id = str(len(self.scripts) + 1)
        self.scripts[job_id] = script
        self.resources[job_id] = resources
        return [sys.executable, "-c", RUN_JOB, str(script), str(log_path), job_id]

    def parse_submission(self, output):
        return JobRef(output)

    def statuses(self, jobs, run):
        results = {}
        for job in jobs:
            code = self.scripts[job.job_id].with_suffix(".exit").read_text()
            results[job] = SchedulerStatus(
                JobState.SUCCEEDED if code == "0" else JobState.FAILED,
                "DONE",
                f"{code}:0",
            )
        return results

    def cancel_commands(self, jobs):
        return []

    def find_jobs(self, job_name, run):
        return []


class WorkerLauncher:
    """A task environment whose command is a small Python worker."""

    kind = "conda"

    def __init__(self, fail=()):
        self.fail = set(fail)

    def prepare(self, *, environment, request, shared=False):
        task = request.task
        spec_path = task_spec_path(task_id=task.id, work_path=request.work_path)
        manifest = result_manifest_path(task_id=task.id, work_path=request.work_path)
        write_json_file(
            spec_path,
            {
                "outputs": dict(request.outputs),
                "result_manifest": str(manifest),
                "fail": task.id in self.fail,
            },
        )
        container_python_root(work_path=request.work_path, force_stage=shared)
        return PreparedInvocation(
            command=[sys.executable, "-c", WORKER, str(spec_path)],
            env=None,
            cwd=request.cwd,
            spec_path=spec_path,
            manifest_path=manifest,
            log_path=container_log_path(task_id=task.id, work_path=request.work_path),
            request=request,
            image_ref=environment.reference,
        )


class Events(Monitor):
    def __init__(self):
        self.events = []

    def start_task(self, *, task_id, **details):
        self.events.append(("start", task_id, details.get("scheduler_job_id")))

    def finish_task(self, *, task_id, status="completed", error=None, **details):
        self.events.append(("finish", task_id, status, details.get("log_path")))


def run(tmp_path, *, fail=()):
    environment = tmp_path / "env"
    environment.mkdir()
    table = tmp_path / "table.qza"
    table.write_text("input")
    scheduler = HereScheduler()
    backend = BatchBackend(
        config=BatchExecutorConfig(work_dir=str(tmp_path / "shared"), max_in_flight=2),
        scheduler=scheduler,
        sleep=lambda _: None,
    )
    executor = TaskEnvironmentExecutor(
        environment_resolver=ConfigurableTaskEnvironmentResolver(
            default_override=TaskEnvironmentOverride(
                kind="conda", reference=str(environment)
            )
        ),
        launchers={"conda": WorkerLauncher(fail)},
        backend=backend,
    )
    monitor = Events()
    pipeline = AdagioPipeline.model_validate(json.loads(PIPELINE.read_text()))
    arguments = AdagioArguments(
        inputs={"table": str(table)}, parameters={}, outputs=str(tmp_path / "out")
    )
    error = None
    try:
        executor.execute(
            pipeline=pipeline,
            arguments=arguments,
            monitor=monitor,
            log_dir=str(tmp_path / "logs"),
        )
    except RuntimeError as raised:
        error = raised
    return monitor.events, scheduler, list((tmp_path / "shared").glob("run-*")), error


def test_pipeline_runs_one_job_per_task_and_cleans_up(tmp_path):
    events, scheduler, runs, error = run(tmp_path)
    assert error is None
    assert len(scheduler.scripts) == 4
    assert [e for e in events if e[0] == "start"] == [
        ("start", "A", "1"),
        ("start", "B", "2"),
        ("start", "C", "3"),
        ("start", "D", "4"),
    ]
    assert [e[1] for e in events if e[0] == "finish" and e[2] == "completed"] == [
        "A",
        "B",
        "C",
        "D",
    ]
    assert [p.name for p in (tmp_path / "out").iterdir()] == ["merged"]
    assert runs == []
    assert "D_container.log" in {p.name for p in (tmp_path / "logs").iterdir()}
    assert all(r.cpus == 1 for r in scheduler.resources.values())


def test_failed_job_stops_dependents_and_keeps_its_logs(tmp_path):
    events, scheduler, runs, error = run(tmp_path, fail={"B"})
    assert "Here job 2 ended DONE (exit 3:0)" in str(error)
    finished = {e[1]: e for e in events if e[0] == "finish"}
    assert finished["A"][2] == "completed"
    assert finished["B"][2] == "failed"
    assert finished["D"][2] == "skipped"
    assert "working on" in Path(finished["B"][3]).read_text()
    assert "4" not in scheduler.scripts
    (kept,) = runs
    registry = json.loads((kept / "submissions.json").read_text())
    assert {entry["node_id"] for entry in registry["attempts"].values()} <= {"A", "B", "C"}
