"""One coordinator publishes results; workers never mutate pipeline state.

Task coroutines prepare an opaque invocation, suspend across backend execution,
then publish its validated result on the coordinating thread. Whole-action
bindings are empty; attempt and scheduler identities are separate.
"""

import inspect
import signal
import threading
import time
import traceback
import uuid
from dataclasses import replace
from pathlib import Path

from adagio.executors.serial_runner import (
    TaskOutcome,
    _coerce_outcome,
    _persist_task_log,
)
from adagio.executors.task_contract import write_json_file
from adagio.model.task import input_source_ids

from .slurm import TERMINAL, SlurmBackend


def run_local_steps(steps):
    from adagio.executors.task_environments import _launch

    if not inspect.isgenerator(steps):
        return steps
    value = None
    while True:
        try:
            launcher, kwargs = steps.send(value)
        except StopIteration as done:
            return done.value
        value = _launch(launcher, **kwargs)


def coordinate(
    *,
    execution_plan,
    state,
    resolve_task,
    finish_outputs,
    sig,
    arguments,
    monitor,
    console,
    run_config,
):
    slurm = run_config is not None and run_config.executor.kind == "slurm"
    backend = (
        SlurmBackend(config=run_config.executor, run_dir=state.work_path)
        if slurm
        else None
    )
    limit = run_config.executor.max_in_flight if slurm else 1
    completed = set()
    remaining = list(execution_plan)
    producers = {
        output.id: task.id
        for task in execution_plan
        for output in task.outputs.values()
    }
    dependencies = {
        task.id: {
            producers[i]
            for src in task.inputs.values()
            for i in input_source_ids(src)
            if i in producers and producers[i] != task.id
        }
        for task in execution_plan
    }
    active = {}  # node -> coroutine, handle, isolated attempt path
    task_id = None
    original_handler = None
    if threading.current_thread() is threading.main_thread():
        original_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, _interrupt)
    if slurm:
        write_json_file(
            state.work_path / "config.json", run_config.model_dump(exclude_none=True)
        )
        write_json_file(
            state.work_path / "plan.json",
            {
                "tasks": [
                    {
                        "node_id": task.id,
                        "binding": {},
                        "inputs": [
                            i
                            for src in task.inputs.values()
                            for i in input_source_ids(src)
                        ],
                        "resources": run_config.resources.for_task(task.id).model_dump(
                            exclude_none=True
                        ),
                    }
                    for task in execution_plan
                ]
            },
        )
        if console:
            console.print(f"Slurm run directory: {state.work_path}")
        # Runtime fallback discovers this single run's registry; no credentials in it.
        import os

        if pointer := os.getenv("ADAGIO_SLURM_REGISTRY_POINTER"):
            write_json_file(Path(pointer), {"registry": str(backend.registry_path)})

    def publish(task, outcome, task_state):
        outcome = _coerce_outcome(outcome)
        _persist_task_log(state=task_state, task_id=task.id, outcome=outcome)
        finish_outputs(
            sig=sig,
            arguments=arguments,
            state=state,
            monitor=monitor,
            require_all=False,
        )
        monitor.advance_task(task_id=task.id, advance=1)
        monitor.finish_task(
            task_id=task.id,
            status="cached" if outcome.reused else "completed",
            **outcome.enrichment,
        )
        completed.add(task.id)

    def advance(task, steps, task_state, value=None):
        try:
            launcher, kwargs = steps.send(value)
        except StopIteration as done:
            publish(task, done.value, task_state)
            return
        environment, request = kwargs["environment"], kwargs["request"]
        if environment.kind not in {"apptainer", "conda"} or not hasattr(
            launcher, "prepare"
        ):
            raise ValueError(
                "Slurm supports Apptainer or shared Conda environments; Docker is unsupported."
            )
        prepared = launcher.prepare(
            environment=environment, request=request, shared=True
        )
        prepared.node_id = task.id
        handle = backend.submit(prepared, run_config.resources.for_task(task.id))
        active[task.id] = (task, steps, handle, task_state, False)
        if console:
            console.print(f"Task {task.id}: queued as Slurm job {handle.job_id}")

    try:
        while remaining or active:
            for task in list(remaining):
                if len(active) >= limit:
                    break
                if not dependencies[task.id] <= completed:
                    continue
                task_id = task.id
                remaining.remove(task)
                # Shared dictionaries are touched exclusively by this thread;
                # only work_path differs per coroutine. No worker receives state.
                task_state = state
                if slurm:
                    attempt_dir = state.work_path / ("attempt-" + uuid.uuid4().hex)
                    attempt_dir.mkdir(mode=0o700)
                    task_state = replace(
                        state, work_path=attempt_dir, cwd=state.work_path
                    )
                steps = resolve_task(task, task_state, console)
                if not slurm:
                    monitor.start_task(task_id=task.id)
                    publish(task, run_local_steps(steps), task_state)
                elif inspect.isgenerator(steps):
                    advance(task, steps, task_state)
                else:
                    publish(task, steps, task_state)
            if active:
                backend.poll([entry[2] for entry in active.values()])
                # Detect every failure before releasing any successful dependents.
                for task, steps, handle, task_state, started in list(active.values()):
                    task_id = task.id
                    if handle.state in TERMINAL and (
                        handle.state != "COMPLETED" or handle.exit_code != "0:0"
                    ):
                        backend.collect(handle)
                for task, steps, handle, task_state, started in list(active.values()):
                    task_id = task.id
                    if not started and handle.state != "PENDING":
                        monitor.start_task(
                            task_id=task.id, scheduler_job_id=handle.job_id
                        )
                        active[task.id] = (task, steps, handle, task_state, True)
                    if handle.state == "COMPLETED":
                        result = backend.collect(handle)
                        del active[task.id]
                        advance(task, steps, task_state, result)
                if active:
                    time.sleep(0.5)
            elif remaining and not any(
                dependencies[t.id] <= completed for t in remaining
            ):
                raise RuntimeError(
                    "Unable to resolve task dependencies after publication."
                )
    except BaseException as error:
        cleanup = backend.cancel(backend.handles) if backend else []
        diagnostic = (
            str(error) or "Run interrupted; outstanding Slurm jobs were canceled."
        )
        if cleanup:
            diagnostic += " Cleanup incomplete: " + "; ".join(cleanup)
            if hasattr(error, "add_note"):
                error.add_note(diagnostic)
            if console:
                console.print(diagnostic)
        canceled = isinstance(error, (KeyboardInterrupt, SystemExit))
        failed_logs = {}
        for task, steps, handle, task_state, started in active.values():
            outcome = TaskOutcome(
                enrichment={"log_path": str(handle.prepared.log_path)}
            )
            _persist_task_log(state=task_state, task_id=task.id, outcome=outcome)
            failed_logs[task.id] = outcome.enrichment["log_path"]
        for task in execution_plan:
            if task.id in completed:
                continue
            monitor.finish_task(
                task_id=task.id,
                status="canceled"
                if canceled
                else ("failed" if task.id == task_id else "skipped"),
                error=diagnostic
                if task.id == task_id or canceled
                else f"Skipped because task {task_id!r} failed.",
                traceback=traceback.format_exc() if task.id == task_id else None,
                log_path=failed_logs.get(task.id),
            )
        if state.save_output_started:
            monitor.finish_save_output()
        raise
    finally:
        if original_handler is not None:
            signal.signal(signal.SIGTERM, original_handler)


def _interrupt(signum, frame):
    raise KeyboardInterrupt("Run interrupted by SIGTERM.")
