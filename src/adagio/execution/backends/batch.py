"""Scheduler-neutral batch execution: one scheduler job per task invocation.

A scheduler adapter (``slurm``) only translates: how to submit a script with a
resource request, how to read job states, and how to cancel or find jobs.
Everything about owning a run on shared storage lives here, so every
scheduler gets the same guarantees:

* the run's files live in a private directory under the shared work directory,
  removed only after the run succeeds;
* each submission is recorded before the scheduler is asked, and a submission
  whose reply is lost is never retried;
* a job that disappears from the scheduler is never taken as success, and a
  scheduler that cannot be reached is waited out rather than failing tasks;
* polling backs off while nothing changes;
* every job is looked up and cancelled by its run-unique name, never by a
  bare job id, which the scheduler reuses;
* cancellation acts only on this run's recorded jobs, and cleanup after an
  abnormal exit (``adagio cleanup``) runs exactly the same code, and only once
  the run's owner is gone (``run_record.owning_run``).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

from adagio.executors.task_contract import read_json_file, write_json_file

from .base import JobHandle, JobState, TaskInvocation, Workspace
from .job_script import check_shared_paths, render_job_script
from .run_record import owning_run, write_run_record
from .submissions import SubmissionRegistry

if TYPE_CHECKING:
    from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult
    from adagio.executors.prepared import PreparedInvocation

    from ..resources import TaskResourceRequirements

#: Task environment kinds a compute host can run from shared storage.
SHARED_ENVIRONMENTS = frozenset({"apptainer", "conda"})

logger = logging.getLogger(__name__)


class BatchExecutorConfig(BaseModel):
    """Settings every batch scheduler shares; adapters add their own options."""

    model_config = ConfigDict(extra="forbid")

    work_dir: str
    max_in_flight: int = Field(default=8, ge=1, strict=True)

    @field_validator("work_dir")
    @classmethod
    def _shared_work_dir(cls, value: str) -> str:
        if any(c in value for c in "\n\r\0"):
            raise ValueError(
                "Shared work directory must not contain control characters."
            )
        if not value.startswith("/"):
            raise ValueError(
                "Batch execution requires an absolute shared work directory, "
                "visible at the same path on submit and compute hosts."
            )
        return value


@dataclass(frozen=True)
class JobRef:
    """One job, identified by the run-unique name it was submitted under.

    Schedulers reuse job ids, so every lookup and cancellation matches the
    name; ``job_id`` is None while the scheduler has not confirmed one.
    """

    name: str
    job_id: str | None = None
    cluster: str | None = None


@dataclass(frozen=True)
class SchedulerStatus:
    """What the scheduler reports about one job.

    ``state`` is the confirmed outcome so far, None while there is none.
    ``active`` is whether the queue still holds the job: None when the queue
    could not be asked. A job can leave the queue before its outcome is
    recorded; ``answered`` is False when a query that could have confirmed
    the outcome failed, so the missing outcome proves nothing.
    """

    state: JobState | None
    detail: str
    exit_code: str | None = None
    active: bool | None = None
    answered: bool = True
    #: The id the scheduler listed, for a job looked up by name alone.
    job_id: str | None = None


class SchedulerCommandError(RuntimeError):
    """A scheduler command exited unsuccessfully."""

    def __init__(self, message: str, *, stderr: str = "") -> None:
        super().__init__(message)
        self.stderr = stderr


CommandRunner = Callable[[list[str]], str]


class Scheduler(Protocol):
    kind: ClassVar[str]
    name: ClassVar[str]
    #: Executables the scheduler needs on the submit host.
    commands: ClassVar[tuple[str, ...]]
    #: Environment variable prefixes that would silently change what is submitted.
    scrubbed_environment: ClassVar[tuple[str, ...]]

    def submit_argv(
        self,
        *,
        script: Path,
        job_name: str,
        cwd: Path,
        log_path: Path,
        resources: TaskResourceRequirements,
    ) -> list[str]: ...

    def parse_submission(self, output: str, job_name: str) -> JobRef: ...

    def rejected(self, error: SchedulerCommandError) -> bool:
        """Whether a failed submission was refused outright, so no job exists."""
        ...

    def statuses(
        self, jobs: Sequence[JobRef], run: CommandRunner
    ) -> dict[JobRef, SchedulerStatus]:
        """Look jobs up by name, and by id as well where one is known."""
        ...

    def cancel_commands(self, jobs: Sequence[JobRef]) -> list[list[str]]:
        """Commands that cancel exactly these jobs, matched by name."""
        ...


def missing_commands(scheduler: Scheduler | type[Scheduler]) -> list[str]:
    return [name for name in scheduler.commands if shutil.which(name) is None]


def make_command_runner(
    scheduler: Scheduler | type[Scheduler],
    *,
    timeout: float = 30.0,
    runner: Callable[..., subprocess.CompletedProcess] | None = None,
) -> CommandRunner:
    """Run scheduler commands with a timeout and without overriding variables."""
    runner = runner or subprocess.run

    def run(argv: list[str]) -> str:
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(scheduler.scrubbed_environment)
        }
        result = runner(
            argv,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            env=env,
        )
        if result.returncode:
            raise SchedulerCommandError(
                f"{argv[0]} failed ({result.returncode}): {result.stderr.strip()}",
                stderr=result.stderr,
            )
        return result.stdout.strip()

    return run


SCHEDULER_ERRORS = (SchedulerCommandError, OSError, subprocess.SubprocessError)


@dataclass
class BatchHandle(JobHandle):
    job: JobRef | None = None
    prepared: PreparedInvocation | None = None
    attempt_id: str = ""
    detail: str = ""
    exit_code: str | None = None
    #: When the scheduler was first seen without the job or its outcome.
    unknown_since: float | None = None
    #: When the scheduler first could not be asked about the job.
    unreachable_since: float | None = None


class BatchBackend:
    """Submit each invocation as one scheduler job on shared storage."""

    starts_on_submit = False

    def __init__(
        self,
        *,
        config: BatchExecutorConfig,
        scheduler: Scheduler,
        run_record: Path | None = None,
        command_runner: Callable[..., subprocess.CompletedProcess] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        poll_interval: tuple[float, float] = (2.0, 30.0),
        accounting_timeout: float = 120.0,
        outage_timeout: float = 1800.0,
        command_timeout: float = 30.0,
    ) -> None:
        self.kind = scheduler.kind
        self.config = config
        self.scheduler = scheduler
        self.capacity = config.max_in_flight
        self._run_record = run_record
        self._run = make_command_runner(
            scheduler, timeout=command_timeout, runner=command_runner
        )
        self._clock = clock
        self._sleep = sleep
        self._min_interval, self._max_interval = poll_interval
        self._interval = self._min_interval
        self._accounting_timeout = accounting_timeout
        self._outage_timeout = outage_timeout
        # A submission whose reply was lost cannot become a job after this.
        self._submit_deadline = 2 * command_timeout
        self._registry: SubmissionRegistry | None = None
        self._stopped = False

    @property
    def registry(self) -> SubmissionRegistry:
        if self._registry is None:
            raise RuntimeError("The batch workspace has not been created.")
        return self._registry

    def check_available(self) -> None:
        missing = missing_commands(self.scheduler)
        if missing:
            raise RuntimeError(
                f"The {self.scheduler.name} executor needs {', '.join(missing)} "
                "on PATH on this host; the run was not started."
            )

    def unsupported_reason(self, environment: TaskEnvironmentSpec) -> str | None:
        if environment.kind in SHARED_ENVIRONMENTS:
            return None
        return (
            f"{self.scheduler.name} supports Apptainer or shared Conda environments; "
            f"{environment.kind.capitalize()} is unsupported."
        )

    @contextmanager
    def workspace(self) -> Iterator[Workspace]:
        with ExitStack() as owned:
            if self._run_record is not None:
                # Held until the run is over, so ``adagio cleanup`` cannot act
                # on this run while this process still submits and cancels.
                owned.enter_context(owning_run(self._run_record))
            root = Path(self.config.work_dir) / f"run-{uuid.uuid4().hex}"
            root.mkdir(parents=True, mode=0o700)
            self._registry = SubmissionRegistry.create(root, executor=self.kind)
            if self._run_record is not None:
                write_run_record(
                    self._run_record, executor=self.kind, registry=self._registry.path
                )
            yield Workspace(root=root, cwd=root)
            # Reached only when the run finished and saved its outputs. A failed
            # or interrupted run keeps its directory: task logs, and the registry
            # that cancellation and reconciliation read.
            shutil.rmtree(root, ignore_errors=True)
            if self._run_record is not None:
                self._run_record.unlink(missing_ok=True)

    def attempt_directory(self, workspace: Workspace) -> Path:
        path = workspace.root / f"attempt-{uuid.uuid4().hex}"
        path.mkdir(mode=0o700)
        return path

    def submit(
        self, invocation: TaskInvocation, resources: TaskResourceRequirements
    ) -> BatchHandle:
        name = self.scheduler.name
        if self._stopped:
            raise RuntimeError(f"{name} submissions have stopped for this run.")
        prepare = getattr(invocation.launcher, "prepare", None)
        if prepare is None:
            raise RuntimeError(
                f"The {invocation.environment.kind} task environment cannot run on {name}."
            )
        prepared = prepare(
            environment=invocation.environment, request=invocation.request, shared=True
        )
        check_shared_paths(prepared)
        # The worker echoes this into its manifest, so a stale or foreign
        # manifest can never be collected as this attempt's result.
        spec = read_json_file(prepared.spec_path)
        spec["attempt_id"] = prepared.attempt_id
        write_json_file(prepared.spec_path, spec)
        script = prepared.request.work_path / "job.sh"
        script.write_text(render_job_script(prepared), encoding="utf-8")
        job_name = f"adagio-{prepared.attempt_id}"
        argv = self.scheduler.submit_argv(
            script=script,
            job_name=job_name,
            cwd=prepared.cwd,
            log_path=prepared.log_path,
            resources=resources,
        )
        registry = self.registry
        registry.record_intent(
            prepared.attempt_id,
            node_id=invocation.task_id or invocation.request.task.id,
            job_name=job_name,
            argv=argv,
            script=str(script),
            log_path=str(prepared.log_path),
        )
        try:
            job = self.scheduler.parse_submission(self._run(argv), job_name)
        except BaseException as error:
            self._stopped = True
            if isinstance(error, SchedulerCommandError) and self.scheduler.rejected(
                error
            ):
                reason = error.stderr.strip()
                registry.record_rejected(prepared.attempt_id, reason)
                raise RuntimeError(
                    f"{name} rejected the job for task "
                    f"{invocation.task_id or invocation.request.task.id!r}: {reason}"
                ) from error
            registry.record_unconfirmed(
                prepared.attempt_id, str(error) or type(error).__name__
            )
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(
                f"{name} did not confirm submission of {job_name}: {error}. "
                "It was not retried; if a job with that name exists, cancel it. "
                f"Registry: {registry.path}."
            ) from error
        registry.record_submitted(
            prepared.attempt_id, job_id=job.job_id, cluster=job.cluster
        )
        self._interval = self._min_interval
        return BatchHandle(
            state=JobState.QUEUED,
            job_id=job.job_id,
            log_path=prepared.log_path,
            job=job,
            prepared=prepared,
            attempt_id=prepared.attempt_id,
        )

    def wait(self, handles: Sequence[BatchHandle]) -> None:
        self._sleep(self._interval)
        if self._refresh(handles):
            self._interval = self._min_interval
        else:
            self._interval = min(self._interval * 2, self._max_interval)

    def _refresh(self, handles: Sequence[BatchHandle]) -> bool:
        now = self._clock()
        statuses = self.scheduler.statuses([h.job for h in handles], self._run)
        changed = False
        for handle in handles:
            status = statuses.get(handle.job) or SchedulerStatus(
                None, "no status", answered=False
            )
            if status.state is None:
                reason = self._unresolved(handle, status, now)
                if reason is None:
                    continue
                # Disappearance is never success: the task fails. The job may
                # still be running, so its registry entry stays outstanding
                # and cancellation and cleanup still try to stop it.
                handle.state = JobState.FAILED
                handle.detail = f"{reason}. Registry: {self.registry.path}"
                self.registry.record_unknown(handle.attempt_id, status.detail)
                changed = True
                continue
            handle.unknown_since = handle.unreachable_since = None
            current = (status.state, status.detail, status.exit_code)
            if current != (handle.state, handle.detail, handle.exit_code):
                changed = True
            handle.state, handle.detail, handle.exit_code = current
            self.registry.record_status(
                handle.attempt_id,
                state=status.state,
                scheduler_state=status.detail,
                exit_code=status.exit_code,
            )
        self.registry.save()
        return changed

    def _unresolved(
        self, handle: BatchHandle, status: SchedulerStatus, now: float
    ) -> str | None:
        """Why a job with no confirmed state has failed, or None to keep waiting.

        A scheduler that cannot be asked says nothing about the job, so an
        outage is waited out for far longer than a job that is positively gone
        without an outcome.
        """
        if not status.answered:
            if handle.unreachable_since is None:
                handle.unreachable_since = now
                logger.warning(
                    "%s could not be asked about job %s (%s); waiting for it.",
                    self.scheduler.name,
                    handle.job_id,
                    status.detail,
                )
            if now - handle.unreachable_since < self._outage_timeout:
                return None
            return (
                f"state is unknown: {self.scheduler.name} could not be asked for "
                f"{self._outage_timeout:.0f}s: {status.detail}"
            )
        handle.unreachable_since = None
        if handle.unknown_since is None:
            handle.unknown_since = now
        if now - handle.unknown_since < self._accounting_timeout:
            return None
        return f"state is unknown: {status.detail}. Disappearance is not success"

    def collect(self, handle: BatchHandle) -> TaskExecutionResult:
        if handle.state is not JobState.SUCCEEDED:
            name = self.scheduler.name
            if handle.exit_code is None:
                raise RuntimeError(f"{name} job {handle.job_id} {handle.detail}.")
            raise RuntimeError(
                f"{name} job {handle.job_id} ended {handle.detail} "
                f"(exit {handle.exit_code}). Logs: {handle.log_path}"
            )
        return handle.prepared.collect()

    def cancel(self) -> list[str]:
        self._stopped = True
        if self._registry is None:
            return []
        return cancel_outstanding(
            self._registry,
            self.scheduler,
            self._run,
            clock=self._clock,
            sleep=self._sleep,
            submit_deadline=self._submit_deadline,
        )

    def describe(self) -> dict[str, Any]:
        return self.config.model_dump(exclude_none=True)


def cancel_outstanding(
    registry: SubmissionRegistry,
    scheduler: Scheduler,
    run: CommandRunner,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.time,
    confirm_for: float = 10.0,
    submit_deadline: float = 60.0,
) -> list[str]:
    """Cancel every recorded job that may still hold resources.

    Returns what could not be confirmed. Only jobs in the registry are touched,
    and each is cancelled and confirmed by its run-unique name. A submission
    whose reply was lost is cancelled by name as well; once ``submit_deadline``
    has passed since it was requested and no job has that name, none will.
    """
    jobs: dict[JobRef, tuple[str, float]] = {
        JobRef(entry["job_name"], entry.get("job_id"), entry.get("cluster")): (
            attempt_id,
            float(entry.get("intent_at") or 0),
        )
        for attempt_id, entry in registry.outstanding()
    }
    failures: list[str] = []

    def cancel(targets: Sequence[JobRef]) -> None:
        for argv in scheduler.cancel_commands(targets):
            try:
                run(argv)
            except SCHEDULER_ERRORS as error:
                # Often the job already ended; only confirmation decides.
                failures.append(f"{scheduler.name} cancellation failed: {error}")

    cancel(list(jobs))
    unconfirmed = [intent for job, (_, intent) in jobs.items() if job.job_id is None]
    wait = max([confirm_for] + [i + submit_deadline - now() for i in unconfirmed])
    deadline = clock() + wait
    remaining = list(jobs)
    while remaining:
        statuses = scheduler.statuses(remaining, run)
        appeared = []
        for job in list(remaining):
            status = statuses.get(job)
            if status is None or status.active is None:
                continue
            attempt_id, intent = jobs[job]
            if job.job_id is None and status.job_id is not None:
                registry.record_found(attempt_id, job_id=status.job_id)
            if status.active:
                if job.job_id is None:
                    appeared.append(job)
                continue
            if (
                job.job_id is None
                and status.job_id is None
                and now() < intent + submit_deadline
            ):
                # Not listed yet: the lost reply may still become a job.
                continue
            if status.state is not None and status.state.terminal:
                registry.record_status(
                    attempt_id,
                    state=status.state,
                    scheduler_state=status.detail,
                    exit_code=status.exit_code,
                )
            else:
                registry.record_ended(attempt_id, status.detail)
            remaining.remove(job)
        if not remaining or clock() >= deadline:
            break
        # A lost reply that became a job after the first cancellation.
        cancel(appeared)
        sleep(0.5)
    errors: list[str] = []
    if remaining:
        errors.extend(failures)
        errors.append(
            f"Cancellation not confirmed for {scheduler.name} jobs "
            + ", ".join(
                f"{job.name} ({job.job_id or 'id unknown'})" for job in remaining
            )
            + f". Registry: {registry.path}."
        )
    registry.save()
    return errors
