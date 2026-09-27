"""Slurm for the batch backend: sbatch, squeue, sacct and scancel.

Slurm reuses job ids, so ``squeue`` and ``scancel`` always name the job as
well: given both a name and an id, Slurm matches only a job with both.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..resources import memory_bytes
from .base import JobState
from .batch import (
    SCHEDULER_ERRORS,
    BatchExecutorConfig,
    CommandRunner,
    JobRef,
    SchedulerCommandError,
    SchedulerStatus,
)

if TYPE_CHECKING:
    from ..resources import TaskResourceRequirements

# Exact long options only. An allowlist also blocks Slurm's abbreviated options,
# short aliases, replacement commands and options added by future Slurm releases.
SLURM_EXTRA_OPTIONS = frozenset({"constraint", "reservation", "licenses", "prefer", "nice"})


class SlurmOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: str | None = None
    account: str | None = None
    time_limit: str | None = None
    qos: str | None = None
    extra_args: list[str] = Field(default_factory=list)

    @field_validator("partition", "account", "qos")
    @classmethod
    def _clean_name(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[A-Za-z0-9_.@,+-]+", value):
            raise ValueError(
                "Slurm names must contain only letters, digits, _, ., @, comma, + or -."
            )
        return value

    @field_validator("time_limit")
    @classmethod
    def _valid_time(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not re.fullmatch(r"(?:[0-9]+-)?[0-9]+(?::[0-5][0-9]){0,2}", value):
            raise ValueError("Time limit must use minutes, HH:MM:SS or D-HH:MM:SS.")
        if not any(c in "123456789" for c in value):
            raise ValueError("Time limit must be greater than zero.")
        return value

    @field_validator("extra_args")
    @classmethod
    def _safe_arguments(cls, values: list[str]) -> list[str]:
        seen = set()
        for value in values:
            option, separator, argument = value.partition("=")
            name = option.removeprefix("--")
            if (
                not option.startswith("--")
                or name not in SLURM_EXTRA_OPTIONS
                or not separator
                or not argument
                or any(c in value for c in "\n\r\0")
            ):
                raise ValueError(
                    "Additional Slurm options must use --option=value; allowed options: "
                    + ", ".join(sorted(SLURM_EXTRA_OPTIONS))
                    + "."
                )
            if name in seen:
                raise ValueError(f"Duplicate additional Slurm option: {option}.")
            seen.add(name)
        return values


class SlurmExecutorConfig(BatchExecutorConfig):
    kind: Literal["slurm"] = "slurm"
    slurm: SlurmOptions = Field(default_factory=SlurmOptions)


_QUEUED = frozenset(
    {
        "PENDING",
        "REQUEUED",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
        "RESV_DEL_HOLD",
        "SPECIAL_EXIT",
    }
)
_RUNNING = frozenset(
    {
        "CONFIGURING",
        "RUNNING",
        "COMPLETING",
        "SUSPENDED",
        "STOPPED",
        "RESIZING",
        "SIGNALING",
        "STAGE_OUT",
    }
)
_ACTIVE = {state: JobState.QUEUED for state in _QUEUED} | {
    state: JobState.RUNNING for state in _RUNNING
}
_FINISHED = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
        "REVOKED",
    }
)
_EXIT_CODE = re.compile(r"\d+:\d+")
# sbatch reports every failure as "Batch job submission failed: <reason>",
# including failures reading the controller's reply after it may have created
# the job. Only these reasons are the controller refusing the request outright;
# anything else leaves the submission unconfirmed.
_SUBMISSION_FAILED = re.compile(r"batch job submission failed:\s*(.+)", re.IGNORECASE)
_REFUSALS = (
    "invalid partition name specified",
    "no partition specified or system default partition",
    "invalid account or account/partition combination specified",
    "invalid qos specification",
    "requested time limit is invalid",
    "requested node configuration is not available",
    "requested partition configuration not available now",
    "memory required by task is not available",
    "job violates accounting/qos policy",
    "invalid feature specification",
    "requested reservation is invalid",
    "invalid license specification",
    "invalid generic resource (gres) specification",
    "invalid wckey specification",
    "more processors requested than permitted",
    "node count specification invalid",
    "cpu count specification invalid",
    "user's group not permitted to use this partition",
    "access/permission denied",
)
_JOB_NAME = re.compile(r"adagio-[0-9a-f]{32}")


class SlurmScheduler:
    kind = "slurm"
    name = "Slurm"
    commands = ("sbatch", "squeue", "sacct", "scancel")
    # These variables act as extra options and filters: they would change what
    # is submitted, and hide jobs from lookups and cancellation.
    scrubbed_environment = ("SBATCH_", "SQUEUE_", "SACCT_", "SCANCEL_")
    # slurmctld acts on a request it receives until the request's credential
    # expires (MUNGE's default lifetime is five minutes), so a lost sbatch
    # reply can still become a job that long after it was sent.
    lost_reply_deadline = 360.0

    def __init__(self, options: SlurmOptions | None = None) -> None:
        self.options = options or SlurmOptions()

    @classmethod
    def for_executor(cls, config: SlurmExecutorConfig) -> SlurmScheduler:
        return cls(config.slurm)

    def submit_argv(
        self,
        *,
        script: Path,
        job_name: str,
        cwd: Path,
        log_path: Path,
        resources: TaskResourceRequirements,
    ) -> list[str]:
        argv = [
            "sbatch",
            "--parsable",
            "--nodes=1",
            "--ntasks=1",
            "--no-requeue",
            "--export=NONE",
            f"--job-name={job_name}",
            f"--chdir={cwd}",
            f"--output={log_path}",
            f"--error={log_path}",
            f"--cpus-per-task={resources.cpus}",
        ]
        if resources.memory:
            argv.append(f"--mem={memory_megabytes(resources.memory)}M")
        for field, option in (
            ("partition", "partition"),
            ("account", "account"),
            ("time_limit", "time"),
            ("qos", "qos"),
        ):
            value = getattr(self.options, field)
            if value is not None:
                argv.append(f"--{option}={value}")
        argv.extend(self.options.extra_args)
        argv.append(str(script))
        return argv

    def parse_submission(self, output: str, job_name: str) -> JobRef:
        match = re.fullmatch(r"([0-9]+)(?:;([A-Za-z0-9_.-]+))?", output)
        if match is None:
            raise SchedulerCommandError(
                f"Invalid sbatch --parsable response: {output!r}."
            )
        return JobRef(job_name, match.group(1), match.group(2))

    def rejected(self, error: SchedulerCommandError) -> bool:
        reasons = [
            reason.strip().lower() for reason in _SUBMISSION_FAILED.findall(error.stderr)
        ]
        return bool(reasons) and all(reason.startswith(_REFUSALS) for reason in reasons)

    def statuses(
        self, jobs: Sequence[JobRef], run: CommandRunner
    ) -> dict[JobRef, SchedulerStatus]:
        results: dict[JobRef, SchedulerStatus] = {}
        for cluster, group in _by_cluster(jobs):
            cluster_args = [f"--clusters={cluster}"] if cluster else []
            names = sorted({job.name for job in group})
            # The queue holds every job that can still use resources, and
            # finished ones for a few minutes; it is asked by our names only,
            # in every partition, including hidden ones.
            listed: dict[str, list[tuple[str, str]]] = {}
            queue_error: Exception | None = None
            try:
                for line in run(
                    [
                        "squeue",
                        "--noheader",
                        "--all",
                        "--states=all",
                        f"--name={','.join(names)}",
                        "--format=%i|%j|%T",
                        *cluster_args,
                    ]
                ).splitlines():
                    job_id, _, rest = line.strip().partition("|")
                    name, _, state = rest.partition("|")
                    listed.setdefault(name, []).append((job_id, state))
            except SCHEDULER_ERRORS as error:
                queue_error = error
            rows = {job: _queue_row(job, listed) for job in group}
            # Only accounting says how a job's allocation ended. Job ids are
            # reused, so it is asked by our names as well.
            ended = {
                job: job.job_id or row[0]
                for job, row in rows.items()
                if (row is None or row[1] in _FINISHED) and (job.job_id or row)
            }
            accounting: dict[str, tuple[str, str]] = {}
            accounting_error: Exception | None = None
            if ended:
                try:
                    for line in run(
                        [
                            "sacct",
                            "--noheader",
                            "--parsable2",
                            "--allocations",
                            f"--jobs={','.join(sorted(set(ended.values())))}",
                            f"--name={','.join(sorted({job.name for job in ended}))}",
                            "--format=JobIDRaw,State,ExitCode",
                            *cluster_args,
                        ]
                    ).splitlines():
                        parts = line.strip().split("|")
                        if len(parts) >= 3 and parts[1].strip():
                            accounting[parts[0]] = (
                                parts[1].split()[0].rstrip("+"),
                                parts[2],
                            )
                except SCHEDULER_ERRORS as error:
                    accounting_error = error
            for job in group:
                row = rows[job]
                results[job] = _status(
                    row,
                    accounting.get(ended.get(job) or ""),
                    queue_error=queue_error,
                    accounting_error=accounting_error if job in ended else None,
                )
        return results

    def cancel_commands(self, jobs: Sequence[JobRef]) -> list[list[str]]:
        commands = []
        for job in jobs:
            if not _JOB_NAME.fullmatch(job.name):
                raise ValueError(f"Not an Adagio job name: {job.name!r}.")
            # One job per command: scancel matches nothing given several names.
            commands.append(
                [
                    "scancel",
                    *([f"--clusters={job.cluster}"] if job.cluster else []),
                    f"--name={job.name}",
                    *([job.job_id] if job.job_id else []),
                ]
            )
        return commands


def memory_megabytes(value: str) -> int:
    """Slurm's ``--mem`` in whole MiB, rounded up so the request is never short."""
    return max(1, -(-memory_bytes(value) // 2**20))


def _queue_row(
    job: JobRef, listed: dict[str, list[tuple[str, str]]]
) -> tuple[str, str] | None:
    """The queue's (id, state) for ``job``, matched by its name and any known id."""
    rows = [row for row in listed.get(job.name, ()) if job.job_id in (None, row[0])]
    return max(rows, key=lambda row: row[1] not in _FINISHED, default=None)


def _status(
    row: tuple[str, str] | None,
    accounted: tuple[str, str] | None,
    *,
    queue_error: Exception | None,
    accounting_error: Exception | None,
) -> SchedulerStatus:
    job_id = row[0] if row else None
    if row is not None and row[1] not in _FINISHED:
        # Any state that is not an end, including ones this code does not
        # know, means the job may still hold or wait for resources.
        state = _ACTIVE.get(row[1], JobState.QUEUED)
        return SchedulerStatus(state, row[1], active=True, job_id=job_id)
    if accounted is not None:
        state, exit_code = accounted
        if state in _FINISHED and _EXIT_CODE.fullmatch(exit_code):
            succeeded = state == "COMPLETED" and exit_code == "0:0"
            return SchedulerStatus(
                JobState.SUCCEEDED if succeeded else JobState.FAILED,
                state,
                exit_code,
                active=False,
                job_id=job_id,
            )
        if queue_error is not None and state in _ACTIVE:
            return SchedulerStatus(_ACTIVE[state], state, active=True, job_id=job_id)
    if queue_error is not None:
        return SchedulerStatus(
            None, str(queue_error), active=None, answered=False, job_id=job_id
        )
    queue = f"{row[1]} in the queue" if row else "not in the queue"
    if accounting_error is not None:
        return SchedulerStatus(
            None,
            f"{queue}; accounting failed: {accounting_error}",
            active=False,
            answered=False,
            job_id=job_id,
        )
    accounting = (
        f"accounting shows {accounted[0]}, exit code {accounted[1] or 'none'}"
        if accounted is not None
        else "no accounting record"
    )
    return SchedulerStatus(None, f"{queue}; {accounting}", active=False, job_id=job_id)


def _by_cluster(
    jobs: Iterable[JobRef],
) -> list[tuple[str | None, list[JobRef]]]:
    groups: dict[str | None, list[JobRef]] = {}
    for job in jobs:
        groups.setdefault(job.cluster, []).append(job)
    return list(groups.items())
