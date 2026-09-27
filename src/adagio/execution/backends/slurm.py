"""Slurm for the batch backend: sbatch, squeue, sacct and scancel."""

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


_QUEUED = frozenset({"PENDING", "REQUEUED", "REQUEUE_FED", "REQUEUE_HOLD"})
_RUNNING = frozenset(
    {
        "CONFIGURING",
        "RUNNING",
        "COMPLETING",
        "SUSPENDED",
        "RESIZING",
        "SIGNALING",
        "STAGE_OUT",
    }
)
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
    # SBATCH_* variables override sbatch options and would change what is submitted.
    scrubbed_environment = ("SBATCH_",)

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

    def parse_submission(self, output: str) -> JobRef:
        match = re.fullmatch(r"([0-9]+)(?:;([A-Za-z0-9_.-]+))?", output)
        if match is None:
            raise SchedulerCommandError(
                f"Invalid sbatch --parsable response: {output!r}."
            )
        return JobRef(match.group(1), match.group(2))

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
            ids = [job.job_id for job in group]
            failure: Exception | None = None
            queued: dict[str, str] = {}
            try:
                for line in run(
                    [
                        "squeue",
                        "--noheader",
                        f"--jobs={','.join(ids)}",
                        "--format=%i|%T",
                        *cluster_args,
                    ]
                ).splitlines():
                    job_id, _, state = line.strip().partition("|")
                    if job_id in ids:
                        queued[job_id] = state
            except SCHEDULER_ERRORS as error:
                # squeue fails outright once a job has left the controller;
                # accounting still knows how it ended.
                failure = error
            # A finished job leaves the queue, or lingers there without an exit
            # code; only accounting says how its allocation ended.
            accounting: dict[str, tuple[str, str]] = {}
            finished = [i for i in ids if queued.get(i) not in _QUEUED | _RUNNING]
            if finished:
                try:
                    for line in run(
                        [
                            "sacct",
                            "--noheader",
                            "--parsable2",
                            "--allocations",
                            f"--jobs={','.join(finished)}",
                            "--format=JobIDRaw,State,ExitCode",
                            *cluster_args,
                        ]
                    ).splitlines():
                        parts = line.strip().split("|")
                        if len(parts) >= 3 and parts[0] in ids and parts[1].strip():
                            accounting[parts[0]] = (
                                parts[1].split()[0].rstrip("+"),
                                parts[2],
                            )
                except SCHEDULER_ERRORS as error:
                    failure = error
            for job in group:
                state, code = accounting.get(job.job_id, (queued.get(job.job_id), None))
                status = _status(state, code)
                if status.state is None and failure is not None:
                    status = SchedulerStatus(None, str(failure))
                results[job] = status
        return results

    def cancel_commands(self, jobs: Sequence[JobRef]) -> list[list[str]]:
        return [
            [
                "scancel",
                *([f"--clusters={cluster}"] if cluster else []),
                *(job.job_id for job in group),
            ]
            for cluster, group in _by_cluster(jobs)
        ]

    def find_jobs(self, job_name: str, run: CommandRunner) -> list[JobRef]:
        if not _JOB_NAME.fullmatch(job_name):
            raise ValueError(f"Not an Adagio job name: {job_name!r}.")
        output = run(
            ["squeue", "--noheader", f"--name={job_name}", "--format=%i|%j"]
        )
        found = []
        for line in output.splitlines():
            job_id, _, name = line.strip().partition("|")
            if name == job_name and job_id.isdigit():
                found.append(JobRef(job_id))
        return found


def memory_megabytes(value: str) -> int:
    """Slurm's ``--mem`` in whole MiB, rounded up so the request is never short."""
    return max(1, -(-memory_bytes(value) // 2**20))


def _status(state: str | None, exit_code: str | None) -> SchedulerStatus:
    if state in _QUEUED:
        return SchedulerStatus(JobState.QUEUED, state)
    if state in _RUNNING:
        return SchedulerStatus(JobState.RUNNING, state)
    if state in _FINISHED and exit_code and _EXIT_CODE.fullmatch(exit_code):
        succeeded = state == "COMPLETED" and exit_code == "0:0"
        return SchedulerStatus(
            JobState.SUCCEEDED if succeeded else JobState.FAILED, state, exit_code
        )
    return SchedulerStatus(
        None, "queue and accounting have no confirmed state and exit code"
    )


def _by_cluster(
    jobs: Iterable[JobRef],
) -> list[tuple[str | None, list[JobRef]]]:
    groups: dict[str | None, list[JobRef]] = {}
    for job in jobs:
        groups.setdefault(job.cluster, []).append(job)
    return list(groups.items())
