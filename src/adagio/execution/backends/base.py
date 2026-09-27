"""The contract between the coordinator and the places tasks run."""

from __future__ import annotations

import enum
from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from rich.console import Console

    from adagio.executors.base import (
        TaskEnvironmentSpec,
        TaskExecutionRequest,
        TaskExecutionResult,
    )
    from adagio.monitor.api import Monitor

    from ..resources import TaskResourceRequirements


class JobState(str, enum.Enum):
    """A backend-neutral view of where one submitted invocation stands."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (JobState.SUCCEEDED, JobState.FAILED)


@dataclass
class JobHandle:
    """A backend's record of one submitted invocation.

    Backends subclass it for their own bookkeeping; the coordinator reads only
    these fields.
    """

    state: JobState
    job_id: str | None = None
    log_path: Path | None = None


@dataclass(frozen=True)
class TaskInvocation:
    """One execution of a task inside its environment, as a backend receives it."""

    launcher: Any
    environment: TaskEnvironmentSpec
    request: TaskExecutionRequest
    console: Console | None = None
    monitor: Monitor | None = None
    task_id: str | None = None


@dataclass(frozen=True)
class Workspace:
    """Where one run keeps intermediate files, and the directory tasks run from."""

    root: Path
    cwd: Path


class Backend(Protocol):
    """Places task invocations somewhere and reports how they end."""

    kind: ClassVar[str]
    #: True when submitting an invocation starts it (local execution); False
    #: when it may wait in a queue first (a batch scheduler).
    starts_on_submit: ClassVar[bool]
    #: Invocations this run may have submitted and unfinished at once.
    capacity: int

    def check_available(self) -> None:
        """Raise if this host cannot use the backend (checked before any work)."""

    def unsupported_reason(self, environment: TaskEnvironmentSpec) -> str | None:
        """Explain why a task environment cannot run here, or return None."""

    def workspace(self) -> AbstractContextManager[Workspace]:
        """Provide the run's working directory for the duration of the run."""

    def attempt_directory(self, workspace: Workspace) -> Path:
        """Return the directory one task attempt writes its files into."""

    def submit(
        self, invocation: TaskInvocation, resources: TaskResourceRequirements
    ) -> JobHandle:
        """Hand one invocation to the backend."""

    def wait(self, handles: Sequence[JobHandle]) -> None:
        """Block until some handle may have progressed, then refresh their states."""

    def collect(self, handle: JobHandle) -> TaskExecutionResult:
        """Return a succeeded invocation's result; raise its failure otherwise."""

    def cancel(self) -> list[str]:
        """Stop all of this run's outstanding work; describe anything left behind."""

    def describe(self) -> dict[str, Any]:
        """Return the executor settings this backend runs with."""
