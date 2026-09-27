"""Run each task on this host through its environment launcher, one at a time."""

from __future__ import annotations

import inspect
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict

from adagio.executors.task_contract import container_log_path

from .base import JobHandle, JobState, TaskInvocation, Workspace

if TYPE_CHECKING:
    from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult

    from ..resources import TaskResourceRequirements


class LocalExecutorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["local"] = "local"


@dataclass
class LocalHandle(JobHandle):
    result: TaskExecutionResult | None = None
    error: BaseException | None = None


class LocalBackend:
    """Run tasks in this process, in submission order, in any environment kind.

    Submitting runs the invocation to completion, so a failure (including a
    launcher's ``SystemExit`` for a missing runtime) is reported through the
    handle like any other backend's. Interrupts propagate immediately.
    """

    kind = "local"
    starts_on_submit = True
    capacity = 1

    def __init__(self, config: LocalExecutorConfig | None = None) -> None:
        self.config = config or LocalExecutorConfig()

    def check_available(self) -> None:
        return None

    def unsupported_reason(self, environment: TaskEnvironmentSpec) -> str | None:
        return None

    @contextmanager
    def workspace(self) -> Iterator[Workspace]:
        with tempfile.TemporaryDirectory(prefix="adagio-work-") as root:
            yield Workspace(root=Path(root), cwd=Path.cwd().resolve())

    def attempt_directory(self, workspace: Workspace) -> Path:
        return workspace.root

    def submit(
        self, invocation: TaskInvocation, resources: TaskResourceRequirements
    ) -> LocalHandle:
        log_path = container_log_path(
            task_id=invocation.request.task.id, work_path=invocation.request.work_path
        )
        try:
            result = launch(invocation)
        except (Exception, SystemExit) as error:
            return LocalHandle(state=JobState.FAILED, log_path=log_path, error=error)
        return LocalHandle(state=JobState.SUCCEEDED, log_path=log_path, result=result)

    def wait(self, handles: Sequence[JobHandle]) -> None:
        return None

    def collect(self, handle: LocalHandle) -> TaskExecutionResult:
        if handle.error is not None:
            raise handle.error
        return handle.result

    def cancel(self) -> list[str]:
        return []

    def describe(self) -> dict[str, Any]:
        return self.config.model_dump()


def launch(invocation: TaskInvocation) -> TaskExecutionResult:
    """Call ``launcher.launch`` passing only the keyword arguments it accepts.

    The launcher protocol gained optional ``monitor`` / ``task_id`` parameters
    for fine-phase telemetry. Third-party launchers (and test doubles) written
    against the older signature do not accept them; filter to the callable's
    real parameters so those keep working unchanged.
    """
    launcher = invocation.launcher
    kwargs: dict[str, Any] = {
        "environment": invocation.environment,
        "request": invocation.request,
        "console": invocation.console,
    }
    if invocation.monitor is not None:
        kwargs["monitor"] = invocation.monitor
    if invocation.task_id is not None:
        kwargs["task_id"] = invocation.task_id
    try:
        signature = inspect.signature(launcher.launch)
    except (TypeError, ValueError):
        return launcher.launch(**kwargs)
    params = signature.parameters
    accepts_var_kw = any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    if accepts_var_kw:
        return launcher.launch(**kwargs)
    accepted = {name: value for name, value in kwargs.items() if name in params}
    return launcher.launch(**accepted)
