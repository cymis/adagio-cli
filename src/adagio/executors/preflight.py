"""Lightweight runtime checks at the task launch boundary."""

from .base import TaskEnvironmentLauncher, TaskEnvironmentSpec


class RuntimePreflightError(RuntimeError):
    """A task cannot start because its runtime is unavailable."""


def preflight_environment(
    launcher: TaskEnvironmentLauncher,
    *,
    environment: TaskEnvironmentSpec,
    task_id: str,
) -> None:
    # Older third-party launchers only implement launch(). Built-ins register
    # their check through the same launcher registry used for execution.
    check = getattr(launcher, "preflight", None)
    if check is None:
        return
    try:
        check(environment=environment)
    except (RuntimeError, OSError, SystemExit) as exc:
        # Existing executable resolvers use SystemExit. Availability failures
        # must be reported as failed tasks, not user cancellations.
        raise RuntimePreflightError(
            f"Cannot start node {task_id!r}: {environment.kind} preflight failed. {exc}"
        ) from exc
