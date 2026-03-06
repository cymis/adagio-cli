import time
from typing import Any

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor


SLEEP_SECONDS = 5.0
SUBTASK_COUNT = 3


def execute(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    monitor: Monitor | None = None,
) -> None:
    """Execute a pipeline with fixed dummy progress."""
    sig = pipeline.signature
    monitor = monitor or LogMonitor()
    tasks = list(pipeline.iter_tasks())

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    subtasks = SUBTASK_COUNT
    sleep_per_subtask = SLEEP_SECONDS / SUBTASK_COUNT

    monitor.start_pipeline(total_tasks=len(tasks))
    try:
        for task in tasks:
            monitor.queue_task(
                task_id=task.id,
                label=_task_label(task),
                total_subtasks=subtasks,
            )

        for task in tasks:
            monitor.start_task(task_id=task.id)
            for _ in range(subtasks):
                time.sleep(sleep_per_subtask)
                monitor.advance_task(task_id=task.id, advance=1)

            monitor.finish_task(task_id=task.id, status="completed")
    finally:
        monitor.finish_pipeline()


def _task_label(task: Any) -> str:
    """Build a human-readable label for a task."""
    kind = getattr(task, "kind", "unknown")
    task_id = getattr(task, "id", "<unknown>")
    if kind == "plugin-action":
        plugin = getattr(task, "plugin", "<plugin>")
        action = getattr(task, "action", "<action>")
        return f"{task_id} ({plugin}.{action})"
    if kind == "built-in":
        name = getattr(task, "name", "built-in")
        return f"{task_id} ({name})"
    return task_id
