from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor


@dataclass
class DummyExecutionConfig:
    min_seconds: float = 10.0
    max_seconds: float = 15.0
    fail_rate: float = 0.0
    subtasks: int = 3
    seed: int | None = None


class DummyExecutionFailed(RuntimeError):
    pass


def execute_dummy_pipeline(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    monitor: Monitor | None = None,
    dummy: DummyExecutionConfig | None = None,
) -> None:
    sig = pipeline.signature
    monitor = monitor or LogMonitor()
    dummy = dummy or DummyExecutionConfig()
    tasks = list(pipeline.iter_tasks())

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    subtasks = max(dummy.subtasks, 1)
    fail_rate = min(max(dummy.fail_rate, 0.0), 1.0)
    min_seconds, max_seconds = sorted(
        (max(dummy.min_seconds, 0.0), max(dummy.max_seconds, 0.0))
    )
    rng = random.Random(dummy.seed)

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
            duration = rng.uniform(min_seconds, max_seconds)
            sleep_per_subtask = duration / subtasks

            for subtask_index in range(subtasks):
                if sleep_per_subtask > 0:
                    time.sleep(sleep_per_subtask)
                monitor.advance_task(
                    task_id=task.id,
                    advance=1,
                )

            if rng.random() < fail_rate:
                monitor.finish_task(
                    task_id=task.id,
                    status="failed",
                    error="simulated failure",
                )
                raise DummyExecutionFailed(
                    f"Dummy execution failed at task '{task.id}'."
                )

            monitor.finish_task(task_id=task.id, status="completed")
    finally:
        monitor.finish_pipeline()


def _task_label(task: Any) -> str:
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
