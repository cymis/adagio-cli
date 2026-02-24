from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from .api import Monitor


@dataclass
class _TaskState:
    progress_task_id: int
    total_subtasks: int
    completed_subtasks: int = 0
    status: str = "pending"


class RichMonitor(Monitor):
    def __init__(self, *, console: Console | None = None):
        self._console = console or Console()
        self._progress = Progress(
            TextColumn("{task.fields[badge]} {task.fields[label]}\n"),
            TextColumn(" "),
            BarColumn(bar_width=40),
            TextColumn("{task.fields[state]}"),
            TimeElapsedColumn(),
            console=self._console,
            expand=False,
            transient=False,
        )
        self._task_lookup: dict[str, _TaskState] = {}
        self._status_counts: dict[str, int] = {
            "completed": 0,
            "failed": 0,
            "skipped": 0,
        }
        self._pipeline_started = False
        self._total_tasks = 0

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        if self._pipeline_started:
            return
        self._pipeline_started = True
        self._total_tasks = total_tasks
        self._progress.start()
        self._console.print("[bold]Task Checklist[/bold]")

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        total = max(total_subtasks, 1)
        progress_task_id = self._progress.add_task(
            description="",
            total=total,
            completed=0,
            badge="PEND",
            label=label,
            state=f"pending (0/{total})",
        )
        self._task_lookup[task_id] = _TaskState(
            progress_task_id=progress_task_id,
            total_subtasks=total,
            completed_subtasks=0,
            status="pending",
        )

    def start_task(self, *, task_id: str) -> None:
        task = self._task_lookup.get(task_id)
        if task is None:
            return
        task.status = "running"
        self._progress.update(
            task.progress_task_id,
            badge="RUN",
            state=f"running ({task.completed_subtasks}/{task.total_subtasks})",
        )

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        task = self._task_lookup.get(task_id)
        if task is None:
            return
        task.completed_subtasks = min(
            task.total_subtasks, task.completed_subtasks + max(advance, 0)
        )
        state = f"running ({task.completed_subtasks}/{task.total_subtasks})"
        if message:
            state = f"{state} {message}"
        self._progress.update(
            task.progress_task_id,
            completed=task.completed_subtasks,
            state=state,
        )

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        task = self._task_lookup.get(task_id)
        if task is None:
            return

        task.status = status
        badge_lookup = {
            "completed": "DONE",
            "failed": "FAIL",
            "skipped": "SKIP",
        }
        if status == "completed":
            task.completed_subtasks = task.total_subtasks
            state = f"completed ({task.completed_subtasks}/{task.total_subtasks})"
        elif status == "failed":
            state = "failed"
            if error:
                state = f"{state}: {error}"
        elif status == "skipped":
            task.completed_subtasks = task.total_subtasks
            state = f"skipped ({task.completed_subtasks}/{task.total_subtasks})"
        else:
            state = status

        if status in self._status_counts:
            self._status_counts[status] += 1

        self._progress.update(
            task.progress_task_id,
            completed=task.completed_subtasks,
            badge=badge_lookup.get(status, "PEND"),
            state=state,
        )

    def finish_pipeline(self) -> None:
        if not self._pipeline_started:
            return
        self._progress.stop()
        pending = self._total_tasks - sum(self._status_counts.values())
        self._console.print(
            "Summary: "
            f"{self._status_counts['completed']} completed, "
            f"{self._status_counts['failed']} failed, "
            f"{self._status_counts['skipped']} skipped, "
            f"{max(pending, 0)} pending"
        )
