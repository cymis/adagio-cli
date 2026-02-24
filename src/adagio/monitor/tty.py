from __future__ import annotations

import time
from dataclasses import dataclass

from rich.console import Console
from rich.progress import Progress, TextColumn

from .api import Monitor


@dataclass
class _TaskState:
    progress_task_id: int
    label: str
    total_subtasks: int
    completed_subtasks: int = 0
    status: str = "pending"
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None


class RichMonitor(Monitor):
    def __init__(self, *, console: Console | None = None):
        self._console = console or Console()
        self._progress = Progress(
            TextColumn("{task.fields[row]}"),
            console=self._console,
            expand=True,
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
        state = _TaskState(
            progress_task_id=-1,
            label=label,
            total_subtasks=total,
        )
        row = self._render_row(state)
        progress_task_id = self._progress.add_task(
            description="",
            total=total,
            completed=0,
            row=row,
        )
        state.progress_task_id = progress_task_id
        self._task_lookup[task_id] = state

    def start_task(self, *, task_id: str) -> None:
        task = self._task_lookup.get(task_id)
        if task is None:
            return
        task.status = "running"
        task.started_at = time.monotonic()
        self._refresh_row(task)

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        del message
        task = self._task_lookup.get(task_id)
        if task is None:
            return
        task.completed_subtasks = min(
            task.total_subtasks, task.completed_subtasks + max(advance, 0)
        )
        self._refresh_row(task)

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        task = self._task_lookup.get(task_id)
        if task is None:
            return

        task.status = status
        task.error = error
        task.finished_at = time.monotonic()
        if status in {"completed", "skipped"}:
            task.completed_subtasks = task.total_subtasks
        if status in self._status_counts:
            self._status_counts[status] += 1
        self._refresh_row(task)

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

    def _refresh_row(self, task: _TaskState) -> None:
        self._progress.update(
            task.progress_task_id,
            completed=task.completed_subtasks,
            row=self._render_row(task),
        )

    def _render_row(self, task: _TaskState) -> str:
        status_styles = {
            "pending": ("PEND", "yellow"),
            "running": ("RUN", "cyan"),
            "completed": ("DONE", "green"),
            "failed": ("FAIL", "red"),
            "skipped": ("SKIP", "magenta"),
        }
        badge_text, color = status_styles.get(task.status, ("PEND", "yellow"))
        badge = f"[bold {color}]{badge_text}[/]"
        bar = _bar_text(task.completed_subtasks, task.total_subtasks, color)

        if task.status == "completed":
            state_text = f"completed ({task.completed_subtasks}/{task.total_subtasks})"
        elif task.status == "failed":
            state_text = "failed"
            if task.error:
                state_text = f"{state_text}: {task.error}"
        elif task.status == "skipped":
            state_text = f"skipped ({task.completed_subtasks}/{task.total_subtasks})"
        elif task.status == "running":
            state_text = f"running ({task.completed_subtasks}/{task.total_subtasks})"
        else:
            state_text = f"pending ({task.completed_subtasks}/{task.total_subtasks})"

        elapsed = _elapsed(task)
        return (
            f"{badge} {task.label}\n"
            f" {bar}   {state_text}   {elapsed}"
        )


def _bar_text(completed: int, total: int, color: str, width: int = 40) -> str:
    if total <= 0:
        total = 1
    ratio = min(max(completed / total, 0.0), 1.0)
    filled = int(round(ratio * width))
    empty = width - filled
    return f"[{color}]{'━' * filled}[/]{' ' * empty}"


def _elapsed(task: _TaskState) -> str:
    start = task.started_at
    if start is None:
        seconds = 0
    elif task.finished_at is not None:
        seconds = max(0, int(task.finished_at - start))
    else:
        seconds = max(0, int(time.monotonic() - start))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{sec:02d}"
