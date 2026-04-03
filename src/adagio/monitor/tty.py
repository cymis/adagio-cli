import re
import time
from dataclasses import dataclass

from rich.console import Console
from rich.progress import Progress, TextColumn

from .api import Monitor

BADGE_WIDTH = 8
LABEL_WIDTH = 28
BAR_WIDTH = 28
COUNTER_WIDTH = 5
ELAPSED_WIDTH = 4


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
    """Render compact pipeline progress rows."""

    def __init__(self, *, console: Console | None = None):
        """Initialize the Rich monitor."""
        self._console = console or Console()
        self._progress = Progress(
            TextColumn("{task.fields[row]}"),
            console=self._console,
            auto_refresh=False,
            expand=True,
            transient=False,
        )
        self._task_lookup: dict[str, _TaskState] = {}
        self._status_counts: dict[str, int] = {
            "completed": 0,
            "cached": 0,
            "failed": 0,
            "skipped": 0,
        }
        self._pipeline_started = False
        self._total_tasks = 0

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        """Start rendering pipeline progress."""
        if self._pipeline_started:
            return
        self._pipeline_started = True
        self._total_tasks = total_tasks
        self._console.print("[bold]Task Progress[/bold]")
        self._progress.start()

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        """Queue a task row in the progress view."""
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
        """Mark a task as running."""
        task = self._task_lookup.get(task_id)
        if task is None:
            return
        task.status = "running"
        task.started_at = time.monotonic()
        self._refresh_row(task, refresh=False)
        self._progress.refresh()

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        """Advance a task's subtask progress."""
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
        """Mark a task as finished."""
        task = self._task_lookup.get(task_id)
        if task is None:
            return

        task.status = status
        task.error = error
        task.finished_at = time.monotonic()
        if status in {"completed", "cached", "skipped"}:
            task.completed_subtasks = task.total_subtasks
        if status in self._status_counts:
            self._status_counts[status] += 1
        self._refresh_row(task)

    def finish_pipeline(self) -> None:
        """Stop rendering and print a summary."""
        if not self._pipeline_started:
            return
        self._progress.stop()
        pending = self._total_tasks - sum(self._status_counts.values())
        self._console.print(
            "Summary: "
            f"{self._status_counts['completed']} completed, "
            f"{self._status_counts['cached']} cached, "
            f"{self._status_counts['failed']} failed, "
            f"{self._status_counts['skipped']} skipped, "
            f"{max(pending, 0)} pending"
        )
        self._pipeline_started = False

    def _refresh_row(self, task: _TaskState, *, refresh: bool = True) -> None:
        """Refresh a rendered task row."""
        self._progress.update(
            task.progress_task_id,
            completed=task.completed_subtasks,
            row=self._render_row(task),
        )
        if refresh:
            self._progress.refresh()

    def _render_row(self, task: _TaskState) -> str:
        """Build a compact row for a task."""
        badge_text, color = _status_style(task.status)
        badge_plain = badge_text.ljust(BADGE_WIDTH)
        badge = f"[bold {color}]{badge_plain}[/]"
        label = _compact_label(task.label, LABEL_WIDTH).ljust(LABEL_WIDTH)
        bar = _bar_text(task.completed_subtasks, task.total_subtasks, color, BAR_WIDTH)
        counter = f"{task.completed_subtasks}/{task.total_subtasks}"
        elapsed = _elapsed(task)
        error = ""
        if task.status == "failed" and task.error:
            error = f"  [red]{task.error}[/]"
        return (
            f"{badge} {label} {bar}  "
            f"{counter.rjust(COUNTER_WIDTH)}  {elapsed.rjust(ELAPSED_WIDTH)}{error}"
        )


def _status_style(status: str) -> tuple[str, str]:
    """Map task state to badge text and color."""
    lookup = {
        "pending": ("PENDING", "yellow"),
        "running": ("RUNNING", "cyan"),
        "completed": ("DONE", "green"),
        "cached": ("CACHED", "blue"),
        "failed": ("FAILED", "red"),
        "skipped": ("SKIPPED", "magenta"),
    }
    return lookup.get(status, ("PENDING", "yellow"))


def _compact_label(label: str, width: int = 28) -> str:
    """Trim task labels to a compact display name."""
    match = re.search(r"\(([^)]+)\)\s*$", label)
    compact = match.group(1) if match else label
    if len(compact) <= width:
        return compact
    return compact[: width - 1] + "…"


def _bar_text(completed: int, total: int, color: str, width: int = 28) -> str:
    """Build a colored progress bar string."""
    if total <= 0:
        total = 1
    ratio = min(max(completed / total, 0.0), 1.0)
    filled = int(round(ratio * width))
    empty = width - filled
    return f"[{color}]{'━' * filled}[/][dim]{'─' * empty}[/]"


def _elapsed(task: _TaskState) -> str:
    """Format elapsed task time as M:SS."""
    if task.started_at is not None and task.finished_at is None:
        return "..."
    seconds = _elapsed_seconds(task)
    minutes, sec = divmod(seconds, 60)
    return f"{minutes}:{sec:02d}"


def _elapsed_seconds(task: _TaskState) -> int:
    """Return elapsed task time in whole seconds."""
    start = task.started_at
    if start is None:
        return 0
    if task.finished_at is not None:
        return max(0, int(task.finished_at - start))
    return max(0, int(time.monotonic() - start))
