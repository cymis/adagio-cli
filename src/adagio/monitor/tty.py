import re
import threading
import time
from dataclasses import dataclass

from rich.console import Console
from rich.control import Control
from rich.segment import ControlType

from .api import Monitor

BADGE_WIDTH = 8
LABEL_WIDTH = 28
BAR_WIDTH = 28
COUNTER_WIDTH = 5
ELAPSED_WIDTH = 4
ELAPSED_REFRESH_POLL_SECONDS = 0.2
ELAPSED_COLUMN = (
    BADGE_WIDTH + 1 + LABEL_WIDTH + 1 + BAR_WIDTH + 2 + COUNTER_WIDTH + 2
)


@dataclass
class _TaskState:
    task_id: str
    label: str
    total_subtasks: int
    completed_subtasks: int = 0
    status: str = "pending"
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    last_rendered_elapsed_seconds: int | None = None


class RichMonitor(Monitor):
    """Render compact pipeline progress rows."""

    def __init__(self, *, console: Console | None = None):
        """Initialize the Rich monitor."""
        self._console = console or Console()
        self._inline_updates = (
            self._console.is_terminal and not self._console.is_dumb_terminal
        )
        self._task_lookup: dict[str, _TaskState] = {}
        self._task_order: list[str] = []
        self._status_counts: dict[str, int] = {
            "completed": 0,
            "cached": 0,
            "failed": 0,
            "skipped": 0,
        }
        self._lock = threading.RLock()
        self._stop_refresh = threading.Event()
        self._refresh_thread: threading.Thread | None = None
        self._pipeline_started = False
        self._total_tasks = 0

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        """Start rendering pipeline progress."""
        with self._lock:
            if self._pipeline_started:
                return
            self._pipeline_started = True
            self._total_tasks = total_tasks
            self._stop_refresh.clear()
            setattr(self._console, "_adagio_inline_monitor_active", self._inline_updates)
            if self._inline_updates:
                self._console.control(Control.show_cursor(False))
            self._console.print("[bold]Task Progress[/bold]")
            if self._inline_updates:
                self._refresh_thread = threading.Thread(
                    target=self._refresh_loop,
                    name="adagio-rich-monitor",
                    daemon=True,
                )
                self._refresh_thread.start()

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        """Queue a task row in the progress view."""
        with self._lock:
            total = max(total_subtasks, 1)
            state = _TaskState(
                task_id=task_id,
                label=label,
                total_subtasks=total,
            )
            self._task_lookup[task_id] = state
            self._task_order.append(task_id)
            self._print_row(self._render_row(state))

    def start_task(self, *, task_id: str) -> None:
        """Mark a task as running."""
        with self._lock:
            task = self._task_lookup.get(task_id)
            if task is None:
                return
            task.status = "running"
            task.started_at = time.monotonic()
            self._refresh_row(task)

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        """Advance a task's subtask progress."""
        del message
        with self._lock:
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
        with self._lock:
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
        self._stop_refresh.set()
        if self._refresh_thread is not None:
            self._refresh_thread.join(timeout=1.0)
            self._refresh_thread = None
        with self._lock:
            if self._inline_updates:
                self._console.control(Control.show_cursor(True))
            setattr(self._console, "_adagio_inline_monitor_active", False)
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

    def _refresh_row(self, task: _TaskState) -> None:
        """Refresh a rendered task row."""
        task.last_rendered_elapsed_seconds = _elapsed_seconds(task)
        if self._inline_updates:
            self._rewrite_task_row(task)
            return
        self._print_row(self._render_row(task))

    def _refresh_loop(self) -> None:
        """Refresh running task timers once per displayed second."""
        while not self._stop_refresh.wait(ELAPSED_REFRESH_POLL_SECONDS):
            with self._lock:
                self._refresh_running_timers()

    def _refresh_running_timers(self) -> None:
        """Refresh only the elapsed field for running tasks that advanced."""
        for task in self._task_lookup.values():
            if task.status != "running":
                continue
            elapsed_seconds = _elapsed_seconds(task)
            if elapsed_seconds == task.last_rendered_elapsed_seconds:
                continue
            self._rewrite_elapsed(task)
            task.last_rendered_elapsed_seconds = elapsed_seconds

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

    def _print_row(self, row: str) -> None:
        """Print a single task row."""
        self._console.print(
            row,
            markup=True,
            highlight=False,
            no_wrap=True,
            overflow="crop",
        )

    def _rewrite_task_row(self, task: _TaskState) -> None:
        """Rewrite a task row in place without repainting the whole table."""
        distance = self._distance_from_bottom(task)
        self._console.control(
            Control.move_to_column(0, y=-distance),
            Control((ControlType.ERASE_IN_LINE, 2)),
        )
        self._console.print(
            self._render_row(task),
            markup=True,
            highlight=False,
            no_wrap=True,
            overflow="crop",
            end="",
        )
        self._restore_cursor(distance)

    def _rewrite_elapsed(self, task: _TaskState) -> None:
        """Rewrite only the elapsed field for a running task."""
        distance = self._distance_from_bottom(task)
        elapsed = _elapsed(task)
        padded = elapsed.rjust(max(ELAPSED_WIDTH, len(elapsed)))
        self._console.control(Control.move_to_column(ELAPSED_COLUMN, y=-distance))
        self._console.out(padded, end="")
        self._restore_cursor(distance)

    def _restore_cursor(self, distance: int) -> None:
        """Return the cursor to the stable line below the task list."""
        self._console.control(Control.move_to_column(0, y=distance))

    def _distance_from_bottom(self, task: _TaskState) -> int:
        """Return the cursor distance from the footer line to a task row."""
        row_index = self._task_order.index(task.task_id)
        return len(self._task_order) - row_index


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
