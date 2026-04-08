import io
import unittest
from unittest.mock import patch

from rich.console import Console

from adagio.monitor.tty import RichMonitor, _TaskState, _elapsed


class RichMonitorTests(unittest.TestCase):
    def test_progress_auto_refresh_is_disabled(self) -> None:
        monitor = RichMonitor(console=Console(file=io.StringIO()))

        self.assertFalse(monitor._inline_updates)

    def test_running_task_elapsed_uses_current_second(self) -> None:
        task = _TaskState(
            task_id="task-1",
            label="demo",
            total_subtasks=1,
            status="running",
            started_at=10.0,
        )

        with patch("adagio.monitor.tty.time.monotonic", return_value=18.9):
            self.assertEqual(_elapsed(task), "0:08")

    def test_finished_task_elapsed_uses_duration(self) -> None:
        task = _TaskState(
            task_id="task-1",
            label="demo",
            total_subtasks=1,
            status="completed",
            started_at=10.0,
            finished_at=75.0,
        )

        self.assertEqual(_elapsed(task), "1:05")

    def test_refresh_running_rows_skips_same_elapsed_bucket(self) -> None:
        monitor = RichMonitor(console=Console(file=io.StringIO()))
        monitor._task_lookup["task-1"] = _TaskState(
            task_id="task-1",
            label="demo",
            total_subtasks=1,
            status="running",
            started_at=10.0,
            last_rendered_elapsed_seconds=9,
        )
        monitor._task_order.append("task-1")

        with patch.object(monitor, "_rewrite_elapsed") as rewrite_elapsed:
            with patch("adagio.monitor.tty.time.monotonic", return_value=19.9):
                monitor._refresh_running_timers()

        rewrite_elapsed.assert_not_called()

    def test_refresh_running_rows_updates_on_new_elapsed_second(self) -> None:
        monitor = RichMonitor(console=Console(file=io.StringIO()))
        task = _TaskState(
            task_id="task-1",
            label="demo",
            total_subtasks=1,
            status="running",
            started_at=10.0,
            last_rendered_elapsed_seconds=9,
        )
        monitor._task_lookup["task-1"] = task
        monitor._task_order.append("task-1")

        with patch.object(monitor, "_rewrite_elapsed") as rewrite_elapsed:
            with patch("adagio.monitor.tty.time.monotonic", return_value=20.0):
                monitor._refresh_running_timers()

        rewrite_elapsed.assert_called_once_with(task)
