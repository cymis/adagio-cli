from rich.console import Console

from .api import Monitor


class LogMonitor(Monitor):
    """Log monitor events to a Rich console."""

    def __init__(self, *, console: Console | None = None):
        """Initialize the log monitor."""
        self._console = console or Console(stderr=True)

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        """Log pipeline start."""
        self._console.log(f"pipeline started (tasks={total_tasks})")

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        """Log task queueing."""
        self._console.log(
            f"queued task id={task_id} label={label!r} subtasks={total_subtasks}"
        )

    def start_task(self, *, task_id: str) -> None:
        """Log task start."""
        self._console.log(f"started task id={task_id}")

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        """Log task progress updates."""
        details = f" advanced={advance}"
        if message:
            details += f" message={message!r}"
        self._console.log(f"updated task id={task_id}{details}")

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        """Log task completion."""
        details = f"status={status}"
        if error:
            details += f" error={error!r}"
        self._console.log(f"finished task id={task_id} {details}")

    def start_save_output(self) -> None:
        """Log output-save start."""
        self._console.log("saving outputs")

    def finish_output(
        self,
        *,
        output_id: str,
        output_name: str,
        destination: str,
        status: str = "succeeded",
        error: str | None = None,
    ) -> None:
        """Log completion of an individual output."""
        details = f"status={status} id={output_id} name={output_name!r} destination={destination!r}"
        if error:
            details += f" error={error!r}"
        self._console.log(f"saved output {details}")

    def finish_save_output(self) -> None:
        """Log output-save completion."""
        self._console.log("finished saving outputs")

    def finish_pipeline(self) -> None:
        """Log pipeline completion."""
        self._console.log("pipeline finished")
