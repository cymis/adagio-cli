class Monitor:
    """Define monitor hooks used by pipeline execution."""

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        """Start tracking a pipeline run."""
        return None

    def start_load_input(self) -> None:
        """Start tracking input loading."""
        return None

    def finish_load_input(self) -> None:
        """Finish tracking input loading."""
        return None

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        """Queue a task before execution starts."""
        return None

    def start_task(self, *, task_id: str) -> None:
        """Start tracking an individual task."""
        return None

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        """Advance progress for an individual task."""
        return None

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        """Finish tracking an individual task."""
        return None

    def start_save_output(self) -> None:
        """Start tracking output saving."""
        return None

    def finish_output(
        self,
        *,
        output_id: str,
        output_name: str,
        destination: str,
        status: str = "succeeded",
        error: str | None = None,
    ) -> None:
        """Track completion for an individual output artifact."""
        return None

    def finish_save_output(self) -> None:
        """Finish tracking output saving."""
        return None

    def finish_pipeline(self) -> None:
        """Finish tracking a pipeline run."""
        return None
