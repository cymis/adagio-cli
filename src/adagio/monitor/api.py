from typing import Any


class Monitor:
    """Define monitor hooks used by pipeline execution."""

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        """Start tracking a pipeline run."""
        return None

    def report_reproducibility(self, *, reproducibility: dict[str, Any]) -> None:
        """Report a run-level reproducibility header (adagio version, digests…).

        Optional/best-effort; default monitors ignore it. Emitted once near the
        start of a run (design §5.1).
        """
        return None

    def pulling_image(
        self, *, task_id: str, image_ref: str | None = None
    ) -> None:
        """Signal that a task's container image is being pulled (fine phase)."""
        return None

    def starting_container(
        self, *, task_id: str, image_ref: str | None = None
    ) -> None:
        """Signal that a task's container is starting (fine phase)."""
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

    def start_task(self, *, task_id: str, **details: Any) -> None:
        """Start tracking an individual task.

        ``details`` carries optional enrichment (e.g. ``input_signature``,
        ``image_ref``) that structured monitors relay; console monitors ignore it.
        """
        return None

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        """Advance progress for an individual task."""
        return None

    def finish_task(
        self,
        *,
        task_id: str,
        status: str = "completed",
        error: str | None = None,
        **details: Any,
    ) -> None:
        """Finish tracking an individual task.

        ``details`` carries optional enrichment (``command``, ``exit_code``,
        ``image_ref``, ``image_digest``, ``reused``, ``timings``,
        ``input_signature``, ``log_path``…) relayed by structured monitors.
        """
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
        **details: Any,
    ) -> None:
        """Track completion for an individual output artifact.

        ``details`` carries optional enrichment (e.g. ``log_path``) relayed by
        structured monitors.
        """
        return None

    def finish_save_output(self) -> None:
        """Finish tracking output saving."""
        return None

    def finish_pipeline(self) -> None:
        """Finish tracking a pipeline run."""
        return None
