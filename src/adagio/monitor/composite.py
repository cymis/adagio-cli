from typing import Any

from .api import Monitor


class CompositeMonitor(Monitor):
    """Fan out monitor hooks to multiple monitor instances."""

    def __init__(self, *monitors: Monitor):
        self._monitors = tuple(monitors)

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        for monitor in self._monitors:
            monitor.start_pipeline(total_tasks=total_tasks)

    def report_reproducibility(self, *, reproducibility: dict[str, Any]) -> None:
        for monitor in self._monitors:
            monitor.report_reproducibility(reproducibility=reproducibility)

    def pulling_image(self, *, task_id: str, image_ref: str | None = None) -> None:
        for monitor in self._monitors:
            monitor.pulling_image(task_id=task_id, image_ref=image_ref)

    def starting_container(
        self, *, task_id: str, image_ref: str | None = None
    ) -> None:
        for monitor in self._monitors:
            monitor.starting_container(task_id=task_id, image_ref=image_ref)

    def start_load_input(self) -> None:
        for monitor in self._monitors:
            monitor.start_load_input()

    def finish_load_input(self) -> None:
        for monitor in self._monitors:
            monitor.finish_load_input()

    def queue_task(self, *, task_id: str, label: str, total_subtasks: int = 1) -> None:
        for monitor in self._monitors:
            monitor.queue_task(
                task_id=task_id,
                label=label,
                total_subtasks=total_subtasks,
            )

    def start_task(self, *, task_id: str, **details: Any) -> None:
        for monitor in self._monitors:
            monitor.start_task(task_id=task_id, **details)

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        for monitor in self._monitors:
            monitor.advance_task(task_id=task_id, advance=advance, message=message)

    def finish_task(
        self,
        *,
        task_id: str,
        status: str = "completed",
        error: str | None = None,
        **details: Any,
    ) -> None:
        for monitor in self._monitors:
            monitor.finish_task(
                task_id=task_id, status=status, error=error, **details
            )

    def start_save_output(self) -> None:
        for monitor in self._monitors:
            monitor.start_save_output()

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
        for monitor in self._monitors:
            monitor.finish_output(
                output_id=output_id,
                output_name=output_name,
                destination=destination,
                status=status,
                error=error,
                **details,
            )

    def finish_save_output(self) -> None:
        for monitor in self._monitors:
            monitor.finish_save_output()

    def finish_pipeline(self) -> None:
        for monitor in self._monitors:
            monitor.finish_pipeline()
