from __future__ import annotations


class Monitor:
    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        return None

    def start_load_input(self) -> None:
        return None

    def finish_load_input(self) -> None:
        return None

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        return None

    def start_task(self, *, task_id: str) -> None:
        return None

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        return None

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        return None

    def start_save_output(self) -> None:
        return None

    def finish_save_output(self) -> None:
        return None

    def finish_pipeline(self) -> None:
        return None
