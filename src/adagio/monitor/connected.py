from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from .api import Monitor


class ConnectedMonitor(Monitor):
    """Send monitor lifecycle events to the runtime-adapter."""

    def __init__(self, *, runtime_url: str, job_id: str, timeout: float = 5.0):
        base = runtime_url.rstrip("/")
        self._url = f"{base}/jobs/{job_id}/events"
        self._timeout = timeout

    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        self._post(event="pipeline_start", total_tasks=total_tasks)

    def start_load_input(self) -> None:
        self._post(event="load_input_start")

    def finish_load_input(self) -> None:
        self._post(event="load_input_finish")

    def queue_task(
        self, *, task_id: str, label: str, total_subtasks: int = 1
    ) -> None:
        self._post(
            event="task_queued",
            task_id=task_id,
            label=label,
            total_subtasks=total_subtasks,
        )

    def start_task(self, *, task_id: str) -> None:
        self._post(event="task_started", task_id=task_id)

    def advance_task(
        self, *, task_id: str, advance: int = 1, message: str | None = None
    ) -> None:
        payload: dict[str, Any] = {
            "event": "task_progress",
            "task_id": task_id,
            "advance": advance,
        }
        if message:
            payload["message"] = message
        self._post(**payload)

    def finish_task(
        self, *, task_id: str, status: str = "completed", error: str | None = None
    ) -> None:
        payload: dict[str, Any] = {
            "event": "task_finished",
            "task_id": task_id,
            "status": status,
        }
        if error:
            payload["error"] = error
        self._post(**payload)

    def start_save_output(self) -> None:
        self._post(event="save_output_start")

    def finish_output(
        self,
        *,
        output_id: str,
        output_name: str,
        destination: str,
        status: str = "succeeded",
        error: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "event": "output_saved",
            "output_id": output_id,
            "output_name": output_name,
            "destination": destination,
            "status": status,
        }
        if error:
            payload["error"] = error
        self._post(**payload)

    def finish_save_output(self) -> None:
        self._post(event="save_output_finish")

    def finish_pipeline(self) -> None:
        self._post(event="pipeline_finish")

    def _post(self, **payload: Any) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout):
                pass
        except (urllib.error.URLError, TimeoutError):
            # Best-effort telemetry: execution should continue even if the
            # adapter is unavailable.
            return None
