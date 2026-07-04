import json
import os
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Any

from .api import Monitor

# Bounded in-memory buffer of events that failed to POST. A transient loopback
# failure (the adapter briefly down / restarting) must not permanently drop
# telemetry, but the buffer is capped so a long outage can never grow unbounded.
_DEFAULT_BUFFER_LIMIT = 256
_DEFAULT_MAX_RETRIES = 2


class ConnectedMonitor(Monitor):
    """Send monitor lifecycle events to the runtime-adapter loopback sink.

    Transport (design §5.2): best-effort, but hardened over the original
    fire-and-forget POST:

    * Optional ``Authorization: Bearer <token>`` when ``RUNTIME_TOKEN`` is set in
      the environment (CLI -> adapter loopback auth; the adapter owns action
      auth). An explicit ``token`` argument overrides the env var.
    * Bounded retry per event plus a bounded FIFO buffer, so a transient
      loopback failure re-drains on the next successful post instead of dropping
      events. Never raises: execution continues even if the adapter is down.

    Events are the superset of the original connected events, enriched with the
    fields the adapter relays to ``action`` (§5.1): ``pulling_image`` /
    ``starting_container`` lifecycle, ``command`` / ``exit_code`` /
    ``image_ref`` / ``image_digest`` / ``reused`` / ``timings`` /
    ``input_signature`` on task/output events, and a run-level ``reproducibility``
    header.
    """

    def __init__(
        self,
        *,
        runtime_url: str,
        job_id: str,
        timeout: float = 5.0,
        token: str | None = None,
        buffer_limit: int = _DEFAULT_BUFFER_LIMIT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
    ):
        base = runtime_url.rstrip("/")
        self._url = f"{base}/jobs/{job_id}/events"
        self._timeout = timeout
        self._token = token if token is not None else os.getenv("RUNTIME_TOKEN")
        self._buffer: deque[dict[str, Any]] = deque(maxlen=max(buffer_limit, 1))
        self._max_retries = max(max_retries, 0)

    # -- lifecycle events -----------------------------------------------------
    def start_pipeline(self, *, total_tasks: int = 0) -> None:
        self._post(event="pipeline_start", total_tasks=total_tasks)

    def report_reproducibility(self, *, reproducibility: dict[str, Any]) -> None:
        self._post(event="reproducibility", reproducibility=reproducibility)

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

    def pulling_image(self, *, task_id: str, image_ref: str | None = None) -> None:
        payload: dict[str, Any] = {"event": "pulling_image", "task_id": task_id}
        if image_ref is not None:
            payload["image_ref"] = image_ref
        self._post(**payload)

    def starting_container(
        self, *, task_id: str, image_ref: str | None = None
    ) -> None:
        payload: dict[str, Any] = {"event": "starting_container", "task_id": task_id}
        if image_ref is not None:
            payload["image_ref"] = image_ref
        self._post(**payload)

    def start_task(self, *, task_id: str, **details: Any) -> None:
        payload: dict[str, Any] = {"event": "task_started", "task_id": task_id}
        _merge_enrichment(payload, details)
        self._post(**payload)

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
        self,
        *,
        task_id: str,
        status: str = "completed",
        error: str | None = None,
        **details: Any,
    ) -> None:
        payload: dict[str, Any] = {
            "event": "task_finished",
            "task_id": task_id,
            "status": status,
        }
        if error:
            payload["error"] = error
        _merge_enrichment(payload, details)
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
        **details: Any,
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
        _merge_enrichment(payload, details)
        self._post(**payload)

    def finish_save_output(self) -> None:
        self._post(event="save_output_finish")

    def finish_pipeline(self) -> None:
        self._post(event="pipeline_finish")

    # -- transport ------------------------------------------------------------
    def _post(self, **payload: Any) -> None:
        # Drain any previously-buffered events first so ordering is preserved as
        # best it can be, then send the new one.
        self._drain_buffer()
        if not self._send(payload):
            self._buffer.append(payload)

    def _drain_buffer(self) -> None:
        while self._buffer:
            pending = self._buffer[0]
            if not self._send(pending):
                # Still unreachable; stop draining and keep the buffer intact.
                return
            self._buffer.popleft()

    def _send(self, payload: dict[str, Any]) -> bool:
        """POST one event with bounded retries. Returns True on success."""
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        for attempt in range(self._max_retries + 1):
            req = urllib.request.Request(
                self._url,
                data=data,
                method="POST",
                headers=headers,
            )
            try:
                with urllib.request.urlopen(req, timeout=self._timeout):
                    return True
            except urllib.error.HTTPError:
                # A 4xx/5xx is a server-side decision (bad payload / auth); no
                # amount of retrying changes it. Treat as "delivered" so the
                # buffer does not wedge on a permanently-rejected event.
                return True
            except (urllib.error.URLError, TimeoutError):
                if attempt < self._max_retries:
                    # Small linear backoff between attempts.
                    time.sleep(min(0.2 * (attempt + 1), 1.0))
                    continue
                return False
        return False


def _merge_enrichment(payload: dict[str, Any], details: dict[str, Any]) -> None:
    """Fold non-None enrichment fields into an event payload (excluding keys the
    event already owns)."""
    for key, value in details.items():
        if value is None:
            continue
        payload.setdefault(key, value)
