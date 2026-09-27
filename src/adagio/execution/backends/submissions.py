"""The durable record of every job one run handed to a batch scheduler.

Each submission is recorded before the scheduler is asked, so a crash between
the request and its reply leaves an entry to reconcile instead of an untracked
job. Cancellation, and cleanup after an abnormal exit, act on this record and
never on the scheduler's wider view of what the user owns.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from adagio.executors.task_contract import read_json_file, write_json_file

from .base import JobState

REGISTRY_VERSION = 1
#: Recorded before the scheduler replies; the job may or may not exist.
SUBMITTING = "submitting"
#: The scheduler's reply never confirmed the job; it is never retried.
UNCONFIRMED = "unconfirmed"
#: The scheduler explicitly refused the submission, so no job exists.
REJECTED = "rejected"
#: The scheduler stopped accounting for the job; it may still be running.
UNKNOWN = "unknown"
#: The scheduler no longer holds the job, or never created it; how it ended
#: is not known, but it holds no resources.
ENDED = "ended"


class SubmissionRegistry:
    FILENAME = "submissions.json"

    def __init__(self, path: Path, data: dict[str, Any]) -> None:
        self.path = path
        self._data = data

    @classmethod
    def create(cls, run_dir: Path, *, executor: str) -> SubmissionRegistry:
        registry = cls(
            run_dir / cls.FILENAME,
            {
                "version": REGISTRY_VERSION,
                "executor": executor,
                "run_dir": str(run_dir),
                "attempts": {},
            },
        )
        registry.save()
        return registry

    @classmethod
    def load(cls, path: Path) -> SubmissionRegistry:
        data = read_json_file(path)
        if (
            not isinstance(data, dict)
            or data.get("version") != REGISTRY_VERSION
            or not isinstance(data.get("attempts"), dict)
        ):
            raise ValueError(f"Unrecognized submission registry: {path}.")
        return cls(path, data)

    @property
    def executor(self) -> str:
        return self._data["executor"]

    def save(self) -> None:
        write_json_file(self.path, self._data)

    def record_intent(self, attempt_id: str, **entry: Any) -> None:
        self._data["attempts"][attempt_id] = {
            **entry,
            "state": SUBMITTING,
            "job_id": None,
            "cluster": None,
            "intent_at": time.time(),
        }
        self.save()

    def record_submitted(
        self, attempt_id: str, *, job_id: str, cluster: str | None
    ) -> None:
        self._data["attempts"][attempt_id].update(
            state=JobState.QUEUED.value, job_id=job_id, cluster=cluster
        )
        self.save()

    def record_unconfirmed(self, attempt_id: str, error: str) -> None:
        self._data["attempts"][attempt_id].update(state=UNCONFIRMED, error=error)
        self.save()

    def record_rejected(self, attempt_id: str, error: str) -> None:
        self._data["attempts"][attempt_id].update(state=REJECTED, error=error)
        self.save()

    def record_unknown(self, attempt_id: str, reason: str) -> None:
        """Keep a job the scheduler lost track of outstanding, so it is cancelled."""
        self._data["attempts"][attempt_id].update(
            state=UNKNOWN, scheduler_state="UNKNOWN", error=reason
        )

    def record_status(
        self,
        attempt_id: str,
        *,
        state: JobState,
        scheduler_state: str,
        exit_code: str | None,
    ) -> None:
        """Update one attempt in memory; callers ``save`` once per poll."""
        self._data["attempts"][attempt_id].update(
            state=state.value, scheduler_state=scheduler_state, exit_code=exit_code
        )

    def record_found(self, attempt_id: str, *, job_id: str) -> None:
        self._data["attempts"][attempt_id].update(job_id=job_id)

    def record_ended(self, attempt_id: str, scheduler_state: str) -> None:
        self._data["attempts"][attempt_id].update(
            state=ENDED, scheduler_state=scheduler_state
        )

    def outstanding(self) -> list[tuple[str, dict[str, Any]]]:
        """Attempts that may still hold scheduler resources."""
        finished = {JobState.SUCCEEDED.value, JobState.FAILED.value, REJECTED, ENDED}
        return [
            (attempt_id, dict(entry))
            for attempt_id, entry in self._data["attempts"].items()
            if entry.get("state") not in finished
        ]
