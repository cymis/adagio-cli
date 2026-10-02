"""Task phases travel over a mounted file, without credentials in workers.

The launcher keeps capturing stdout/stderr as before. A reader relays complete
JSON lines while the worker runs; no parsing of scientific output is involved.
"""

import json
import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from adagio.monitor.api import Monitor

TASK_PHASES = frozenset(
    {
        "preparing",
        "preparing_cache",
        "checking_cache",
        "using_cache",
        "running",
        "saving",
    }
)


class TaskTelemetry:
    """Measure worker stages and publish their phases to the parent launcher."""

    def __init__(self, progress_path: str | None):
        self.path = Path(progress_path) if progress_path else None
        self.started = time.monotonic()
        self.timings: dict[str, float] = {"action_seconds": 0.0}

    def phase(self, phase: str) -> None:
        if self.path is None:
            return
        try:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"phase": phase}) + "\n")
        except OSError:
            # Telemetry must not change whether the scientific action succeeds.
            pass

    @contextmanager
    def measure(self, name: str):
        started = time.monotonic()
        try:
            yield
        finally:
            self.timings[name] = time.monotonic() - started

    def finish(self) -> dict[str, float]:
        self.timings["worker_seconds"] = time.monotonic() - self.started
        return self.timings


@contextmanager
def relay_task_progress(
    *, path: Path, monitor: "Monitor | None", task_id: str
) -> Iterator[None]:
    """Relay worker phases during a blocking launch, draining before completion."""
    try:
        path.write_text("", encoding="utf-8")
    except OSError:
        yield
        return
    if monitor is None:
        yield
        return

    stopped = threading.Event()

    def relay():
        try:
            with path.open(encoding="utf-8") as stream:
                while True:
                    position = stream.tell()
                    line = stream.readline()
                    if line.endswith("\n"):
                        try:
                            phase = json.loads(line)["phase"]
                        except (ValueError, KeyError, TypeError):
                            continue
                        if phase in TASK_PHASES:
                            monitor.update_task_phase(task_id=task_id, phase=phase)
                        continue
                    # A writer may still be midway through a line.
                    stream.seek(position)
                    if stopped.is_set():
                        return
                    stopped.wait(0.1)
        except Exception:
            logging.getLogger(__name__).exception("Could not relay task progress")

    thread = threading.Thread(target=relay, name="adagio-task-progress", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join()


def execution_timings(
    payload: dict, *, run_seconds: float, **host: float
) -> dict[str, float]:
    """Combine worker measurements with the launcher's encompassing wall time."""
    timings = dict(payload.get("timings", {}))
    if "worker_seconds" in timings:
        # This includes process startup AND shutdown, not just container startup.
        timings["environment_overhead_seconds"] = max(
            0.0, run_seconds - timings["worker_seconds"]
        )
    timings.update(host, run_seconds=run_seconds)
    return timings


def append_timing_summary(*, log_path: str, timings: dict, reused: bool) -> None:
    """Keep the measured breakdown in the existing downloadable node log."""
    try:
        with Path(log_path).open("a", encoding="utf-8") as stream:
            stream.write("\nAdagio task timings (seconds; totals overlap stages)\n")
            stream.write(f"  Reused cached result: {str(reused).lower()}\n")
            for name, seconds in timings.items():
                stream.write(f"  {name}: {seconds:.6f}\n")
    except OSError:
        pass
