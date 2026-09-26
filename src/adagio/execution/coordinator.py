"""Dispatch a dependency graph of work items to a backend.

The coordinator decides *when* work runs: an item is dispatched once every
item it depends on has succeeded, and never beyond the backend's capacity.
*Where* it runs belongs to the backend, *what* it does to the item's program
(a generator that yields task invocations and receives their results), and
how progress is reported to the listener. Nothing here knows about pipelines,
QIIME, or any particular scheduler.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from .backends.base import Backend, JobHandle, JobState, TaskInvocation

if TYPE_CHECKING:
    from adagio.executors.base import TaskExecutionResult

    from .resources import TaskResourceRequirements

Program = Generator[TaskInvocation, "TaskExecutionResult", Any]


@dataclass(frozen=True)
class WorkItem:
    id: str
    depends_on: frozenset[str]
    resources: TaskResourceRequirements
    #: Begin the item's program. Every invocation it yields is submitted to the
    #: backend and the result sent back; its return value is the outcome.
    start: Callable[[], Program]


class Listener(Protocol):
    def started(self, item: WorkItem, handle: JobHandle | None) -> None: ...

    def succeeded(self, item: WorkItem, outcome: Any) -> None: ...

    def stopped(
        self,
        error: BaseException,
        *,
        failed: WorkItem | None,
        unfinished: Sequence[tuple[WorkItem, JobHandle | None]],
        canceled: bool,
        cleanup_errors: Sequence[str],
    ) -> None:
        """Report a run that ended early; ``failed`` is the item that caused it."""


@dataclass
class _Active:
    item: WorkItem
    program: Program
    handle: JobHandle


def coordinate(
    *, items: Sequence[WorkItem], backend: Backend, listener: Listener
) -> None:
    """Run ``items`` (given in dependency order) to completion, or stop on failure.

    On any failure or interrupt, the backend cancels its outstanding work and
    the listener hears about every unfinished item before the error propagates.
    """
    pending = list(items)
    completed: set[str] = set()
    started: set[str] = set()
    active: dict[str, _Active] = {}
    current: WorkItem | None = None

    def start(item: WorkItem, handle: JobHandle | None = None) -> None:
        if item.id not in started:
            started.add(item.id)
            listener.started(item, handle)

    def advance(item: WorkItem, program: Program, result: Any) -> None:
        nonlocal current
        current = item
        try:
            invocation = program.send(result)
        except StopIteration as done:
            start(item)
            listener.succeeded(item, done.value)
            completed.add(item.id)
            return
        if backend.starts_on_submit:
            start(item)
        active[item.id] = _Active(item, program, backend.submit(invocation, item.resources))

    restore_sigterm = _interrupt_on_sigterm()
    try:
        while pending or active:
            dispatched = False
            for item in list(pending):
                if len(active) >= backend.capacity:
                    break
                if not item.depends_on <= completed:
                    continue
                pending.remove(item)
                dispatched = True
                advance(item, item.start(), None)
            if active:
                # A failure while waiting belongs to no single item.
                current = None
                backend.wait([entry.handle for entry in active.values()])
                for entry in active.values():
                    if entry.handle.state is not JobState.QUEUED:
                        start(entry.item, entry.handle)
                # Surface every failure before releasing any dependents.
                for entry in list(active.values()):
                    if entry.handle.state is JobState.FAILED:
                        current = entry.item
                        backend.collect(entry.handle)
                for entry in list(active.values()):
                    if entry.handle.state is JobState.SUCCEEDED:
                        del active[entry.item.id]
                        current = entry.item
                        advance(entry.item, entry.program, backend.collect(entry.handle))
            elif pending and not dispatched:
                current = None
                raise RuntimeError(
                    "Unable to resolve task dependencies: "
                    + ", ".join(item.id for item in pending)
                )
    except BaseException as error:
        canceled = isinstance(error, KeyboardInterrupt)
        cleanup_errors = backend.cancel()
        listener.stopped(
            error,
            failed=None if canceled else current,
            unfinished=[
                (item, active[item.id].handle if item.id in active else None)
                for item in items
                if item.id not in completed
            ],
            canceled=canceled,
            cleanup_errors=cleanup_errors,
        )
        raise
    finally:
        restore_sigterm()


def _interrupt_on_sigterm() -> Callable[[], None]:
    """Turn SIGTERM into KeyboardInterrupt so a terminated run cancels its work."""
    if threading.current_thread() is not threading.main_thread():
        return lambda: None
    previous = signal.getsignal(signal.SIGTERM)

    def interrupt(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt("Run interrupted by SIGTERM.")

    signal.signal(signal.SIGTERM, interrupt)
    return lambda: signal.signal(
        signal.SIGTERM, previous if previous is not None else signal.SIG_DFL
    )
