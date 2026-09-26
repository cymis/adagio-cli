"""Coordination invariants, independent of any scientific runtime or scheduler."""

from types import SimpleNamespace as NS

import pytest

from adagio.execution.backends.base import JobHandle, JobState
from adagio.execution.coordinator import WorkItem, coordinate
from adagio.execution.resources import TaskResourceRequirements


class QueueBackend:
    """A scheduler-like backend: jobs queue, then finish on the next wait."""

    starts_on_submit = False

    def __init__(self, *, capacity, fail=()):
        self.capacity = capacity
        self.fail = set(fail)
        self.submitted = []
        self.waits = []
        self.cancelled = False

    def submit(self, invocation, resources):
        self.submitted.append(invocation)
        return JobHandle(state=JobState.QUEUED, job_id=f"job-{invocation}")

    def wait(self, handles):
        self.waits.append(len(handles))
        for handle in handles:
            name = handle.job_id.removeprefix("job-")
            handle.state = JobState.FAILED if name in self.fail else JobState.SUCCEEDED

    def collect(self, handle):
        if handle.state is JobState.FAILED:
            raise RuntimeError(f"{handle.job_id} failed")
        return handle.job_id.removeprefix("job-")

    def cancel(self):
        self.cancelled = True
        return []


class Recorder:
    def __init__(self):
        self.events = []

    def started(self, item, handle):
        self.events.append(("started", item.id, handle and handle.job_id))

    def succeeded(self, item, outcome):
        self.events.append(("succeeded", item.id, outcome))

    def stopped(self, error, *, failed, unfinished, canceled, cleanup_errors):
        self.events.append(
            (
                "stopped",
                failed and failed.id,
                [item.id for item, _ in unfinished],
                canceled,
            )
        )


def items(graph, published=None):
    published = [] if published is None else published

    def program(name):
        def start():
            result = yield name
            assert result == name
            published.append(name)
            return f"{name}-outcome"

        return start

    return [
        WorkItem(
            id=name,
            depends_on=frozenset(deps),
            resources=TaskResourceRequirements(cpus=1),
            start=program(name),
        )
        for name, deps in graph
    ]


DIAMOND = [("A", []), ("B", ["A"]), ("C", ["A"]), ("D", ["B", "C"])]


@pytest.mark.parametrize("capacity", [1, 2, 8])
def test_dependents_wait_for_publication_and_capacity_is_respected(capacity):
    published = []
    backend = QueueBackend(capacity=capacity)
    recorder = Recorder()
    coordinate(items=items(DIAMOND, published), backend=backend, listener=recorder)
    assert published == ["A", "B", "C", "D"] or published == ["A", "C", "B", "D"]
    assert max(backend.waits) <= capacity
    # A queued job is reported started only once the scheduler runs it.
    assert recorder.events[0] == ("started", "A", "job-A")
    assert ("succeeded", "D", "D-outcome") in recorder.events


def test_failure_cancels_outstanding_work_and_reports_every_unfinished_item():
    backend = QueueBackend(capacity=2, fail={"B"})
    recorder = Recorder()
    with pytest.raises(RuntimeError, match="job-B failed"):
        coordinate(items=items(DIAMOND), backend=backend, listener=recorder)
    assert backend.cancelled
    assert "D" not in backend.submitted
    assert recorder.events[-1] == ("stopped", "B", ["B", "C", "D"], False)


def test_local_backends_report_start_before_running_the_program():
    order = []

    class InlineBackend(QueueBackend):
        starts_on_submit = True

        def submit(self, invocation, resources):
            order.append(f"run {invocation}")
            return JobHandle(state=JobState.SUCCEEDED, job_id=f"job-{invocation}")

    class OrderRecorder(Recorder):
        def started(self, item, handle):
            order.append(f"start {item.id}")

    coordinate(
        items=items([("A", []), ("B", ["A"])]),
        backend=InlineBackend(capacity=1),
        listener=OrderRecorder(),
    )
    assert order == ["start A", "run A", "start B", "run B"]


def test_items_without_invocations_start_and_succeed_immediately():
    def done():
        return True
        yield  # pragma: no cover - makes this a generator

    recorder = Recorder()
    coordinate(
        items=[
            WorkItem(
                id="input",
                depends_on=frozenset(),
                resources=TaskResourceRequirements(cpus=1),
                start=done,
            )
        ],
        backend=QueueBackend(capacity=1),
        listener=recorder,
    )
    assert recorder.events == [
        ("started", "input", None),
        ("succeeded", "input", True),
    ]


def test_interrupt_cancels_and_reports_everything_canceled():
    class InterruptingBackend(QueueBackend):
        def wait(self, handles):
            raise KeyboardInterrupt("Run interrupted by SIGTERM.")

    backend = InterruptingBackend(capacity=2)
    recorder = Recorder()
    with pytest.raises(KeyboardInterrupt):
        coordinate(items=items(DIAMOND), backend=backend, listener=recorder)
    assert backend.cancelled
    assert recorder.events[-1] == ("stopped", None, ["A", "B", "C", "D"], True)


def test_unsatisfiable_dependencies_are_reported():
    recorder = Recorder()
    with pytest.raises(RuntimeError, match="Unable to resolve task dependencies: B"):
        coordinate(
            items=items([("B", ["missing"])]),
            backend=QueueBackend(capacity=1),
            listener=recorder,
        )


def test_sigterm_handler_is_restored(monkeypatch):
    import signal

    before = signal.getsignal(signal.SIGTERM)
    coordinate(
        items=items([("A", [])]), backend=QueueBackend(capacity=1), listener=Recorder()
    )
    assert signal.getsignal(signal.SIGTERM) == before


def test_multi_step_programs_resubmit_until_they_return():
    def program():
        first = yield "import"
        second = yield "materialize"
        return (first, second)

    recorder = Recorder()
    backend = QueueBackend(capacity=1)
    coordinate(
        items=[
            WorkItem(
                id="data-import",
                depends_on=frozenset(),
                resources=TaskResourceRequirements(cpus=1),
                start=program,
            )
        ],
        backend=backend,
        listener=recorder,
    )
    assert backend.submitted == ["import", "materialize"]
    assert recorder.events[-1] == (
        "succeeded",
        "data-import",
        ("import", "materialize"),
    )


def test_listener_failure_is_attributed_to_the_item():
    class FailingRecorder(Recorder):
        def succeeded(self, item, outcome):
            if item.id == "A":
                raise OSError("disk full")
            super().succeeded(item, outcome)

    recorder = FailingRecorder()
    with pytest.raises(OSError):
        coordinate(
            items=items([("A", []), ("B", ["A"])]),
            backend=QueueBackend(capacity=1),
            listener=recorder,
        )
    assert recorder.events[-1] == ("stopped", "A", ["A", "B"], False)


def test_handles_reach_the_listener_for_unfinished_items():
    seen = []

    class HandleRecorder(Recorder):
        def stopped(self, error, *, failed, unfinished, canceled, cleanup_errors):
            seen.extend((item.id, handle and handle.job_id) for item, handle in unfinished)

    with pytest.raises(RuntimeError):
        coordinate(
            items=items(DIAMOND),
            backend=QueueBackend(capacity=2, fail={"A"}),
            listener=HandleRecorder(),
        )
    assert seen == [("A", "job-A"), ("B", None), ("C", None), ("D", None)]


def test_local_backend_reports_launcher_failures_and_system_exit(tmp_path):
    from adagio.execution.backends.base import TaskInvocation
    from adagio.execution.backends.local import LocalBackend

    class Launcher:
        def __init__(self, error):
            self.error = error

        def launch(self, *, environment, request, console=None):
            raise self.error

    request = NS(task=NS(id="node"), work_path=tmp_path)
    backend = LocalBackend()
    for error in (RuntimeError("container failed"), SystemExit("docker not found")):
        handle = backend.submit(
            TaskInvocation(launcher=Launcher(error), environment=NS(), request=request),
            TaskResourceRequirements(cpus=1),
        )
        assert handle.state is JobState.FAILED
        assert handle.log_path == tmp_path / "node_container.log"
        with pytest.raises(type(error)):
            backend.collect(handle)


def test_a_failure_while_waiting_blames_no_item():
    class BrokenBackend(QueueBackend):
        def wait(self, handles):
            raise RuntimeError("scheduler unreachable")

    recorder = Recorder()
    with pytest.raises(RuntimeError, match="scheduler unreachable"):
        coordinate(
            items=items([("A", [])]), backend=BrokenBackend(capacity=1), listener=recorder
        )
    assert recorder.events[-1] == ("stopped", None, ["A"], False)
