"""Coordination invariants independent of a scientific runtime or scheduler."""

from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from adagio.cli.config import AdagioRunConfig
from adagio.execution import coordinator
from adagio.executors.serial_runner import SerialExecutionState, TaskOutcome


@pytest.mark.parametrize("limit,fail", [(1, False), (2, False), (2, True)])
def test_dependencies_publish_before_release_and_fail_fast(
    tmp_path, monkeypatch, limit, fail
):
    tasks = [
        NS(
            id=name,
            inputs={str(i): NS(kind="archive", id=i + "-out") for i in deps},
            outputs={"out": NS(id=name + "-out")},
        )
        for name, deps in [("A", []), ("B", ["A"]), ("C", ["A"]), ("D", ["B", "C"])]
    ]
    submitted = []
    published = []
    batch_sizes = []
    canceled = []

    class Backend:
        def __init__(self, **kwargs):
            self.handles = []

        def submit(self, prepared, resources):
            name = prepared.name
            assert all(
                src.id[:-4] in published
                for src in tasks[["A", "B", "C", "D"].index(name)].inputs.values()
            )
            submitted.append(name)
            h = NS(job_id=name, prepared=prepared, state="PENDING", exit_code=None)
            self.handles.append(h)
            return h

        def poll(self, handles):
            batch_sizes.append(len(handles))
            for h in handles:
                h.state = "FAILED" if fail and h.job_id == "B" else "COMPLETED"
                h.exit_code = "1:0" if h.state == "FAILED" else "0:0"

        def collect(self, h):
            if h.state == "FAILED":
                raise RuntimeError("original worker failure")
            return h.job_id

        def cancel(self, handles):
            canceled.extend(h.job_id for h in handles)
            return []

    monkeypatch.setattr(coordinator, "SlurmBackend", Backend)
    monkeypatch.setattr(coordinator.time, "sleep", lambda _: None)

    def resolve(task, state, console):
        prepared = NS(name=task.id, log_path=tmp_path / "log")
        result = yield (
            NS(prepare=lambda **kwargs: prepared),
            {"environment": NS(kind="conda"), "request": NS()},
        )
        assert result == task.id
        published.append(task.id)
        state.scope[task.id + "-out"] = "validated"
        return TaskOutcome()

    state = SerialExecutionState(
        cwd=tmp_path, work_path=tmp_path, params={}, scope={}, cache_config=None
    )
    args = {
        "execution_plan": tasks,
        "state": state,
        "resolve_task": resolve,
        "finish_outputs": lambda **kwargs: None,
        "sig": None,
        "arguments": None,
        "monitor": Mock(),
        "console": None,
        "run_config": AdagioRunConfig(
            executor={
                "kind": "slurm",
                "work_dir": str(tmp_path),
                "max_in_flight": limit,
            }
        ),
    }
    if fail:
        with pytest.raises(RuntimeError, match="original worker failure"):
            coordinator.coordinate(**args)
        assert "D" not in submitted and "B" not in published and "C" in canceled
    else:
        coordinator.coordinate(**args)
        assert set(published) == {"A", "B", "C", "D"}
    assert max(batch_sizes) <= limit
