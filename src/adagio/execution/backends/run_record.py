"""The file that lets a supervisor clean up after a run it had to kill.

``adagio runtime --run-record PATH`` names a file this CLI writes while a run
owns work outside its own process tree, such as scheduler jobs. If the CLI
dies before cancelling that work, the supervisor runs ``adagio cleanup PATH``.
The contents are private to the CLI: supervisors only pass the path around.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from adagio.executors.task_contract import read_json_file, write_json_file

RUN_RECORD_VERSION = 1


@dataclass(frozen=True)
class RunRecord:
    executor: str
    registry: Path


def write_run_record(path: Path, *, executor: str, registry: Path) -> None:
    write_json_file(
        path,
        {"version": RUN_RECORD_VERSION, "executor": executor, "registry": str(registry)},
    )


def read_run_record(path: Path) -> RunRecord | None:
    """Return the record at ``path``, or None when the run left nothing behind."""
    if not path.exists():
        return None
    data = read_json_file(path)
    registry = Path(str(data.get("registry", "")))
    if (
        data.get("version") != RUN_RECORD_VERSION
        or not isinstance(data.get("executor"), str)
        or not registry.is_absolute()
    ):
        raise ValueError(f"Unrecognized run record: {path}.")
    return RunRecord(executor=data["executor"], registry=registry)
