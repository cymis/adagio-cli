import tempfile
import typing as t
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor
from adagio.monitor.tty import RichMonitor

from .cache_support import ExecutionCacheConfig
from .container_support import is_uri
from .common import plan_execution_order, task_label
from .path_utils import InputSource, resolve_host_input, resolve_host_path

CONTAINER_SUBTASK_COUNT = 1


@dataclass
class SerialExecutionState:
    cwd: Path
    work_path: Path
    params: dict[str, t.Any]
    scope: dict[str, InputSource]
    cache_config: ExecutionCacheConfig | None
    missing_optional_ids: set[str] = field(default_factory=set)
    saved_output_ids: set[str] = field(default_factory=set)
    save_output_started: bool = False


def run_serial_pipeline(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    resolve_task: t.Callable[[t.Any, SerialExecutionState, Console | None], bool],
    finish_outputs: t.Callable[
        [t.Any, AdagioArguments, SerialExecutionState, Monitor | None, bool], None
    ],
    console: Console | None = None,
    monitor: Monitor | None = None,
    total_subtasks: int = CONTAINER_SUBTASK_COUNT,
    cache_config: ExecutionCacheConfig | None = None,
) -> None:
    sig = pipeline.signature
    tasks = list(pipeline.iter_tasks())
    active_monitor = resolve_monitor(console=console, monitor=monitor)

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    active_monitor.start_pipeline(total_tasks=len(tasks))

    with tempfile.TemporaryDirectory(prefix="adagio-work-") as work_dir:
        state = SerialExecutionState(
            cwd=Path.cwd().resolve(),
            work_path=Path(work_dir),
            params=sig.get_params(arguments),
            scope={},
            cache_config=cache_config,
        )
        completed_task_ids: set[str] = set()

        active_monitor.start_load_input()
        for input_def in sig.inputs:
            source = arguments.inputs.get(input_def.name)
            if _is_missing(source):
                if not input_def.required:
                    state.missing_optional_ids.add(input_def.id)
                continue
            state.scope[input_def.id] = resolve_pipeline_input(
                source=source, type_name=input_def.type, cwd=state.cwd
            )
        active_monitor.finish_load_input()

        execution_plan = plan_execution_order(
            tasks=tasks,
            scope=state.scope,
            optional_missing_ids=state.missing_optional_ids,
        )
        for task in execution_plan:
            active_monitor.queue_task(
                task_id=task.id,
                label=task_label(task),
                total_subtasks=total_subtasks,
            )

        try:
            for task in execution_plan:
                active_monitor.start_task(task_id=task.id)
                try:
                    reused = resolve_task(task, state, console)
                    finish_outputs(
                        sig=sig,
                        arguments=arguments,
                        state=state,
                        monitor=active_monitor,
                        require_all=False,
                    )
                    active_monitor.advance_task(task_id=task.id, advance=1)
                    active_monitor.finish_task(
                        task_id=task.id,
                        status="cached" if reused else "completed",
                    )
                    completed_task_ids.add(task.id)
                except Exception as exc:  # noqa: BLE001
                    active_monitor.finish_task(
                        task_id=task.id, status="failed", error=str(exc)
                    )
                    for skipped_task in tasks:
                        if (
                            skipped_task.id == task.id
                            or skipped_task.id in completed_task_ids
                        ):
                            continue
                        active_monitor.finish_task(
                            task_id=skipped_task.id,
                            status="skipped",
                            error=f"Skipped because task {task.id!r} failed.",
                        )
                    if state.save_output_started:
                        active_monitor.finish_save_output()
                    raise

            try:
                finish_outputs(
                    sig=sig,
                    arguments=arguments,
                    state=state,
                    monitor=active_monitor,
                    require_all=True,
                )
            finally:
                if state.save_output_started:
                    active_monitor.finish_save_output()
        finally:
            active_monitor.finish_pipeline()


def resolve_monitor(*, console: Console | None, monitor: Monitor | None) -> Monitor:
    if monitor is not None:
        return monitor
    if console is not None:
        return RichMonitor(console=console)
    return LogMonitor()


def _is_missing(value: t.Any) -> bool:
    return value is None or value == "" or value == "<fill me>" or value == [] or value == {}


def resolve_pipeline_input(
    *, source: InputSource, type_name: str, cwd: Path
) -> InputSource:
    resolved = resolve_host_input(source=source, cwd=cwd)
    if not is_collection_type(type_name):
        return resolved

    if isinstance(resolved, str):
        return expand_collection_input_source(resolved)
    if isinstance(resolved, list):
        if len(resolved) == 1:
            return expand_collection_input_source(resolved[0])
        return resolved
    return list(resolved.values())


def is_collection_type(type_name: str) -> bool:
    return type_name.startswith("List[") or type_name.startswith("Collection[")


def expand_collection_input_source(source: str) -> list[str]:
    path = Path(source)
    if (
        not is_uri(source)
        and path.suffix.lower() in {".tsv", ".txt"}
        and path.is_file()
    ):
        return read_collection_manifest(path)
    return [source]


def read_collection_manifest(path: Path) -> list[str]:
    rows = [
        line.rstrip("\n").split("\t")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows:
        return []

    header = [cell.strip().lower() for cell in rows[0]]
    path_index = header.index("path") if "path" in header else None
    data_rows = rows[1:] if path_index is not None else rows

    result: list[str] = []
    for row in data_rows:
        if path_index is not None:
            if path_index >= len(row):
                continue
            raw_path = row[path_index].strip()
        elif len(row) >= 2:
            raw_path = row[1].strip()
        else:
            raw_path = row[0].strip()

        if raw_path:
            result.append(resolve_host_path(source=raw_path, cwd=path.parent))
    return result
