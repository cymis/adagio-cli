import tempfile
import typing as t
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor
from adagio.monitor.tty import RichMonitor

from .cache_support import ExecutionCacheConfig
from .common import plan_execution_order, task_label
from .path_utils import resolve_host_path

CONTAINER_SUBTASK_COUNT = 1


@dataclass
class SerialExecutionState:
    cwd: Path
    work_path: Path
    params: dict[str, t.Any]
    scope: dict[str, str]
    cache_config: ExecutionCacheConfig | None


def run_serial_pipeline(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    resolve_task: t.Callable[[t.Any, SerialExecutionState, Console | None], bool],
    finish_outputs: t.Callable[[t.Any, AdagioArguments, SerialExecutionState, Monitor | None], None],
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
            source = arguments.inputs[input_def.name]
            state.scope[input_def.id] = resolve_host_path(source=source, cwd=state.cwd)
        active_monitor.finish_load_input()

        execution_plan = plan_execution_order(tasks=tasks, scope=state.scope)
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
                    active_monitor.advance_task(task_id=task.id, advance=1)
                    active_monitor.finish_task(
                        task_id=task.id,
                        status="cached" if reused else "completed",
                    )
                    completed_task_ids.add(task.id)
                except Exception as exc:  # noqa: BLE001
                    active_monitor.finish_task(task_id=task.id, status="failed", error=str(exc))
                    for skipped_task in tasks:
                        if skipped_task.id == task.id or skipped_task.id in completed_task_ids:
                            continue
                        active_monitor.finish_task(
                            task_id=skipped_task.id,
                            status="skipped",
                            error=f"Skipped because task {task.id!r} failed.",
                        )
                    raise

            active_monitor.start_save_output()
            finish_outputs(
                sig=sig,
                arguments=arguments,
                state=state,
                monitor=active_monitor,
            )
            active_monitor.finish_save_output()
        finally:
            active_monitor.finish_pipeline()


def resolve_monitor(*, console: Console | None, monitor: Monitor | None) -> Monitor:
    if monitor is not None:
        return monitor
    if console is not None:
        return RichMonitor(console=console)
    return LogMonitor()
