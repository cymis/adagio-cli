import shutil
import tempfile
import traceback
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
from .common import plan_execution_order, prune_to_targets, task_label
from .path_utils import InputSource, resolve_host_input, resolve_host_path
from .task_contract import container_log_path

CONTAINER_SUBTASK_COUNT = 1


@dataclass
class TaskOutcome:
    """Result of resolving one task, carrying optional enrichment for events.

    ``resolve_task`` may return a bare ``bool`` (legacy: just ``reused``) or a
    ``TaskOutcome``. Enrichment (``input_signature``, ``command``, ``exit_code``,
    ``image_ref``, ``image_digest``, ``log_path``, ``timings``) is best-effort
    and relayed to structured monitors on ``finish_task``.
    """

    reused: bool = False
    enrichment: dict[str, t.Any] = field(default_factory=dict)


@dataclass
class SerialExecutionState:
    cwd: Path
    work_path: Path
    params: dict[str, t.Any]
    scope: dict[str, InputSource]
    cache_config: ExecutionCacheConfig | None
    materializations: dict[str, t.Any] = field(default_factory=dict)
    missing_optional_ids: set[str] = field(default_factory=set)
    saved_output_ids: set[str] = field(default_factory=set)
    save_output_started: bool = False
    # For a partial (``target_ids``) run, the pipeline outputs the pruned plan
    # is actually expected to produce. The terminal ``require_all`` save only
    # demands these — outputs whose producing task was pruned away are not
    # required (they were never meant to run). ``None`` => full run, require all.
    expected_output_ids: set[str] | None = None
    # Set per task by ``resolve_task`` so the runner can copy the container log
    # out to ``--log-dir`` before the temp work dir is torn down (design §5.5).
    log_dir: Path | None = None
    # Populated as tasks finish: node_id -> persisted log path (in ``log_dir``).
    persisted_logs: dict[str, str] = field(default_factory=dict)
    # Target node ids to prune the plan to (design §5.6); None => run all.
    target_ids: set[str] | None = None
    # The monitor is threaded onto the state so launchers can emit fine-phase
    # ``pulling_image`` / ``starting_container`` events.
    monitor: Monitor | None = None


def run_serial_pipeline(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    resolve_task: t.Callable[[t.Any, SerialExecutionState, Console | None], t.Any],
    finish_outputs: t.Callable[
        [t.Any, AdagioArguments, SerialExecutionState, Monitor | None, bool], None
    ],
    console: Console | None = None,
    monitor: Monitor | None = None,
    total_subtasks: int = CONTAINER_SUBTASK_COUNT,
    cache_config: ExecutionCacheConfig | None = None,
    target_ids: set[str] | None = None,
    log_dir: str | Path | None = None,
) -> None:
    sig = pipeline.signature
    tasks = list(pipeline.iter_tasks())
    active_monitor = resolve_monitor(console=console, monitor=monitor)

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    resolved_log_dir: Path | None = None
    if log_dir is not None:
        resolved_log_dir = Path(log_dir).expanduser()
        resolved_log_dir.mkdir(parents=True, exist_ok=True)

    active_monitor.start_pipeline(total_tasks=len(tasks))

    with tempfile.TemporaryDirectory(prefix="adagio-work-") as work_dir:
        state = SerialExecutionState(
            cwd=Path.cwd().resolve(),
            work_path=Path(work_dir),
            params=sig.get_params(arguments),
            scope={},
            cache_config=cache_config,
            log_dir=resolved_log_dir,
            target_ids=set(target_ids) if target_ids else None,
            monitor=active_monitor,
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
        if state.target_ids:
            execution_plan = prune_to_targets(
                execution_plan=execution_plan, target_ids=state.target_ids
            )
        planned_task_ids = {task.id for task in execution_plan}

        # A partial run only produces the outputs of its pruned plan, so the
        # terminal require_all save must not demand outputs from pruned tasks
        # (that KeyError'd every editor "run node"). Full runs keep None and
        # still require every signature output.
        if state.target_ids:
            producer_task_of_output = {
                output.id: task.id
                for task in tasks
                for output in task.outputs.values()
            }
            state.expected_output_ids = {
                out.id
                for out in sig.outputs
                if producer_task_of_output.get(out.id) in planned_task_ids
            }

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
                    outcome = _coerce_outcome(resolve_task(task, state, console))
                    _persist_task_log(state=state, task_id=task.id, outcome=outcome)
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
                        status="cached" if outcome.reused else "completed",
                        **outcome.enrichment,
                    )
                    completed_task_ids.add(task.id)
                except Exception as exc:  # noqa: BLE001
                    active_monitor.finish_task(
                        task_id=task.id,
                        status="failed",
                        error=str(exc),
                        traceback=traceback.format_exc(),
                    )
                    for skipped_task in execution_plan:
                        if (
                            skipped_task.id == task.id
                            or skipped_task.id in completed_task_ids
                            or skipped_task.id not in planned_task_ids
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


def _coerce_outcome(value: t.Any) -> TaskOutcome:
    """Normalize a ``resolve_task`` return into a ``TaskOutcome``.

    Accepts a ``TaskOutcome`` (new) or a bare ``bool`` (legacy ``reused``).
    """
    if isinstance(value, TaskOutcome):
        return value
    return TaskOutcome(reused=bool(value))


def _persist_task_log(
    *, state: SerialExecutionState, task_id: str, outcome: TaskOutcome
) -> None:
    """Copy a task's container log into ``--log-dir`` before teardown (§5.5).

    The work dir (and every ``*_container.log``) is deleted when the run's
    ``TemporaryDirectory`` context exits, so persisting must happen here, per
    task. On success the persisted path is recorded on ``state`` and folded into
    the task's enrichment so the adapter can find it via ``node_finished`` /
    ``output_saved``. Best-effort: a copy failure never fails the task.
    """
    if state.log_dir is None:
        return
    source = container_log_path(task_id=task_id, work_path=state.work_path)
    if not source.exists():
        return
    destination = state.log_dir / source.name
    try:
        shutil.copy2(source, destination)
    except OSError:
        return
    persisted = str(destination)
    state.persisted_logs[task_id] = persisted
    outcome.enrichment["log_path"] = persisted


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
