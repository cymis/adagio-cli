import inspect
import shutil
import traceback
import typing as t
from dataclasses import dataclass, field, replace
from pathlib import Path

from rich.console import Console

from adagio.execution.backends.base import Backend, JobHandle, Workspace
from adagio.execution.backends.local import LocalBackend
from adagio.execution.coordinator import WorkItem, coordinate
from adagio.execution.resources import ConfiguredResourcePolicy, ResourcePolicy
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import input_source_ids
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
    metadata_views: dict[str, str] = field(default_factory=dict)
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


@dataclass(frozen=True)
class PipelinePlan:
    """The tasks one run executes, in dependency order, and its resolved inputs."""

    tasks: list[t.Any]
    scope: dict[str, InputSource]
    missing_optional_ids: set[str]
    target_ids: set[str] | None
    expected_output_ids: set[str] | None


def plan_pipeline(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    target_ids: set[str] | None = None,
) -> PipelinePlan:
    """Select, validate and order a run's tasks and resolve its inputs.

    Nothing is executed: plan-only inspection and execution both start here.
    """
    sig = pipeline.signature
    tasks = list(pipeline.iter_tasks())
    selected_target_ids = set(target_ids) if target_ids else None
    if selected_target_ids:
        tasks = prune_to_targets(execution_plan=tasks, target_ids=selected_target_ids)

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    cwd = Path.cwd().resolve()
    scope: dict[str, InputSource] = {}
    missing_optional_ids: set[str] = set()
    for input_def in sig.inputs:
        source = arguments.inputs.get(input_def.name)
        if _is_missing(source):
            if not input_def.required:
                missing_optional_ids.add(input_def.id)
            continue
        scope[input_def.id] = resolve_pipeline_input(
            source=source, type_name=input_def.type, cwd=cwd
        )

    execution_plan = plan_execution_order(
        tasks=tasks,
        scope=scope,
        optional_missing_ids=missing_optional_ids,
    )

    # A partial run only produces the outputs of its pruned plan, so the
    # terminal require_all save must not demand outputs from pruned tasks
    # (that KeyError'd every editor "run node"). Full runs keep None and
    # still require every signature output.
    expected_output_ids = None
    if selected_target_ids:
        planned_task_ids = {task.id for task in execution_plan}
        producer_task_of_output = {
            output.id: task.id
            for task in tasks
            for output in task.outputs.values()
        }
        expected_output_ids = {
            out.id
            for out in sig.outputs
            if producer_task_of_output.get(out.id) in planned_task_ids
        }

    return PipelinePlan(
        tasks=execution_plan,
        scope=scope,
        missing_optional_ids=missing_optional_ids,
        target_ids=selected_target_ids,
        expected_output_ids=expected_output_ids,
    )


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
    plan: PipelinePlan | None = None,
    backend: Backend | None = None,
    resources: ResourcePolicy | None = None,
) -> None:
    """Run a pipeline's tasks on ``backend`` (this host, one at a time, by default).

    ``resolve_task`` performs one task. It returns the task's outcome, or is a
    generator that yields task invocations for the backend and receives their
    results. ``plan`` skips re-planning when the caller already planned the run.
    """
    sig = pipeline.signature
    if plan is None:
        plan = plan_pipeline(pipeline=pipeline, arguments=arguments, target_ids=target_ids)
    if backend is None:
        backend = LocalBackend()
    if resources is None:
        resources = ConfiguredResourcePolicy()
    active_monitor = resolve_monitor(console=console, monitor=monitor)

    resolved_log_dir: Path | None = None
    if log_dir is not None:
        resolved_log_dir = Path(log_dir).expanduser()
        resolved_log_dir.mkdir(parents=True, exist_ok=True)

    active_monitor.start_pipeline(total_tasks=len(plan.tasks))

    with backend.workspace() as workspace:
        state = SerialExecutionState(
            cwd=Path.cwd().resolve(),
            work_path=workspace.root,
            params=sig.get_params(arguments),
            scope=dict(plan.scope),
            cache_config=cache_config,
            missing_optional_ids=set(plan.missing_optional_ids),
            expected_output_ids=plan.expected_output_ids,
            log_dir=resolved_log_dir,
            target_ids=plan.target_ids,
            monitor=active_monitor,
        )

        # Inputs were resolved while planning; the phase is still reported.
        active_monitor.start_load_input()
        active_monitor.finish_load_input()

        for task in plan.tasks:
            active_monitor.queue_task(
                task_id=task.id,
                label=task_label(task),
                total_subtasks=total_subtasks,
            )

        try:
            try:
                coordinate(
                    items=_work_items(
                        plan.tasks,
                        state=state,
                        workspace=workspace,
                        backend=backend,
                        resources=resources,
                        resolve_task=resolve_task,
                        console=console,
                    ),
                    backend=backend,
                    listener=_TaskReporter(
                        state=state,
                        monitor=active_monitor,
                        finish_outputs=finish_outputs,
                        sig=sig,
                        arguments=arguments,
                    ),
                )
            except BaseException:
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


def _work_items(
    tasks: list[t.Any],
    *,
    state: SerialExecutionState,
    workspace: Workspace,
    backend: Backend,
    resources: ResourcePolicy,
    resolve_task: t.Callable[[t.Any, SerialExecutionState, Console | None], t.Any],
    console: Console | None,
) -> list[WorkItem]:
    """One work item per task; a task depends on the planned tasks it reads from."""
    producers = {output.id: task.id for task in tasks for output in task.outputs.values()}

    def program(task: t.Any) -> t.Callable[[], t.Generator[t.Any, t.Any, t.Any]]:
        def start() -> t.Generator[t.Any, t.Any, t.Any]:
            work_path = backend.attempt_directory(workspace)
            task_state = state
            if work_path != state.work_path:
                task_state = replace(state, work_path=work_path, cwd=workspace.cwd)
            result = resolve_task(task, task_state, console)
            if inspect.isgenerator(result):
                result = yield from result
            return result

        return start

    return [
        WorkItem(
            id=task.id,
            depends_on=frozenset(
                producers[source_id]
                for src in task.inputs.values()
                for source_id in input_source_ids(src)
                if source_id in producers and producers[source_id] != task.id
            ),
            resources=resources.for_task(task),
            start=program(task),
        )
        for task in tasks
    ]


class _TaskReporter:
    """Report coordinator progress as monitor events; save outputs as they appear."""

    def __init__(
        self,
        *,
        state: SerialExecutionState,
        monitor: Monitor,
        finish_outputs: t.Callable[..., None],
        sig: t.Any,
        arguments: AdagioArguments,
    ) -> None:
        self._state = state
        self._monitor = monitor
        self._finish_outputs = finish_outputs
        self._sig = sig
        self._arguments = arguments

    def started(self, item: WorkItem, handle: JobHandle | None) -> None:
        details = {}
        if handle is not None and handle.job_id is not None:
            details["scheduler_job_id"] = handle.job_id
        self._monitor.start_task(task_id=item.id, **details)

    def succeeded(self, item: WorkItem, outcome: t.Any) -> None:
        outcome = _coerce_outcome(outcome)
        _persist_task_log(state=self._state, task_id=item.id, outcome=outcome)
        self._finish_outputs(
            sig=self._sig,
            arguments=self._arguments,
            state=self._state,
            monitor=self._monitor,
            require_all=False,
        )
        self._monitor.advance_task(task_id=item.id, advance=1)
        self._monitor.finish_task(
            task_id=item.id,
            status="cached" if outcome.reused else "completed",
            **outcome.enrichment,
        )

    def stopped(
        self,
        error: BaseException,
        *,
        failed: WorkItem | None,
        unfinished: t.Sequence[tuple[WorkItem, JobHandle | None]],
        canceled: bool,
        cleanup_errors: t.Sequence[str],
    ) -> None:
        message = str(error) or ("Run interrupted." if canceled else type(error).__name__)
        if cleanup_errors:
            message = f"{message} Cleanup incomplete: {'; '.join(cleanup_errors)}"
        for item, handle in unfinished:
            details: dict[str, t.Any] = {}
            if handle is not None and handle.log_path is not None:
                _persist_task_log(
                    state=self._state,
                    task_id=item.id,
                    outcome=TaskOutcome(enrichment={"log_path": str(handle.log_path)}),
                )
                if item.id in self._state.persisted_logs:
                    details["log_path"] = self._state.persisted_logs[item.id]
            if canceled:
                self._monitor.finish_task(
                    task_id=item.id, status="canceled", error=message, **details
                )
            elif failed is not None and item.id == failed.id:
                self._monitor.finish_task(
                    task_id=item.id,
                    status="failed",
                    error=message,
                    traceback="".join(
                        traceback.format_exception(type(error), error, error.__traceback__)
                    ),
                    **details,
                )
            else:
                self._monitor.finish_task(
                    task_id=item.id,
                    status="skipped",
                    error=(
                        f"Skipped because task {failed.id!r} failed."
                        if failed is not None
                        else message
                    ),
                    **details,
                )


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
    source = Path(
        outcome.enrichment.get("log_path")
        or container_log_path(task_id=task_id, work_path=state.work_path)
    )
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
