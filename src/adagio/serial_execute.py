from __future__ import annotations

import os
import typing as t
import warnings
import zipfile
from collections.abc import Mapping

from adagio.model.arguments import AdagioArguments
from adagio.model.ast import TypeAST, TypeASTExpression, TypeASTIntersection, TypeASTUnion
from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import PluginActionTask, RootInputTask
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor

SERIAL_SUBTASK_COUNT = 1


def execute_serial(
    *, pipeline: AdagioPipeline, arguments: AdagioArguments, monitor: Monitor | None = None
) -> None:
    """Execute a pipeline serially using the QIIME API (no Parsl)."""
    from qiime2 import get_cache
    from qiime2.sdk import PluginManager

    sig = pipeline.signature
    tasks = list(pipeline.iter_tasks())
    monitor = monitor or LogMonitor()

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    monitor.start_pipeline(total_tasks=len(tasks))
    try:
        plugin_manager = PluginManager()
        cache = get_cache()
        with cache:
            scope: dict[str, t.Any] = {}
            completed_task_ids: set[str] = set()

            monitor.start_load_input()
            _load_inputs(sig=sig, arguments=arguments, scope=scope)
            monitor.finish_load_input()

            execution_plan = _plan_execution_order(tasks=tasks, scope=scope)
            for task in execution_plan:
                monitor.queue_task(
                    task_id=task.id,
                    label=_task_label(task),
                    # QIIME actions do not expose nested subtask progress.
                    total_subtasks=SERIAL_SUBTASK_COUNT,
                )

            params = sig.get_params(arguments)

            for task in execution_plan:
                monitor.start_task(task_id=task.id)
                try:
                    _execute_task(task=task, plugin_manager=plugin_manager, params=params, scope=scope)
                    monitor.advance_task(task_id=task.id, advance=1)
                    monitor.finish_task(task_id=task.id, status="completed")
                    completed_task_ids.add(task.id)
                except Exception as exc:  # noqa: BLE001
                    monitor.finish_task(task_id=task.id, status="failed", error=str(exc))
                    for skipped_task in tasks:
                        if skipped_task.id == task.id or skipped_task.id in completed_task_ids:
                            continue
                        monitor.finish_task(
                            task_id=skipped_task.id,
                            status="skipped",
                            error=f"Skipped because task {task.id!r} failed.",
                        )
                    raise

            monitor.start_save_output()
            _save_outputs(sig=sig, arguments=arguments, scope=scope, monitor=monitor)
            monitor.finish_save_output()
    finally:
        monitor.finish_pipeline()


def _load_inputs(*, sig, arguments: AdagioArguments, scope: dict[str, t.Any]) -> None:
    from qiime2 import Artifact

    for input_def in sig.inputs:
        source = arguments.inputs[input_def.name]
        if _is_metadata_ast(input_def.ast):
            scope[input_def.id] = _load_metadata(source)
        else:
            scope[input_def.id] = Artifact.load(source)


def _load_metadata(source: str) -> t.Any:
    from qiime2 import Artifact, Metadata

    if zipfile.is_zipfile(source):
        return Artifact.load(source).view(Metadata)
    return Metadata.load(source)


def _execute_task(*, task: t.Any, plugin_manager, params: dict[str, t.Any], scope: dict[str, t.Any]) -> None:
    if isinstance(task, RootInputTask):
        for name, src in task.inputs.items():
            dst = task.outputs[name]
            scope[dst.id] = scope[src.id]
        return None

    if isinstance(task, PluginActionTask):
        _execute_plugin_action(task=task, plugin_manager=plugin_manager, params=params, scope=scope)
        return None

    raise TypeError(f"Unsupported task type: {type(task)}")


def _execute_plugin_action(
    *, task: PluginActionTask, plugin_manager, params: dict[str, t.Any], scope: dict[str, t.Any]
) -> None:
    plugins = plugin_manager.plugins
    resolved_plugin_name, plugin = _resolve_key(plugins, task.plugin)
    if plugin is None:
        available_plugins = ", ".join(sorted(plugins.keys())[:20])
        raise KeyError(
            "Unable to find QIIME plugin "
            f"{task.plugin!r} for task {task.id!r}. "
            "This usually means the runtime image is missing required plugins. "
            f"Available plugins (first 20): [{available_plugins}]"
        )

    actions = plugin.actions
    resolved_action_name, action = _resolve_key(actions, task.action)
    if action is None:
        available_actions = ", ".join(sorted(actions.keys())[:30])
        raise KeyError(
            "Unable to find QIIME action "
            f"{task.plugin!r}.{task.action!r} for task {task.id!r}. "
            "This usually means the runtime image is not the expected QIIME distribution/version. "
            f"Available actions in plugin {task.plugin!r} (first 30): [{available_actions}]"
        )
    kwargs: dict[str, t.Any] = {}
    metadata_inputs: dict[str, t.Any] = {}

    for name, src in task.inputs.items():
        if src.id not in scope:
            raise KeyError(f"Missing input dependency {src.id!r} for task {task.id!r}.")
        value = scope[src.id]
        if src.kind == "archive":
            kwargs[name] = value
        elif src.kind == "metadata":
            metadata_inputs[name] = _as_metadata(value)
        else:
            raise TypeError(f"Unsupported input kind: {src.kind!r}")

    for name, param in task.parameters.items():
        if param.kind == "literal":
            kwargs[name] = _coerce_action_parameter(action=action, parameter_name=name, value=param.value)
        elif param.kind == "promoted":
            if param.id not in params:
                raise KeyError(f"Missing promoted parameter {param.id!r} for task {task.id!r}.")
            kwargs[name] = _coerce_action_parameter(
                action=action,
                parameter_name=name,
                value=params[param.id],
            )
        elif param.kind == "metadata":
            if name not in metadata_inputs:
                raise KeyError(f"Missing metadata input {name!r} for task {task.id!r}.")
            metadata = metadata_inputs.pop(name)
            column = _resolve_metadata_column_name(param=param, params=params)
            kwargs[name] = metadata.get_column(column)
        else:
            raise TypeError(f"Unsupported parameter kind: {param.kind!r}")

    for name, value in metadata_inputs.items():
        kwargs[name] = value

    with _action_output_context():
        results = action(**kwargs)
    for name, dest in task.outputs.items():
        scope[dest.id] = getattr(results, name)


def _coerce_action_parameter(*, action: t.Any, parameter_name: str, value: t.Any) -> t.Any:
    if value is None:
        return None

    signature = getattr(action, "signature", None)
    parameters = getattr(signature, "parameters", None)
    if not isinstance(parameters, Mapping):
        return value
    if parameter_name not in parameters:
        return value

    qiime_type = getattr(parameters[parameter_name], "qiime_type", None)
    if qiime_type is None:
        return value

    from qiime2.sdk.util import parse_primitive

    return parse_primitive(qiime_type, value)


def _resolve_key(mapping: t.Mapping[str, t.Any], requested: str) -> tuple[str | None, t.Any]:
    if requested in mapping:
        return requested, mapping[requested]

    canonical_requested = _canonical_name(requested)
    for key in mapping.keys():
        if _canonical_name(key) == canonical_requested:
            return key, mapping[key]

    return None, None


def _canonical_name(value: str) -> str:
    return value.strip().replace("-", "_").replace(" ", "_").lower()


def _resolve_metadata_column_name(*, param, params: dict[str, t.Any]) -> str:
    column = param.column
    if column.kind == "literal":
        return str(column.value)
    if column.kind == "promoted":
        if column.id not in params:
            raise KeyError(f"Missing promoted metadata column parameter {column.id!r}.")
        return str(params[column.id])
    raise TypeError(f"Unsupported metadata column selector kind: {column.kind!r}")


def _as_metadata(value: t.Any) -> t.Any:
    from qiime2 import Artifact, Metadata

    if isinstance(value, Metadata):
        return value
    if isinstance(value, Artifact):
        return value.view(Metadata)
    return value


def _plan_execution_order(*, tasks: list[t.Any], scope: dict[str, t.Any]) -> list[t.Any]:
    """Return a dependency-respecting serial execution plan."""
    available_ids = set(scope.keys())
    remaining = list(tasks)
    planned: list[t.Any] = []

    while remaining:
        progressed = False
        for task in list(remaining):
            missing = [src.id for src in task.inputs.values() if src.id not in available_ids]
            if missing:
                continue

            planned.append(task)
            remaining.remove(task)
            progressed = True
            for output in task.outputs.values():
                available_ids.add(output.id)

        if not progressed:
            details = []
            for task in remaining:
                missing = ", ".join(src.id for src in task.inputs.values() if src.id not in available_ids)
                details.append(f"{task.id}: missing [{missing}]")
            raise RuntimeError(
                "Unable to resolve task dependencies for serial execution. "
                + "; ".join(details)
            )

    return planned


def _save_outputs(
    *, sig, arguments: AdagioArguments, scope: dict[str, t.Any], monitor: Monitor | None = None
) -> None:
    if isinstance(arguments.outputs, str):
        os.makedirs(arguments.outputs, exist_ok=True)

    for output in sig.outputs:
        if output.id not in scope:
            raise KeyError(f"Missing output value for {output.name!r} ({output.id}).")

        if isinstance(arguments.outputs, str):
            destination = os.path.join(arguments.outputs, output.name)
        elif isinstance(arguments.outputs, dict):
            destination = arguments.outputs.get(output.name)
            if destination is None:
                expected_outputs = ", ".join(sorted(item.name for item in sig.outputs))
                provided_outputs = ", ".join(sorted(arguments.outputs.keys())) or "<none>"
                raise KeyError(
                    "Missing destination for output "
                    f"{output.name!r}. Expected output names: [{expected_outputs}]. "
                    f"Provided output names: [{provided_outputs}]."
                )
        else:
            raise TypeError("Unsupported outputs configuration.")

        parent = os.path.dirname(destination)
        if parent:
            os.makedirs(parent, exist_ok=True)

        value = scope[output.id]
        save_fn = getattr(value, "save", None)
        if not callable(save_fn):
            raise TypeError(f"Output {output.name!r} does not support save().")
        try:
            save_fn(destination)
        except Exception as exc:  # noqa: BLE001
            if monitor is not None:
                monitor.finish_output(
                    output_id=output.id,
                    output_name=output.name,
                    destination=destination,
                    status="failed",
                    error=str(exc),
                )
            raise
        else:
            if monitor is not None:
                monitor.finish_output(
                    output_id=output.id,
                    output_name=output.name,
                    destination=destination,
                    status="succeeded",
                )


def _is_metadata_ast(ast: TypeAST) -> bool:
    if isinstance(ast, TypeASTExpression):
        return bool(ast.builtin and ast.name.startswith("Metadata"))
    if isinstance(ast, (TypeASTUnion, TypeASTIntersection)):
        return any(_is_metadata_ast(member) for member in ast.members)
    return False


def _task_label(task: t.Any) -> str:
    kind = getattr(task, "kind", "unknown")
    task_id = getattr(task, "id", "<unknown>")
    if kind == "plugin-action":
        plugin = getattr(task, "plugin", "<plugin>")
        action = getattr(task, "action", "<action>")
        return f"{task_id} ({plugin}.{action})"
    if kind == "built-in":
        name = getattr(task, "name", "built-in")
        return f"{task_id} ({name})"
    return task_id


class _action_output_context:
    """Suppress plugin stdout/stderr noise unless explicitly enabled."""

    def __enter__(self):
        mode = os.getenv("ADAGIO_ACTION_STDIO", "").strip().lower()
        self._suppress = mode not in {"inherit", "show", "verbose", "1", "true", "yes"}
        if not self._suppress:
            return self

        self._saved_fds: list[tuple[int, int]] = []
        self._sink = open(os.devnull, "w", encoding="utf-8")
        self._warnings = warnings.catch_warnings()
        self._warnings.__enter__()
        warnings.filterwarnings(
            "ignore",
            message="pkg_resources is deprecated as an API.*",
            category=UserWarning,
        )
        for fd in (1, 2):
            saved = os.dup(fd)
            self._saved_fds.append((fd, saved))
            os.dup2(self._sink.fileno(), fd)
        return self

    def __exit__(self, exc_type, exc, tb):
        if not getattr(self, "_suppress", False):
            return False
        for fd, saved in reversed(self._saved_fds):
            try:
                os.dup2(saved, fd)
            finally:
                os.close(saved)
        self._warnings.__exit__(exc_type, exc, tb)
        self._sink.close()
        return False
