from __future__ import annotations

import os
import shutil
import tempfile
import typing as t
from pathlib import Path

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import PluginActionTask, RootInputTask
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor
from adagio.monitor.tty import RichMonitor

from .base import (
    PipelineExecutor,
    TaskEnvironmentLauncher,
    TaskEnvironmentResolver,
    TaskExecutionRequest,
)
from .common import plan_execution_order, task_label
from .container_support import is_uri

CONTAINER_SUBTASK_COUNT = 1


class TaskEnvironmentExecutor(PipelineExecutor):
    mode_label = "per-task environment mode"

    def __init__(
        self,
        *,
        environment_resolver: TaskEnvironmentResolver,
        launchers: dict[str, TaskEnvironmentLauncher],
    ) -> None:
        self._environment_resolver = environment_resolver
        self._launchers = dict(launchers)

    def execute(
        self,
        *,
        pipeline: AdagioPipeline,
        arguments: AdagioArguments,
        console: Console | None = None,
        monitor: Monitor | None = None,
    ) -> None:
        sig = pipeline.signature
        tasks = list(pipeline.iter_tasks())
        active_monitor = _resolve_monitor(console=console, monitor=monitor)

        pipeline.validate_graph()
        sig.validate_arguments(arguments)

        active_monitor.start_pipeline(total_tasks=len(tasks))

        with tempfile.TemporaryDirectory(prefix="adagio-work-") as work_dir:
            work_path = Path(work_dir)
            scope: dict[str, str] = {}
            completed_task_ids: set[str] = set()
            cwd = Path.cwd().resolve()

            active_monitor.start_load_input()
            for input_def in sig.inputs:
                source = arguments.inputs[input_def.name]
                scope[input_def.id] = _resolve_host_path(source=source, cwd=cwd)
            active_monitor.finish_load_input()

            params = sig.get_params(arguments)
            execution_plan = plan_execution_order(tasks=tasks, scope=scope)

            for task in execution_plan:
                active_monitor.queue_task(
                    task_id=task.id,
                    label=task_label(task),
                    total_subtasks=CONTAINER_SUBTASK_COUNT,
                )

            try:
                for task in execution_plan:
                    active_monitor.start_task(task_id=task.id)
                    try:
                        self._execute_task(
                            task=task,
                            params=params,
                            scope=scope,
                            work_path=work_path,
                            cwd=cwd,
                            console=console,
                        )
                        active_monitor.advance_task(task_id=task.id, advance=1)
                        active_monitor.finish_task(task_id=task.id, status="completed")
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
                _save_outputs(
                    sig=sig,
                    arguments=arguments,
                    scope=scope,
                    monitor=active_monitor,
                )
                active_monitor.finish_save_output()
            finally:
                active_monitor.finish_pipeline()

    def _execute_task(
        self,
        *,
        task: t.Any,
        params: dict[str, t.Any],
        scope: dict[str, str],
        work_path: Path,
        cwd: Path,
        console: Console | None,
    ) -> None:
        if isinstance(task, RootInputTask):
            for name, src in task.inputs.items():
                dst = task.outputs[name]
                scope[dst.id] = scope[src.id]
            return

        if isinstance(task, PluginActionTask):
            self._execute_plugin_action(
                task=task,
                params=params,
                scope=scope,
                work_path=work_path,
                cwd=cwd,
                console=console,
            )
            return

        raise TypeError(f"Unsupported task type: {type(task)}")

    def _execute_plugin_action(
        self,
        *,
        task: PluginActionTask,
        params: dict[str, t.Any],
        scope: dict[str, str],
        work_path: Path,
        cwd: Path,
        console: Console | None,
    ) -> None:
        environment = self._environment_resolver.resolve(task=task)
        launcher = self._launchers.get(environment.kind)
        if launcher is None:
            raise RuntimeError(
                f"No task environment launcher registered for kind {environment.kind!r}."
            )

        archive_inputs: dict[str, str] = {}
        metadata_inputs: dict[str, str] = {}
        for name, src in task.inputs.items():
            value = scope[src.id]
            if src.kind == "archive":
                archive_inputs[name] = value
            elif src.kind == "metadata":
                metadata_inputs[name] = value
            else:
                raise TypeError(f"Unsupported input kind: {src.kind!r}")

        resolved_params: dict[str, t.Any] = {}
        metadata_column_kwargs: dict[str, dict[str, str]] = {}
        for name, param in task.parameters.items():
            if param.kind == "literal":
                resolved_params[name] = param.value
            elif param.kind == "promoted":
                resolved_params[name] = params[param.id]
            elif param.kind == "metadata":
                column = param.column
                if column.kind == "literal":
                    column_name = str(column.value)
                elif column.kind == "promoted":
                    column_name = str(params[column.id])
                else:
                    raise TypeError(f"Unsupported metadata column kind: {column.kind!r}")
                metadata_column_kwargs[name] = {"source": name, "column": column_name}
            else:
                raise TypeError(f"Unsupported parameter kind: {param.kind!r}")

        safe_id = task.id.replace("/", "_").replace(" ", "_")
        outputs = {
            name: str((work_path / f"{safe_id}_{name}").resolve())
            for name in task.outputs
        }
        request = TaskExecutionRequest(
            task=task,
            cwd=cwd,
            work_path=work_path,
            archive_inputs=archive_inputs,
            metadata_inputs=metadata_inputs,
            params=resolved_params,
            metadata_column_kwargs=metadata_column_kwargs,
            outputs=outputs,
        )
        result = launcher.launch(
            environment=environment,
            request=request,
            console=console,
        )

        for output_name, dest in task.outputs.items():
            actual_path = result.outputs.get(output_name)
            if not isinstance(actual_path, str):
                raise RuntimeError(
                    f"Task {task.id!r} did not produce output {output_name!r}."
                )
            scope[dest.id] = actual_path


def _resolve_monitor(*, console: Console | None, monitor: Monitor | None) -> Monitor:
    if monitor is not None:
        return monitor
    if console is not None:
        return RichMonitor(console=console)
    return LogMonitor()


def _save_outputs(
    *,
    sig,
    arguments: AdagioArguments,
    scope: dict[str, str],
    monitor: Monitor | None,
) -> None:
    if isinstance(arguments.outputs, str):
        os.makedirs(arguments.outputs, exist_ok=True)

    for output in sig.outputs:
        if output.id not in scope:
            raise KeyError(f"Missing output value for {output.name!r} ({output.id}).")

        source_path = Path(scope[output.id])
        destination = _resolve_output_destination(
            output_name=output.name,
            outputs=arguments.outputs,
            source_path=source_path,
            sig=sig,
        )

        parent = os.path.dirname(destination)
        if parent:
            os.makedirs(parent, exist_ok=True)

        try:
            shutil.copy2(source_path, destination)
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


def _resolve_output_destination(
    *,
    output_name: str,
    outputs: str | dict[str, str],
    source_path: Path,
    sig,
) -> str:
    suffix = source_path.suffix

    if isinstance(outputs, str):
        return _append_output_suffix(os.path.join(outputs, output_name), suffix)

    if isinstance(outputs, dict):
        raw_dest = outputs.get(output_name)
        if raw_dest is None:
            expected_outputs = ", ".join(sorted(item.name for item in sig.outputs))
            provided_outputs = ", ".join(sorted(outputs.keys())) or "<none>"
            raise KeyError(
                "Missing destination for output "
                f"{output_name!r}. Expected output names: [{expected_outputs}]. "
                f"Provided output names: [{provided_outputs}]."
            )
        return _append_output_suffix(raw_dest, suffix)

    raise TypeError("Unsupported outputs configuration.")


def _append_output_suffix(destination: str, suffix: str) -> str:
    if suffix and not destination.endswith(suffix):
        return destination + suffix
    return destination


def _resolve_host_path(*, source: str, cwd: Path) -> str:
    if is_uri(source):
        return source
    path = Path(source)
    if path.is_absolute():
        return str(path.resolve())
    return str((cwd / path).resolve())
