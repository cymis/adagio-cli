import os
import shutil
from pathlib import Path

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.task import PluginActionTask, RootInputTask
from adagio.monitor.api import Monitor

from .base import (
    PipelineExecutor,
    TaskEnvironmentLauncher,
    TaskEnvironmentResolver,
    TaskExecutionRequest,
)
from .path_utils import resolve_output_destination
from .serial_runner import SerialExecutionState, run_serial_pipeline
from .task_contract import build_task_outputs


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
        pipeline,
        arguments: AdagioArguments,
        console: Console | None = None,
        monitor: Monitor | None = None,
    ) -> None:
        run_serial_pipeline(
            pipeline=pipeline,
            arguments=arguments,
            resolve_task=self._resolve_task,
            finish_outputs=_save_outputs,
            console=console,
            monitor=monitor,
        )

    def _resolve_task(
        self,
        task,
        state: SerialExecutionState,
        console: Console | None,
    ) -> None:
        if isinstance(task, RootInputTask):
            for name, src in task.inputs.items():
                dst = task.outputs[name]
                state.scope[dst.id] = state.scope[src.id]
            return

        if isinstance(task, PluginActionTask):
            self._execute_plugin_action(
                task=task,
                state=state,
                console=console,
            )
            return

        raise TypeError(f"Unsupported task type: {type(task)}")

    def _execute_plugin_action(
        self,
        *,
        task: PluginActionTask,
        state: SerialExecutionState,
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
            value = state.scope[src.id]
            if src.kind == "archive":
                archive_inputs[name] = value
            elif src.kind == "metadata":
                metadata_inputs[name] = value
            else:
                raise TypeError(f"Unsupported input kind: {src.kind!r}")

        resolved_params: dict[str, object] = {}
        metadata_column_kwargs: dict[str, dict[str, str]] = {}
        for name, param in task.parameters.items():
            if param.kind == "literal":
                resolved_params[name] = param.value
            elif param.kind == "promoted":
                resolved_params[name] = state.params[param.id]
            elif param.kind == "metadata":
                column = param.column
                if column.kind == "literal":
                    column_name = str(column.value)
                elif column.kind == "promoted":
                    column_name = str(state.params[column.id])
                else:
                    raise TypeError(f"Unsupported metadata column kind: {column.kind!r}")
                metadata_column_kwargs[name] = {"source": name, "column": column_name}
            else:
                raise TypeError(f"Unsupported parameter kind: {param.kind!r}")

        outputs = build_task_outputs(
            task_id=task.id,
            output_names=task.outputs.keys(),
            work_path=state.work_path,
        )
        request = TaskExecutionRequest(
            task=task,
            cwd=state.cwd,
            work_path=state.work_path,
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
            state.scope[dest.id] = actual_path


def _save_outputs(
    *,
    sig,
    arguments: AdagioArguments,
    state: SerialExecutionState,
    monitor: Monitor | None,
) -> None:
    if isinstance(arguments.outputs, str):
        os.makedirs(arguments.outputs, exist_ok=True)

    for output in sig.outputs:
        if output.id not in state.scope:
            raise KeyError(f"Missing output value for {output.name!r} ({output.id}).")

        source_path = Path(state.scope[output.id])
        destination = resolve_output_destination(
            output_name=output.name,
            output_names=[item.name for item in sig.outputs],
            outputs=arguments.outputs,
            source_path=source_path,
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
