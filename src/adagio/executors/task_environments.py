import inspect
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.task import (
    ConvertToMetadataTask,
    DataImportTask,
    PluginActionTask,
    RootInputTask,
)
from adagio.monitor.api import Monitor

from .base import (
    PipelineExecutor,
    TaskEnvironmentLauncher,
    TaskEnvironmentResolver,
    TaskEnvironmentSpec,
    TaskExecutionRequest,
)
from .cache_support import ExecutionCacheConfig
from .path_utils import resolve_output_destination
from .serial_runner import SerialExecutionState, TaskOutcome, run_serial_pipeline
from .signature import compute_input_signature, environment_reference
from .task_contract import DATA_IMPORT_PLUGIN, build_task_outputs


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
        cache_config: ExecutionCacheConfig | None = None,
        target_ids: set[str] | None = None,
        log_dir: str | None = None,
    ) -> None:
        consumer_environments = self._build_consumer_environments(pipeline)

        def finish_outputs(
            *,
            sig,
            arguments: AdagioArguments,
            state: SerialExecutionState,
            monitor: Monitor | None,
            require_all: bool = True,
        ) -> None:
            self._materialize_pending_outputs(
                sig=sig,
                state=state,
                console=console,
                consumer_environments=consumer_environments,
            )
            _save_outputs(
                sig=sig,
                arguments=arguments,
                state=state,
                monitor=monitor,
                require_all=require_all,
            )

        run_serial_pipeline(
            pipeline=pipeline,
            arguments=arguments,
            resolve_task=self._resolve_task,
            finish_outputs=finish_outputs,
            console=console,
            monitor=monitor,
            cache_config=cache_config,
            target_ids=target_ids,
            log_dir=log_dir,
        )

    def _build_consumer_environments(self, pipeline) -> dict[str, TaskEnvironmentSpec]:
        """Map each consumed archive id to an environment that can materialize it.

        A data-import artifact has no plugin of its own, so when it must be
        written as a pipeline output the import runs inside the environment of an
        action that consumes it (which already has the QIIME type registered).
        """
        environments: dict[str, TaskEnvironmentSpec] = {}
        for task in pipeline.iter_tasks():
            if not isinstance(task, PluginActionTask):
                continue
            source_ids: list[str] = []
            for src in task.inputs.values():
                if src.kind == "archive":
                    source_ids.append(src.id)
                elif src.kind == "archive-collection":
                    source_ids.extend(item.id for item in src.items)
            if not source_ids:
                continue
            environment = self._environment_resolver.resolve(task=task)
            for source_id in source_ids:
                environments.setdefault(source_id, environment)
        return environments

    def _materialize_pending_outputs(
        self,
        *,
        sig,
        state: SerialExecutionState,
        console: Console | None,
        consumer_environments: dict[str, TaskEnvironmentSpec],
    ) -> None:
        for output in sig.outputs:
            if output.id in state.saved_output_ids:
                continue
            if output.id not in state.materializations:
                continue
            if output.id not in state.scope:
                continue
            self._materialize_output(
                output=output,
                state=state,
                console=console,
                consumer_environments=consumer_environments,
            )

    def _materialize_output(
        self,
        *,
        output,
        state: SerialExecutionState,
        console: Console | None,
        consumer_environments: dict[str, TaskEnvironmentSpec],
    ) -> None:
        environment = consumer_environments.get(output.id)
        if environment is None:
            raise RuntimeError(
                f"Cannot write data-import output {output.name!r} ({output.id}): "
                "raw imports are materialized inside a consuming action's "
                "environment, but no action in this pipeline consumes it. Connect "
                "the artifact to an action, or remove it from the pipeline outputs."
            )
        launcher = self._launchers.get(environment.kind)
        if launcher is None:
            raise RuntimeError(
                f"No task environment launcher registered for kind {environment.kind!r}."
            )

        materialization = _dump_materialization(state.materializations[output.id])
        task_id = f"data-import-{output.id}"
        outputs = build_task_outputs(
            task_id=task_id,
            output_names=["artifact"],
            work_path=state.work_path,
        )
        request = TaskExecutionRequest(
            task=_ImportMaterializeTask(id=task_id),
            cwd=state.cwd,
            work_path=state.work_path,
            archive_inputs={"source": state.scope[output.id]},
            archive_input_materializations={"source": materialization},
            archive_collection_inputs={},
            metadata_inputs={},
            params={},
            metadata_column_kwargs={},
            outputs=outputs,
            cache_path=None,
            recycle_pool=None,
        )
        result = launcher.launch(
            environment=environment,
            request=request,
            console=console,
        )
        artifact_path = result.outputs.get("artifact")
        if not isinstance(artifact_path, str):
            raise RuntimeError(
                f"Importing data-import output {output.name!r} "
                "did not produce an artifact."
            )

        # Replace the raw source with the imported artifact so it is saved as a
        # real .qza and any downstream consumers load it directly.
        state.scope[output.id] = artifact_path
        del state.materializations[output.id]

    def _resolve_task(
        self,
        task,
        state: SerialExecutionState,
        console: Console | None,
    ) -> "bool | TaskOutcome":
        if isinstance(task, RootInputTask):
            for name, src in task.inputs.items():
                dst = task.outputs[name]
                if src.id in state.missing_optional_ids:
                    state.missing_optional_ids.add(dst.id)
                    continue
                state.scope[dst.id] = state.scope[src.id]
            return False

        if isinstance(task, ConvertToMetadataTask):
            if task.inputs["data"].id in state.missing_optional_ids:
                state.missing_optional_ids.add(task.outputs["metadata"].id)
                return False
            state.scope[task.outputs["metadata"].id] = state.scope[
                task.inputs["data"].id
            ]
            return False

        if isinstance(task, DataImportTask):
            self._resolve_data_import(task=task, state=state)
            return False

        if isinstance(task, PluginActionTask):
            return self._execute_plugin_action(
                task=task,
                state=state,
                console=console,
            )

        raise TypeError(f"Unsupported task type: {type(task)}")

    def _resolve_data_import(
        self,
        *,
        task: DataImportTask,
        state: SerialExecutionState,
    ) -> None:
        src = task.inputs.get("source")
        dst = task.outputs.get("artifact")
        if src is None or dst is None:
            raise ValueError(
                "Data import tasks require source input and artifact output."
            )
        if src.kind != "archive" or dst.kind != "archive":
            raise TypeError(
                "Data import tasks require archive source and artifact values."
            )

        if src.id in state.missing_optional_ids:
            state.missing_optional_ids.add(dst.id)
            return

        semantic_type = _resolve_task_parameter(
            task=task,
            state=state,
            name="semantic_type",
        )
        input_format = _resolve_task_parameter(
            task=task,
            state=state,
            name="input_format",
            default=None,
        )
        validate_level = _resolve_task_parameter(
            task=task,
            state=state,
            name="validate_level",
            default="max",
        )

        if not isinstance(semantic_type, str) or not semantic_type:
            raise ValueError("Data import task is missing semantic_type.")
        if input_format is not None and (
            not isinstance(input_format, str) or not input_format
        ):
            raise ValueError("Data import task has invalid input_format.")
        if validate_level not in {"min", "max"}:
            raise ValueError(
                f"Data import task has invalid validate_level {validate_level!r}."
            )

        state.scope[dst.id] = state.scope[src.id]
        materialization = {
            "mode": "raw",
            "semantic_type": semantic_type,
            "validate_level": validate_level,
        }
        if input_format is not None:
            materialization["input_format"] = input_format
        state.materializations[dst.id] = materialization

    def _execute_plugin_action(
        self,
        *,
        task: PluginActionTask,
        state: SerialExecutionState,
        console: Console | None,
    ) -> TaskOutcome:
        environment = self._environment_resolver.resolve(task=task)
        launcher = self._launchers.get(environment.kind)
        if launcher is None:
            raise RuntimeError(
                f"No task environment launcher registered for kind {environment.kind!r}."
            )

        archive_inputs: dict[str, str] = {}
        archive_input_materializations: dict[str, dict[str, object]] = {}
        archive_collection_inputs: dict[str, list[str]] = {}
        metadata_inputs: dict[str, str] = {}
        for name, src in task.inputs.items():
            if src.kind == "archive":
                if src.id in state.missing_optional_ids:
                    continue
                value = state.scope[src.id]
                if isinstance(value, list):
                    archive_collection_inputs[name] = value
                elif isinstance(value, dict):
                    archive_collection_inputs[name] = list(value.values())
                else:
                    archive_inputs[name] = value
                    materialization = state.materializations.get(src.id)
                    if materialization is not None:
                        archive_input_materializations[name] = _dump_materialization(
                            materialization
                        )
            elif src.kind == "archive-collection":
                values = _present_collection_item_values(
                    items=src.items,
                    state=state,
                )
                if values:
                    archive_collection_inputs[name] = _flatten_collection_items(values)
            elif src.kind == "metadata":
                if src.id in state.missing_optional_ids:
                    continue
                value = state.scope[src.id]
                if not isinstance(value, str):
                    raise TypeError(
                        f"Metadata input {name!r} must resolve to a single path."
                    )
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
                    raise TypeError(
                        f"Unsupported metadata column kind: {column.kind!r}"
                    )
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
            archive_input_materializations=archive_input_materializations,
            archive_collection_inputs=archive_collection_inputs,
            metadata_inputs=metadata_inputs,
            params=resolved_params,
            metadata_column_kwargs=metadata_column_kwargs,
            outputs=outputs,
            cache_path=(
                str(state.cache_config.cache_dir)
                if state.cache_config is not None
                else None
            ),
            recycle_pool=(
                state.cache_config.recycle_pool
                if state.cache_config is not None
                else None
            ),
        )
        # Per-node input signature (design §1.5): content-hash file inputs, hash
        # the resolved params, and pin the environment reference. Best-effort:
        # never let signature computation fail a task.
        input_signature: str | None = None
        try:
            signature_inputs: dict[str, object] = {}
            signature_inputs.update(archive_inputs)
            signature_inputs.update(
                {name: values for name, values in archive_collection_inputs.items()}
            )
            signature_inputs.update(metadata_inputs)
            input_signature = compute_input_signature(
                params=resolved_params,
                inputs=signature_inputs,
                env=environment_reference(
                    kind=environment.kind, reference=environment.reference
                ),
            )
        except Exception:  # noqa: BLE001
            input_signature = None

        result = _launch(
            launcher,
            environment=environment,
            request=request,
            console=console,
            monitor=state.monitor,
            task_id=task.id,
        )

        for output_name, dest in task.outputs.items():
            actual_path = result.outputs.get(output_name)
            if not isinstance(actual_path, str):
                raise RuntimeError(
                    f"Task {task.id!r} did not produce output {output_name!r}."
                )
            state.scope[dest.id] = actual_path

        enrichment: dict[str, object] = {}
        if input_signature is not None:
            enrichment["input_signature"] = input_signature
        if result.command is not None:
            enrichment["command"] = result.command
        if result.exit_code is not None:
            enrichment["exit_code"] = result.exit_code
        if result.image_ref is not None:
            enrichment["image_ref"] = result.image_ref
        if result.image_digest is not None:
            enrichment["image_digest"] = result.image_digest
        if result.timings is not None:
            enrichment["timings"] = dict(result.timings)
            # Minimal resources block (design §4.2 NodeResources): wall time is
            # the only value the host can measure honestly for every launcher.
            # peak_rss/cpu/disk stay unset until in-container measurement lands.
            run_seconds = result.timings.get("run_seconds")
            if isinstance(run_seconds, (int, float)):
                enrichment["resources"] = {"wall_seconds": float(run_seconds)}
        if result.log_path is not None:
            enrichment["log_path"] = result.log_path
        enrichment["reused"] = result.reused

        return TaskOutcome(reused=result.reused, enrichment=enrichment)


def _launch(launcher, **kwargs):  # noqa: ANN001, ANN003
    """Call ``launcher.launch`` passing only the kwargs it accepts.

    The launcher protocol gained optional ``monitor`` / ``task_id`` parameters
    for fine-phase telemetry. Third-party launchers (and test doubles) written
    against the older signature do not accept them; filter to the callable's
    real parameters so those keep working unchanged.
    """
    try:
        signature = inspect.signature(launcher.launch)
    except (TypeError, ValueError):
        return launcher.launch(**kwargs)
    params = signature.parameters
    accepts_var_kw = any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    if accepts_var_kw:
        return launcher.launch(**kwargs)
    accepted = {name: value for name, value in kwargs.items() if name in params}
    return launcher.launch(**accepted)


@dataclass(frozen=True)
class _ImportMaterializeTask:
    """Synthetic task for materializing a raw data-import artifact as an output.

    Carries just the attributes the launchers read (``id``/``plugin``/``action``);
    ``plugin == DATA_IMPORT_PLUGIN`` tells the in-environment runner to import
    rather than invoke a QIIME action.
    """

    id: str
    plugin: str = DATA_IMPORT_PLUGIN
    action: str = "import"


def _resolve_task_parameter(
    *,
    task: DataImportTask,
    state: SerialExecutionState,
    name: str,
    default: object | None = None,
) -> object | None:
    param = task.parameters.get(name)
    if param is None:
        return default
    if param.kind == "literal":
        return param.value
    if param.kind == "promoted":
        return state.params[param.id]
    raise TypeError(f"Data import parameter {name!r} cannot use kind {param.kind!r}.")


def _dump_materialization(materialization) -> dict[str, object]:  # noqa: ANN001
    if hasattr(materialization, "model_dump"):
        return dict(materialization.model_dump(exclude_none=True))
    if isinstance(materialization, dict):
        return dict(materialization)
    raise TypeError(f"Unsupported input materialization: {materialization!r}")


def _flatten_collection_items(
    values: list[str | list[str] | dict[str, str]],
) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, list):
            result.extend(value)
        elif isinstance(value, dict):
            result.extend(value.values())
        else:
            result.append(value)
    return result


def _present_collection_item_values(
    *,
    items,
    state: SerialExecutionState,
) -> list[str | list[str] | dict[str, str]]:
    values: list[str | list[str] | dict[str, str]] = []
    for item in items:
        if item.id in state.missing_optional_ids:
            continue
        values.append(state.scope[item.id])
    return values


def _save_outputs(
    *,
    sig,
    arguments: AdagioArguments,
    state: SerialExecutionState,
    monitor: Monitor | None,
    require_all: bool = True,
) -> None:
    if isinstance(arguments.outputs, str):
        os.makedirs(arguments.outputs, exist_ok=True)

    for output in sig.outputs:
        if output.id in state.saved_output_ids:
            continue
        if output.id not in state.scope:
            # ``require_all`` demands a produced value, but for a partial run an
            # output whose producing task was pruned away is not expected — only
            # the target closure's outputs (``expected_output_ids``) are.
            required = require_all and (
                state.expected_output_ids is None
                or output.id in state.expected_output_ids
            )
            if required:
                raise KeyError(
                    f"Missing output value for {output.name!r} ({output.id})."
                )
            continue

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

        if monitor is not None and not state.save_output_started:
            monitor.start_save_output()
            state.save_output_started = True

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
            state.saved_output_ids.add(output.id)
