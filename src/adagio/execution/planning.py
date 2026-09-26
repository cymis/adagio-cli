"""Internal whole-action plan inspection without importing scientific code."""

from pathlib import Path

from adagio.executors.common import plan_execution_order, prune_to_targets
from adagio.executors.serial_runner import (
    SerialExecutionState,
    _is_missing,
    resolve_pipeline_input,
)
from adagio.model.task import DataImportTask, PluginActionTask, input_source_ids


def inspect_plan(*, executor, pipeline, arguments, target_ids=None):
    pipeline.validate_graph()
    pipeline.signature.validate_arguments(arguments)
    executor._validate_closure_environments(pipeline=pipeline, target_ids=target_ids)
    tasks = list(pipeline.iter_tasks())
    if target_ids:
        tasks = prune_to_targets(execution_plan=tasks, target_ids=target_ids)
    task_ids = [task.id for task in tasks]
    output_ids = [out.id for task in tasks for out in task.outputs.values()]
    if len(set(task_ids)) != len(task_ids) or len(set(output_ids)) != len(output_ids):
        raise ValueError("Pipeline task IDs and produced output IDs must be unique.")
    state = SerialExecutionState(
        cwd=Path.cwd().resolve(),
        work_path=Path.cwd().resolve(),
        params=pipeline.signature.get_params(arguments),
        scope={},
        cache_config=None,
    )
    for definition in pipeline.signature.inputs:
        value = arguments.inputs.get(definition.name)
        if _is_missing(value):
            if not definition.required:
                state.missing_optional_ids.add(definition.id)
        else:
            state.scope[definition.id] = resolve_pipeline_input(
                source=value, type_name=definition.type, cwd=state.cwd
            )
    needed_inputs = {
        i
        for task in tasks
        for src in task.inputs.values()
        for i in input_source_ids(src)
    }
    needed_params = {
        p.id for task in tasks for p in task.parameters.values() if p.kind == "promoted"
    }
    missing = [
        f"input:{d.name}"
        for d in pipeline.signature.inputs
        if d.required
        and d.id in needed_inputs
        and _is_missing(arguments.inputs.get(d.name))
    ]
    missing += [
        f"param:{d.name}"
        for d in pipeline.signature.parameters
        if d.required and d.id in needed_params and _is_missing(state.params.get(d.id))
    ]
    if missing:
        raise ValueError("Missing required runtime arguments: " + ", ".join(missing))
    tasks = plan_execution_order(
        tasks=tasks, scope=state.scope, optional_missing_ids=state.missing_optional_ids
    )
    config = executor.run_config
    nodes = []
    for task in tasks:
        for name, param in task.parameters.items():
            if param.kind == "promoted" and param.id not in state.params:
                raise ValueError(f"Task {task.id!r} parameter {name!r} is unresolved.")
            if (
                param.kind == "metadata"
                and param.column.kind == "promoted"
                and param.column.id not in state.params
            ):
                raise ValueError(
                    f"Task {task.id!r} metadata column {name!r} is unresolved."
                )
        if isinstance(task, DataImportTask):
            # Validate import configuration without opening/importing scientific data.
            source = task.inputs.get("source")
            if source and source.id not in state.scope:
                state.scope[source.id] = f"<upstream:{source.id}>"
            executor._resolve_data_import(task=task, state=state)
            for output in pipeline.signature.outputs:
                if output.id in {item.id for item in task.outputs.values()}:
                    from adagio.executors.task_environments import _build_consumer_tasks

                    environment = executor._resolve_materialization_environment(
                        output=output, consumer_tasks=_build_consumer_tasks(pipeline)
                    )
                    if (
                        config
                        and config.executor.kind == "slurm"
                        and environment.kind not in {"conda", "apptainer"}
                    ):
                        raise ValueError(
                            "Slurm supports Apptainer or shared Conda environments; Docker is unsupported."
                        )
        environment = (
            executor._environment_resolver.resolve(task=task)
            if isinstance(task, PluginActionTask)
            else None
        )
        nodes.append(
            {
                "node_id": task.id,
                "binding": {},
                "kind": task.kind,
                "inputs": [
                    i for src in task.inputs.values() for i in input_source_ids(src)
                ],
                "outputs": {name: out.id for name, out in task.outputs.items()},
                "environment": {
                    "kind": environment.kind,
                    "reference": environment.reference,
                }
                if environment
                else None,
                "resources": config.resources.for_task(task.id).model_dump(
                    exclude_none=True
                )
                if config
                else {"cpus": 1},
            }
        )
    return {
        "executor": config.executor.model_dump(exclude_none=True)
        if config
        else {"kind": "serial"},
        "tasks": nodes,
    }
