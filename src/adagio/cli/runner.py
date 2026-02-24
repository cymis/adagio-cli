import json
from pathlib import Path
from typing import Any

from rich.console import Console


def run_pipeline_from_kwargs(
    pipeline: Path,
    arguments_file: Path | None,
    kwargs: dict[str, Any],
    input_bindings: list[tuple[str, str]],
    param_bindings: list[tuple[str, str]],
    required_inputs: list[str],
    required_params: list[str],
    *,
    console: Console,
) -> None:
    """Run a pipeline from resolved CLI keyword arguments."""
    from ..dummy_execute import execute
    from ..model.arguments import AdagioArgumentsFile
    from ..model.pipeline import AdagioPipeline
    from ..monitor.tty import RichMonitor

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
    arguments = parsed_pipeline.signature.to_default_arguments()

    input_names = {name for _, name in input_bindings}
    param_names = {name for _, name in param_bindings}

    if arguments_file is not None:
        file_data = json.loads(arguments_file.read_text(encoding="utf-8"))
        arguments_data = AdagioArgumentsFile.model_validate(file_data)

        unknown_inputs = sorted(set(arguments_data.inputs) - input_names)
        if unknown_inputs:
            raise SystemExit(
                "Unknown inputs in arguments file: " + ", ".join(unknown_inputs)
            )

        unknown_params = sorted(set(arguments_data.parameters) - param_names)
        if unknown_params:
            raise SystemExit(
                "Unknown parameters in arguments file: " + ", ".join(unknown_params)
            )

        arguments.inputs.update(arguments_data.inputs)
        arguments.parameters.update(arguments_data.parameters)
        if arguments_data.outputs:
            arguments.outputs = arguments_data.outputs

    for ident, original in input_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.inputs[original] = str(value)

    for ident, original in param_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.parameters[original] = value

    missing_inputs = [
        name for name in required_inputs if _is_missing(arguments.inputs.get(name))
    ]
    missing_params = [
        name for name in required_params if _is_missing(arguments.parameters.get(name))
    ]
    if missing_inputs or missing_params:
        missing = [f"input:{name}" for name in missing_inputs] + [
            f"param:{name}" for name in missing_params
        ]
        raise SystemExit("Missing required arguments: " + ", ".join(missing))

    console.print(f"[bold]Pipeline:[/bold] {pipeline}")
    console.print("[bold]Executing pipeline[/bold] (dummy mode)")
    execute(
        pipeline=parsed_pipeline,
        arguments=arguments,
        monitor=RichMonitor(console=console),
    )


def _is_missing(value: Any) -> bool:
    """Treat placeholders and null values as missing."""
    return value is None or value == "<fill me>"
