import json
from pathlib import Path
from typing import Any

from rich.console import Console


def run_pipeline_from_kwargs(
    pipeline: Path,
    kwargs: dict[str, Any],
    input_bindings: list[tuple[str, str]],
    param_bindings: list[tuple[str, str]],
    *,
    console: Console,
) -> None:
    """Run a pipeline command from resolved CLI keyword arguments."""
    from ..dummy_execute import execute
    from ..model.pipeline import AdagioPipeline
    from ..monitor.tty import RichMonitor

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
    arguments = parsed_pipeline.signature.to_default_arguments()

    arguments_file = kwargs.pop("arguments_file", None)
    if arguments_file is not None:
        _merge_arguments_file(arguments, Path(arguments_file))

    for ident, original in input_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.inputs[original] = str(value)

    for ident, original in param_bindings:
        if ident in kwargs:
            arguments.parameters[original] = kwargs.get(ident)

    console.print(f"[bold]Pipeline:[/bold] {pipeline}")
    console.print("[bold]Executing pipeline[/bold] (dummy mode)")
    execute(
        pipeline=parsed_pipeline,
        arguments=arguments,
        monitor=RichMonitor(console=console),
    )


def _merge_arguments_file(arguments, arguments_file: Path) -> None:
    """Merge values from an arguments file into runtime arguments."""
    try:
        text = arguments_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Unable to read arguments file: {arguments_file}") from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in arguments file: {arguments_file}") from exc

    if not isinstance(payload, dict):
        raise SystemExit(f"Invalid arguments file format: {arguments_file}")

    inputs = payload.get("inputs")
    if isinstance(inputs, dict):
        for key, value in inputs.items():
            arguments.inputs[key] = str(value)

    params = payload.get("parameters")
    if isinstance(params, dict):
        for key, value in params.items():
            arguments.parameters[key] = value

    if "outputs" in payload:
        outputs = payload["outputs"]
        if isinstance(outputs, str):
            arguments.outputs = outputs
        elif isinstance(outputs, dict):
            arguments.outputs = {
                str(key): str(value) for key, value in outputs.items()
            }
        else:
            raise SystemExit(f"Invalid outputs in arguments file: {arguments_file}")
