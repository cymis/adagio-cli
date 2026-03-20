import json
import os
import sys
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from .config import load_run_config
from ..executors.cache_support import (
    describe_cache_config,
    resolve_cache_config,
)


def _error_exit(console: Console, message: str) -> None:
    panel = Panel(
        Text.from_markup(message),
        title="Error",
        border_style="red",
        box=box.ROUNDED,
        expand=True,
        title_align="left",
    )
    console.print(panel)
    sys.exit(1)

DEFAULT_OUTPUT_DIRNAME = "adagio-outputs"


def run_pipeline_from_kwargs(
    pipeline: Path,
    arguments_file: Path | None,
    config_file: Path | None,
    kwargs: dict[str, Any],
    input_bindings: list[tuple[str, str]],
    param_bindings: list[tuple[str, str]],
    required_inputs: list[str],
    required_params: list[str],
    *,
    console: Console,
) -> None:
    """Run a pipeline from resolved CLI keyword arguments."""
    from ..model.arguments import AdagioArgumentsFile
    from ..model.pipeline import AdagioPipeline

    cache_dir = kwargs.pop("cache_dir", None)
    reuse = bool(kwargs.pop("reuse", True))

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
    arguments = parsed_pipeline.signature.to_default_arguments()
    run_config = load_run_config(config_file)
    output_names = [output.name for output in parsed_pipeline.signature.outputs]

    input_names = {name for _, name in input_bindings}
    param_names = {name for _, name in param_bindings}
    output_name_set = set(output_names)

    if arguments_file is not None:
        file_data = json.loads(arguments_file.read_text(encoding="utf-8"))
        arguments_data = AdagioArgumentsFile.model_validate(file_data)

        unknown_inputs = sorted(set(arguments_data.inputs) - input_names)
        if unknown_inputs:
            _error_exit(console, "Unknown inputs in arguments file: " + ", ".join(unknown_inputs))

        unknown_params = sorted(set(arguments_data.parameters) - param_names)
        if unknown_params:
            _error_exit(console, "Unknown parameters in arguments file: " + ", ".join(unknown_params))

        unknown_outputs: list[str] = []
        if isinstance(arguments_data.outputs, dict):
            unknown_outputs = sorted(set(arguments_data.outputs) - output_name_set)
        if unknown_outputs:
            _error_exit(console, "Unknown outputs in arguments file: " + ", ".join(unknown_outputs))

        arguments.inputs.update(arguments_data.inputs)
        arguments.parameters.update(arguments_data.parameters)
        if arguments_data.outputs is not None:
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
        missing_opts = (
            [f"--input-{n.replace('_', '-')}" for n in missing_inputs]
            + [f"--param-{n.replace('_', '-')}" for n in missing_params]
        )
        formatted = ", ".join(f"[cyan]{opt}[/cyan]" for opt in missing_opts)
        _error_exit(console, f"Missing required arguments: {formatted}")

    arguments.outputs = _resolve_output_destinations(
        outputs=arguments.outputs,
        output_names=output_names,
        cwd=Path.cwd().resolve(),
    )

    suppress_header = _is_truthy(os.getenv("ADAGIO_SUPPRESS_RUN_HEADER"))
    if not suppress_header:
        console.print(f"[bold]Pipeline:[/bold] {pipeline}")

    cache_config = resolve_cache_config(
        cwd=Path.cwd().resolve(),
        cache_dir=cache_dir,
        reuse=reuse,
    )

    if not suppress_header:
        console.print(f"[bold]Cache:[/bold] {describe_cache_config(cache_config)}")

    from ..executors import select_default_executor

    executor = select_default_executor(
        plugin_image_overrides=(
            {name: override.image for name, override in run_config.plugins.items()}
            if run_config is not None
            else None
        ),
        task_image_overrides=(
            {name: override.image for name, override in run_config.tasks.items()}
            if run_config is not None
            else None
        )
    )

    if not suppress_header:
        console.print(f"[bold]Executing pipeline[/bold] ({executor.mode_label})")

    executor.execute(
        pipeline=parsed_pipeline,
        arguments=arguments,
        console=console,
        cache_config=cache_config,
    )


def _is_missing(value: Any) -> bool:
    """Treat placeholders and null values as missing."""
    return value is None or value == "<fill me>"


def _is_missing_output(value: Any) -> bool:
    if not isinstance(value, str):
        return True
    return value == "" or value == "<fill me>"


def _resolve_output_destinations(
    *,
    outputs: str | dict[str, str],
    output_names: list[str],
    cwd: Path,
) -> str | dict[str, str]:
    default_output_dir = (cwd / DEFAULT_OUTPUT_DIRNAME).resolve()
    if isinstance(outputs, str):
        if _is_missing_output(outputs):
            return str(default_output_dir)
        return outputs

    if not isinstance(outputs, dict):
        raise TypeError("Unsupported outputs configuration.")

    resolved = dict(outputs)
    for output_name in output_names:
        value = resolved.get(output_name)
        if _is_missing_output(value):
            resolved[output_name] = str((default_output_dir / output_name).resolve())
    return resolved


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}
