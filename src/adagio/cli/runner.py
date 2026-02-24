import json
import sys
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
    from ..dummy_execute import (
        DummyExecutionConfig,
        DummyExecutionFailed,
        execute_dummy_pipeline,
    )
    from ..model.pipeline import AdagioPipeline
    from ..monitor.tty import RichMonitor

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
    arguments = parsed_pipeline.signature.to_default_arguments()

    arguments_file = kwargs.pop("arguments_file", None)
    if arguments_file is not None:
        _merge_arguments_file(arguments, Path(arguments_file))

    dummy_enabled = bool(kwargs.pop("dummy", True))
    dummy_config = DummyExecutionConfig(
        min_seconds=float(kwargs.pop("dummy_min_seconds", 10.0)),
        max_seconds=float(kwargs.pop("dummy_max_seconds", 15.0)),
        fail_rate=float(kwargs.pop("dummy_fail_rate", 0.0)),
        subtasks=int(kwargs.pop("dummy_subtasks", 3)),
        seed=kwargs.pop("dummy_seed", None),
    )

    for ident, original in input_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.inputs[original] = str(value)

    for ident, original in param_bindings:
        if ident in kwargs:
            arguments.parameters[original] = kwargs.get(ident)

    console.print(f"[bold]Pipeline:[/bold] {pipeline}")
    console.print(
        f"[bold]Executing pipeline[/bold] ({'dummy' if dummy_enabled else 'runtime'} mode)"
    )

    try:
        if dummy_enabled:
            execute_dummy_pipeline(
                pipeline=parsed_pipeline,
                arguments=arguments,
                monitor=RichMonitor(console=console),
                dummy=dummy_config,
            )
        else:
            raise SystemExit(
                "Runtime execution is temporarily disabled. "
                "Use default dummy mode (or pass --dummy)."
            )
    except DummyExecutionFailed as exc:
        raise SystemExit(str(exc)) from exc
    except (ModuleNotFoundError, ImportError) as exc:
        if dummy_enabled:
            raise
        missing = getattr(exc, "name", None) or "unknown"
        raise SystemExit(
            "Execution dependencies are missing. "
            f"Missing module: {missing!r}. "
            f"Details: {exc}. "
            f"Python executable: {sys.executable}. "
            "Install runtime requirements (for example, qiime2/parsl) in that same environment."
        ) from exc


def _merge_arguments_file(arguments, arguments_file: Path) -> None:
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
