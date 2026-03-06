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
    try:
        from ..execute import execute_pipeline
        from ..model.pipeline import AdagioPipeline
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Execution dependencies are missing. "
            "Install runtime requirements (for example, qiime2/parsl) to run pipelines."
        ) from exc

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    parsed_pipeline = AdagioPipeline.model_validate(data)
    arguments = parsed_pipeline.signature.to_default_arguments()

    for ident, original in input_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.inputs[original] = str(value)

    for ident, original in param_bindings:
        if ident in kwargs:
            arguments.parameters[original] = kwargs.get(ident)

    console.print(f"[bold]Pipeline:[/bold] {pipeline}")
    console.print("[bold]Executing pipeline[/bold]")
    execute_pipeline(pipeline=parsed_pipeline, arguments=arguments)
