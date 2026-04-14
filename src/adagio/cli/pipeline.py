import json
from contextlib import ExitStack
from pathlib import Path

from cyclopts import App
from rich.console import Console

from ..describe import render_pipeline_text
from ..model.pipeline import AdagioPipeline
from .pipeline_sources import PipelineResolutionError, resolve_pipeline_reference

console = Console()


def run_pipeline_cli(argv: list[str]) -> None:
    app = App(
        name="adagio pipeline",
        help="Inspect pipeline definitions.",
    )
    app.command(show_pipeline, name="show")
    app(argv)


def show_pipeline(pipeline: Path) -> None:
    """Print a pipeline summary to the terminal."""
    with ExitStack() as exit_stack:
        try:
            pipeline_path = resolve_pipeline_reference(pipeline, exit_stack=exit_stack)
        except PipelineResolutionError as error:
            raise SystemExit(str(error)) from error

        data = json.loads(pipeline_path.read_text(encoding="utf-8"))
        pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
        parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
        console.print(render_pipeline_text(parsed_pipeline), soft_wrap=True)
