import json
from contextlib import ExitStack
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from ..bundled_config import bundled_config
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
    app.command(show_pipeline_config, name="config")
    app(argv)


def show_pipeline_config(pipeline: Path) -> None:
    """Print the configuration bundled in a pipeline file as an arguments file.

    Save it, edit it, and pass it back with ``adagio run --arguments`` to
    override the bundled values; ``--no-bundled-config`` ignores them instead.
    """
    with ExitStack() as exit_stack:
        try:
            pipeline_path = resolve_pipeline_reference(pipeline, exit_stack=exit_stack)
        except PipelineResolutionError as error:
            raise SystemExit(str(error)) from error

        data = json.loads(pipeline_path.read_text(encoding="utf-8"))
        config = bundled_config(data)
        if config is None:
            raise SystemExit("This pipeline bundles no configuration.")
        print(json.dumps({"version": 1, **config}, indent=2))


def show_pipeline(
    pipeline: Path,
    *,
    verbose: Annotated[
        bool,
        Parameter(
            name=("--verbose", "-v"),
            help="Show user-authored descriptions for inputs, outputs, and actions.",
        ),
    ] = False,
) -> None:
    """Print a pipeline summary to the terminal."""
    with ExitStack() as exit_stack:
        try:
            pipeline_path = resolve_pipeline_reference(pipeline, exit_stack=exit_stack)
        except PipelineResolutionError as error:
            raise SystemExit(str(error)) from error

        data = json.loads(pipeline_path.read_text(encoding="utf-8"))
        pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
        parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
        console.print(
            render_pipeline_text(parsed_pipeline, show_user_descriptions=verbose),
            soft_wrap=True,
        )
