import json
import sys
from functools import partial
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from ..app.parsers.pipeline import parse_inputs, parse_parameters
from .args import extract_flag_value, promote_positional_pipeline
from .dynamic import build_dynamic_run
from .runner import run_pipeline_from_kwargs


console = Console()


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv

    argv, positional_pipeline = promote_positional_pipeline(argv)
    pipeline_str = extract_flag_value(argv, "--pipeline", "-p")
    if pipeline_str is None:
        pipeline_str = positional_pipeline

    app = App(
        name="adagio",
        help="Adagio command line tool for processing pipelines created with the Adagio GUI.",
    )

    if not pipeline_str:

        @app.command
        def run(
            *,
            pipeline: Annotated[
                Path,
                Parameter(
                    name=("--pipeline", "-p"), help="Path to the pipeline JSON file."
                ),
            ],
        ):
            """Run a pipeline (requires --pipeline; dynamic options come from that file)."""
            raise SystemExit(
                "Missing --pipeline. Try:\n  adagio run --pipeline pipeline.json --help"
            )

        app(argv)
        return

    pipeline_path = Path(pipeline_str)
    data = json.loads(pipeline_path.read_text(encoding="utf-8"))
    input_specs = parse_inputs(data)
    param_specs = parse_parameters(data)

    dynamic_run = build_dynamic_run(
        input_specs=input_specs,
        param_specs=param_specs,
        run_handler=partial(run_pipeline_from_kwargs, console=console),
    )
    app.command(dynamic_run, name="run")
    app(argv)


if __name__ == "__main__":
    main()
