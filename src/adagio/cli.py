from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console
import json
from .app.parsers.pipeline import parse_parameters

console = Console()


def _adagio_version_string() -> str:
    try:
        from importlib.metadata import PackageNotFoundError, version as get_version
    except ImportError:  # pragma: no cover
        from importlib_metadata import PackageNotFoundError, version as get_version  # type: ignore

    try:
        return f"Adagio {get_version('adagio')}"
    except PackageNotFoundError:
        return "Adagio version unknown (not installed as a package)"

    # name: Annotated[
    #     str,
    #     Parameter(
    #         name=("--name", "-n"),
    #         help="Say hello to someone else",
    #     ),
    # ],


app = App(
    help="Adagio command line tool for processing pipelines created with the Adagio GUI.",
    version=_adagio_version_string,
)


@app.command(name="run")
def run_cmd(
    pipeline: Annotated[
        Path,
        Parameter(
            name=("--pipeline", "-p"),
            help="Help text",
        ),
    ],
):
    """Run an Adagio pipeline."""
    with open(pipeline, "r") as f:
        data = json.load(f)
    parameters = parse_parameters(data)
    console.print(f"Paramerters: {parameters}")


if __name__ == "__main__":
    app()
