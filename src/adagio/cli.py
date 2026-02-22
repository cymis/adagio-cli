import inspect
import json
import re
import sys
from pathlib import Path
from typing import Any, Annotated

from cyclopts import App, Parameter
from rich.console import Console

from .app.parsers.pipeline import parse_parameters


console = Console()


def _extract_flag_value(argv: list[str], *flags: str) -> str | None:
    """Supports: --flag value, -f value, --flag=value"""
    flag_set = set(flags)
    for i, tok in enumerate(argv):
        if tok in flag_set:
            return argv[i + 1] if i + 1 < len(argv) else None
        for f in flags:
            if tok.startswith(f + "="):
                return tok.split("=", 1)[1]
    return None


def _to_identifier(name: str) -> str:
    """Turn arbitrary names into valid Python identifiers for **kwargs keys."""
    name = name.strip()
    name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    if not name:
        raise ValueError("Empty parameter name in pipeline file.")
    if name[0].isdigit():
        name = "_" + name
    return name


def _kebab(name: str) -> str:
    return name.replace("_", "-")


# ---- Adapt these accessors to whatever parse_parameters returns ----


def _spec_name(spec: Any) -> str:
    return spec["name"] if isinstance(spec, dict) else getattr(spec, "name")


def _spec_required(spec: Any) -> bool:
    if isinstance(spec, dict):
        return bool(spec.get("required", False))
    return bool(getattr(spec, "required", False))


def _spec_default(spec: Any) -> Any:
    if isinstance(spec, dict):
        return spec.get("default", None)
    return getattr(spec, "default", None)


def _spec_help(spec: Any) -> str:
    if isinstance(spec, dict):
        return str(spec.get("help") or spec.get("description") or "")
    return str(getattr(spec, "help", "") or getattr(spec, "description", "") or "")


def _spec_type(spec: Any) -> type:
    """
    Optional: map a spec type -> python type.
    If you don’t have types, just return str.
    """
    t = None
    if isinstance(spec, dict):
        t = spec.get("type") or spec.get("type_")
    else:
        t = getattr(spec, "type", None) or getattr(spec, "type_", None)

    return {"str": str, "int": int, "float": float, "bool": bool}.get(str(t), str)


# @app.command("execute")
# def execute_cmd(
#     pipeline: Annotated[
#         Path,
#         typer.Option(
#             "--input",
#             "-i",
#             help="Adagio created pipeline",
#             exists=False,
#             file_okay=True,
#             dir_okay=False,
#             readable=True,
#         ),
#     ],
#     config: Annotated[
#         Path,
#         typer.Option(
#             "--config",
#             "-c",
#             help="Configuration file for the pipeline",
#             exists=False,
#             file_okay=True,
#             dir_okay=False,
#             readable=True,
#         ),
#     ],
# ):
#     """Execute an Adagio created pipeline"""
#     spec = parse_spec(pipeline)
#     config = parse_config(config)
#
#     process_job(spec, config)
# >>>>>>> dev


# ---- The core: build a dynamic run() with signature + Annotated Parameter ----


def _build_dynamic_run(*, param_specs: list[Any]):
    """
    Build run(pipeline=..., --dynamic-params...) where dynamic params and help
    come from param_specs.
    """
    # Map CLI param -> python identifier for kwargs
    idents: list[tuple[str, str]] = []  # (ident, original_name)

    annotations: dict[str, Any] = {}

    # Fixed param: pipeline
    annotations["pipeline"] = Annotated[
        Path,
        Parameter(
            name=("--pipeline", "-p"),
            help="Path to the pipeline JSON file.",
        ),
    ]

    parameters: list[inspect.Parameter] = [
        inspect.Parameter(
            name="pipeline",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=annotations["pipeline"],
        )
    ]

    for spec in param_specs:
        original = _spec_name(spec)
        ident = _to_identifier(original)
        idents.append((ident, original))

        default = _spec_default(spec)
        required = _spec_required(spec)
        help_text = _spec_help(spec)
        py_type = _spec_type(spec)

        opt = f"--{_kebab(original)}"  # preserve original naming for CLI
        # Required only when required==True and there is no default
        is_required = bool(required and default is None)

        annotations[ident] = Annotated[
            py_type,
            Parameter(
                name=(opt,),
                help=help_text or f"Pipeline parameter: {original}",
                required=is_required,
            ),
        ]

        parameters.append(
            inspect.Parameter(
                name=ident,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=(default if default is not None else inspect._empty),
                annotation=annotations[ident],
            )
        )

    def run(pipeline: Path, **kwargs: Any) -> None:
        # Convert parsed kwargs back to the original pipeline param names
        values: dict[str, Any] = {}
        for ident, original in idents:
            values[original] = kwargs.get(ident)

        console.print(f"[bold]Pipeline:[/bold] {pipeline}")
        console.print("[bold]CLI values:[/bold]")
        for k, v in values.items():
            console.print(f"  {k} = {v!r}")

        # If you want: call your actual runner here using `values`
        # run_pipeline(pipeline, values)

    run.__annotations__ = annotations
    run.__signature__ = inspect.Signature(parameters)
    run.__doc__ = (
        "Run an Adagio pipeline.\n\n"
        "Dynamic parameters are loaded from the pipeline file and exposed as CLI options.\n"
        "Use: adagio run --pipeline <file> --help"
    )
    return run


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv

    pipeline_str = _extract_flag_value(argv, "--pipeline", "-p")

    app = App(
        name="adagio",
        help="Adagio command line tool for processing pipelines created with the Adagio GUI.",
    )

    if not pipeline_str:

        @app.command
        def run(
            pipeline: Annotated[
                Path,
                Parameter(
                    name=("--pipeline", "-p"), help="Path to the pipeline JSON file."
                ),
            ],
        ):
            """Run a pipeline (dynamic parameters come from the pipeline file)."""
            raise SystemExit(
                "Missing --pipeline. Try:\n  adagio run --pipeline pipeline.json --help"
            )

        app(argv)
        return

    pipeline_path = Path(pipeline_str)
    data = json.loads(pipeline_path.read_text(encoding="utf-8"))

    # Your existing loader:
    param_specs = parse_parameters(data)

    dynamic_run = _build_dynamic_run(param_specs=param_specs)
    app.command(dynamic_run, name="run")

    app(argv)


if __name__ == "__main__":
    main()
