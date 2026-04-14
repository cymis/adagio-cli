import json
import sys
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from typing import Annotated, Any

from cyclopts import App, Group, Parameter
from cyclopts.panel import CycloptsPanel
from rich.console import Console

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Output as OutputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from ..app.parsers.pipeline import parse_inputs, parse_outputs, parse_parameters
from ..executors.cache_support import CACHE_DIR_HELP, REUSE_HELP
from .args import ShowParamsMode, extract_flag_value, promote_positional_pipeline
from .config import load_run_config
from .dynamic import build_dynamic_run
from .pipeline import run_pipeline_cli
from .pipeline_sources import PipelineResolutionError, resolve_pipeline_reference
from .qapi import run_qapi
from .runner import run_pipeline_from_kwargs


console = Console()


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv

    if argv and argv[0] == "exec-task":
        from .task_exec import run_task_exec

        run_task_exec(argv[1:])
        return

    if argv and argv[0] == "cache":
        from .cache import run_cache

        run_cache(argv[1:], console=console)
        return

    if argv and argv[0] == "runtime":
        from .runtime import run_runtime

        run_runtime(argv[1:], console=console)
        return

    if argv and argv[0] == "qapi":
        run_qapi(argv[1:])
        return

    if argv and argv[0] == "pipeline":
        run_pipeline_cli(argv[1:])
        return

    argv, positional_pipeline = promote_positional_pipeline(argv)
    pipeline_str = extract_flag_value(argv, "--pipeline", "-p")
    show_mode_str = extract_flag_value(argv, "--show-params")
    try:
        show_mode = (
            ShowParamsMode(show_mode_str) if show_mode_str else ShowParamsMode.REQUIRED
        )
    except ValueError:
        console.print(
            CycloptsPanel(
                "Invalid --show-params value. Use one of: all, missing, required."
            )
        )
        sys.exit(1)
    if pipeline_str is None:
        pipeline_str = positional_pipeline

    app = App(
        name="adagio",
        help="Adagio command line tool for processing pipelines created with the Adagio GUI.",
        help_format="rich",
    )

    @app.command
    def cache() -> None:
        """Manage the shared QIIME cache directory."""
        console.print(CycloptsPanel("Try: adagio cache --help"))
        sys.exit(1)

    @app.command
    def runtime() -> None:
        """Execute a pipeline from spec/config/arguments files."""
        console.print(CycloptsPanel("Try: adagio runtime --help"))
        sys.exit(1)

    @app.command
    def qapi() -> None:
        """Generate and submit QAPI payloads."""
        console.print(CycloptsPanel("Try: adagio qapi --help"))
        sys.exit(1)

    @app.command
    def pipeline() -> None:
        """Inspect pipeline definitions."""
        console.print(CycloptsPanel("Try: adagio pipeline --help"))
        sys.exit(1)

    if not pipeline_str:
        command_group = Group("Command Options", sort_key=0)

        @app.command
        def run(
            *,
            pipeline: Annotated[
                Path,
                Parameter(
                    name=("--pipeline", "-p"),
                    group=command_group,
                    help="Path to the pipeline file or a pipeline source reference.",
                ),
            ],
            arguments: Annotated[
                Path | None,
                Parameter(
                    name=("--arguments",),
                    group=command_group,
                    help="Path to a JSON arguments file.",
                ),
            ] = None,
            config: Annotated[
                Path | None,
                Parameter(
                    name=("--config",),
                    group=command_group,
                    help="Path to a TOML runtime config file.",
                ),
            ] = None,
            show_params: Annotated[
                ShowParamsMode,
                Parameter(
                    name=("--show-params",),
                    group=command_group,
                    help="Parameter display mode: all, missing, or required.",
                ),
            ] = ShowParamsMode.REQUIRED,
            cache_dir: Annotated[
                Path,
                Parameter(
                    name=("--cache-dir",),
                    group=command_group,
                    help=CACHE_DIR_HELP,
                ),
            ],
            reuse: Annotated[
                bool,
                Parameter(
                    name=("--reuse",),
                    negative=("--no-reuse",),
                    group=command_group,
                    help=REUSE_HELP,
                ),
            ] = True,
        ):
            """Run a pipeline (requires --pipeline; dynamic options come from that file)."""
            _ = (config, show_params, cache_dir, reuse)
            console.print(
                CycloptsPanel(
                    "Missing --pipeline. Try:\n  adagio run --pipeline pipeline.adg --help"
                )
            )
            sys.exit(1)

        app(argv)
        return

    with ExitStack() as exit_stack:
        pipeline_path = _resolve_pipeline_path(
            pipeline_str,
            console=console,
            exit_stack=exit_stack,
        )
        data = json.loads(pipeline_path.read_text(encoding="utf-8"))
        input_specs = parse_inputs(data)
        param_specs = parse_parameters(data)
        output_specs = parse_outputs(data)
        arguments_path_str = extract_flag_value(argv, "--arguments")
        config_path_str = extract_flag_value(argv, "--config")
        arguments_data = (
            _load_arguments_data(Path(arguments_path_str), console)
            if arguments_path_str
            else None
        )
        if config_path_str:
            load_run_config(Path(config_path_str))
        visible_inputs, visible_params, visible_outputs = _filter_visible_specs(
            input_specs=input_specs,
            param_specs=param_specs,
            output_specs=output_specs,
            show_mode=show_mode,
            arguments_data=arguments_data,
        )

        dynamic_run = build_dynamic_run(
            input_specs=visible_inputs,
            param_specs=visible_params,
            output_specs=visible_outputs,
            argument_inputs=arguments_data.get("inputs", {})
            if arguments_data
            else None,
            argument_params=arguments_data.get("parameters", {})
            if arguments_data
            else None,
            run_handler=partial(run_pipeline_from_kwargs, console=console),
        )
        app.command(dynamic_run, name="run")
        app(argv)


def _filter_visible_specs(
    *,
    input_specs: list[InputSpec],
    param_specs: list[ParamSpec],
    output_specs: list[OutputSpec],
    show_mode: ShowParamsMode,
    arguments_data: dict[str, Any] | None,
) -> tuple[list[InputSpec], list[ParamSpec], list[OutputSpec]]:
    if show_mode is ShowParamsMode.ALL:
        return input_specs, param_specs, output_specs

    state_inputs = {spec.name: None for spec in input_specs}
    state_params = {spec.name: spec.default for spec in param_specs}

    if arguments_data is not None:
        state_inputs.update(arguments_data.get("inputs", {}))
        state_params.update(arguments_data.get("parameters", {}))

    if show_mode is ShowParamsMode.REQUIRED:
        filtered_inputs = [
            spec
            for spec in input_specs
            if spec.required and _is_missing(state_inputs.get(spec.name))
        ]
        filtered_params = [
            spec
            for spec in param_specs
            if bool(
                spec.required
                and spec.default is None
                and _is_missing(state_params.get(spec.name))
            )
        ]
        return filtered_inputs, filtered_params, []

    filtered_inputs = [
        spec for spec in input_specs if _is_missing(state_inputs.get(spec.name))
    ]
    filtered_params = [
        spec for spec in param_specs if _is_missing(state_params.get(spec.name))
    ]
    return filtered_inputs, filtered_params, []


def _load_arguments_data(path: Path, _console: Console | None = None) -> dict[str, Any]:
    _con = _console or Console(stderr=True)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        _con.print(CycloptsPanel("Invalid arguments file: expected a JSON object."))
        sys.exit(1)
    if "inputs" not in data:
        data["inputs"] = {}
    if "parameters" not in data:
        data["parameters"] = {}
    if not isinstance(data.get("inputs"), dict) or not isinstance(
        data.get("parameters"), dict
    ):
        _con.print(
            CycloptsPanel(
                "Invalid arguments file: 'inputs' and 'parameters' must be objects."
            )
        )
        sys.exit(1)
    return data


def _is_missing(value: Any) -> bool:
    return value is None or value == "<fill me>"


def _resolve_pipeline_path(
    reference: str,
    *,
    console: Console,
    exit_stack: ExitStack,
) -> Path:
    try:
        return resolve_pipeline_reference(reference, exit_stack=exit_stack)
    except PipelineResolutionError as error:
        console.print(CycloptsPanel(str(error)))
        sys.exit(1)


if __name__ == "__main__":
    main()
