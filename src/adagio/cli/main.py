import json
import sys
from functools import partial
from pathlib import Path
from typing import Annotated, Any

from cyclopts import App, Group, Parameter
from rich.console import Console

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from ..app.parsers.pipeline import parse_inputs, parse_parameters
from .args import ShowParamsMode, extract_flag_value, promote_positional_pipeline
from .dynamic import build_dynamic_run
from .qapi import build_qapi
from .runner import run_pipeline_from_kwargs


console = Console()


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv

    if argv and argv[0] == "exec-task":
        from .task_exec import run_task_exec

        run_task_exec(argv[1:])
        return

    if argv and argv[0] == "runtime":
        from .runtime import run_runtime

        run_runtime(argv[1:], console=console)
        return

    argv, positional_pipeline = promote_positional_pipeline(argv)
    pipeline_str = extract_flag_value(argv, "--pipeline", "-p")
    show_mode_str = extract_flag_value(argv, "--show-params")
    try:
        show_mode = (
            ShowParamsMode(show_mode_str) if show_mode_str else ShowParamsMode.REQUIRED
        )
    except ValueError as exc:
        raise SystemExit(
            "Invalid --show-params value. Use one of: all, missing, required."
        ) from exc
    if pipeline_str is None:
        pipeline_str = positional_pipeline

    app = App(
        name="adagio",
        help="Adagio command line tool for processing pipelines created with the Adagio GUI.",
    )
    app.command(build_qapi, name="build-qapi")

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
                    help="Path to the pipeline JSON file.",
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
            show_params: Annotated[
                ShowParamsMode,
                Parameter(
                    name=("--show-params",),
                    group=command_group,
                    help="Parameter display mode: all, missing, or required.",
                ),
            ] = ShowParamsMode.REQUIRED,
        ):
            """Run a pipeline (requires --pipeline; dynamic options come from that file)."""
            _ = show_params
            raise SystemExit(
                "Missing --pipeline. Try:\n  adagio run --pipeline pipeline.json --help"
            )

        app(argv)
        return

    pipeline_path = Path(pipeline_str)
    data = json.loads(pipeline_path.read_text(encoding="utf-8"))
    input_specs = parse_inputs(data)
    param_specs = parse_parameters(data)
    arguments_path_str = extract_flag_value(argv, "--arguments")
    arguments_data = (
        _load_arguments_data(Path(arguments_path_str)) if arguments_path_str else None
    )
    visible_inputs, visible_params = _filter_visible_specs(
        input_specs=input_specs,
        param_specs=param_specs,
        show_mode=show_mode,
        arguments_data=arguments_data,
    )

    dynamic_run = build_dynamic_run(
        input_specs=visible_inputs,
        param_specs=visible_params,
        argument_inputs=arguments_data.get("inputs", {}) if arguments_data else None,
        argument_params=arguments_data.get("parameters", {}) if arguments_data else None,
        run_handler=partial(run_pipeline_from_kwargs, console=console),
    )
    app.command(dynamic_run, name="run")
    app(argv)


def _filter_visible_specs(
    *,
    input_specs: list[InputSpec],
    param_specs: list[ParamSpec],
    show_mode: ShowParamsMode,
    arguments_data: dict[str, Any] | None,
) -> tuple[list[InputSpec], list[ParamSpec]]:
    if show_mode is ShowParamsMode.ALL:
        return input_specs, param_specs

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
        return filtered_inputs, filtered_params

    filtered_inputs = [
        spec for spec in input_specs if _is_missing(state_inputs.get(spec.name))
    ]
    filtered_params = [
        spec for spec in param_specs if _is_missing(state_params.get(spec.name))
    ]
    return filtered_inputs, filtered_params


def _load_arguments_data(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Invalid arguments file: expected a JSON object.")
    if "inputs" not in data:
        data["inputs"] = {}
    if "parameters" not in data:
        data["parameters"] = {}
    if not isinstance(data.get("inputs"), dict) or not isinstance(
        data.get("parameters"), dict
    ):
        raise SystemExit("Invalid arguments file: 'inputs' and 'parameters' must be objects.")
    return data


def _is_missing(value: Any) -> bool:
    return value is None or value == "<fill me>"


if __name__ == "__main__":
    main()
