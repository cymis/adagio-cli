import inspect
import re
from pathlib import Path
from typing import Any, Annotated, Callable

from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from .args import ParamType, dynamic_opt, to_identifier


def _spec_py_type(type_name: str) -> type:
    normalized = re.sub(r"[^a-z0-9]+", " ", (type_name or "").lower()).strip()
    tokens = set(normalized.split())

    if {"bool", "boolean"} & tokens or "bool" in normalized:
        return bool
    if {"int", "integer"} & tokens or "int" in normalized:
        return int
    if {"float", "double", "number", "numeric", "real"} & tokens:
        return float
    if {"str", "string", "text"} & tokens:
        return str
    return str


def _default_py_type(default: Any) -> type | None:
    if isinstance(default, bool):
        return bool
    if isinstance(default, int):
        return int
    if isinstance(default, float):
        return float
    if isinstance(default, str):
        return str
    return None


def _resolve_param_type(type_name: str, default: Any) -> type:
    declared = _spec_py_type(type_name)
    inferred = _default_py_type(default)
    if inferred is None:
        return declared
    if declared is str and inferred is not str:
        return inferred
    return declared


def build_dynamic_run(
    *,
    input_specs: list[InputSpec],
    param_specs: list[ParamSpec],
    run_handler: Callable[
        [Path, dict[str, Any], list[tuple[str, str]], list[tuple[str, str]]], None
    ],
):
    input_bindings: list[tuple[str, str]] = []
    param_bindings: list[tuple[str, str]] = []
    seen_idents: set[str] = set()
    seen_opts: set[str] = {"--pipeline", "-p"}

    annotations: dict[str, Any] = {
        "pipeline": Annotated[
            Path,
            CliParameter(
                name=("--pipeline", "-p"),
                help="Path to the pipeline JSON file.",
            ),
        ]
    }
    parameters: list[inspect.Parameter] = [
        inspect.Parameter(
            name="pipeline",
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=annotations["pipeline"],
        )
    ]

    def add_dynamic_option(
        *,
        ident: str,
        opt: str,
        required: bool,
        py_type: Any,
        help_text: str,
        default: Any,
    ) -> None:
        if opt in seen_opts:
            raise ValueError(f"Conflicting CLI option generated: {opt!r}.")
        seen_opts.add(opt)

        annotations[ident] = Annotated[
            py_type,
            CliParameter(
                name=(opt,),
                help=help_text,
                required=required,
            ),
        ]
        parameters.append(
            inspect.Parameter(
                name=ident,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=annotations[ident],
            )
        )

    add_dynamic_option(
        ident="arguments_file",
        opt="--arguments",
        required=False,
        py_type=Path | None,
        help_text="Path to an arguments JSON file to pre-populate inputs, parameters, and outputs.",
        default=None,
    )
    add_dynamic_option(
        ident="dummy",
        opt="--dummy",
        required=False,
        py_type=bool,
        help_text="Run a simulated pipeline execution instead of invoking runtime plugins.",
        default=True,
    )
    add_dynamic_option(
        ident="dummy_min_seconds",
        opt="--dummy-min-seconds",
        required=False,
        py_type=float,
        help_text="Minimum seconds spent per task in dummy mode.",
        default=10.0,
    )
    add_dynamic_option(
        ident="dummy_max_seconds",
        opt="--dummy-max-seconds",
        required=False,
        py_type=float,
        help_text="Maximum seconds spent per task in dummy mode.",
        default=15.0,
    )
    add_dynamic_option(
        ident="dummy_fail_rate",
        opt="--dummy-fail-rate",
        required=False,
        py_type=float,
        help_text="Failure probability per task in dummy mode (0.0 to 1.0).",
        default=0.0,
    )
    add_dynamic_option(
        ident="dummy_subtasks",
        opt="--dummy-subtasks",
        required=False,
        py_type=int,
        help_text="Number of subtasks shown for each task in dummy mode.",
        default=3,
    )
    add_dynamic_option(
        ident="dummy_seed",
        opt="--dummy-seed",
        required=False,
        py_type=int | None,
        help_text="Optional random seed for deterministic dummy runs.",
        default=None,
    )

    for spec in input_specs:
        original = spec.name
        ident = to_identifier(original, "input")
        if ident in seen_idents:
            raise ValueError(
                f"Duplicate pipeline input name after normalization: {original!r}."
            )
        seen_idents.add(ident)
        input_bindings.append((ident, original))

        required = spec.required
        type_text = spec.type
        opt = dynamic_opt(original, ParamType.INPUT)
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=required,
            py_type=str,
            help_text=f"Pipeline input: {original}" + (f" ({type_text})" if type_text else ""),
            default=inspect._empty if required else None,
        )

    for spec in param_specs:
        original = spec.name
        ident = to_identifier(original, "param")
        if ident in seen_idents:
            raise ValueError(
                f"Duplicate pipeline parameter name after normalization: {original!r}."
            )
        seen_idents.add(ident)
        param_bindings.append((ident, original))

        default = spec.default
        required = spec.required
        is_required = bool(required and default is None)
        param_default = inspect._empty if is_required else default
        param_type: Any = _resolve_param_type(spec.type, default)
        if not is_required and default is None:
            param_type = param_type | None
        opt = dynamic_opt(original, ParamType.PARAM)
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=is_required,
            py_type=param_type,
            help_text=f"Pipeline parameter: {original}",
            default=param_default,
        )

    def run(pipeline: Path, **kwargs: Any) -> None:
        run_handler(
            pipeline,
            kwargs,
            input_bindings,
            param_bindings,
        )

    run.__annotations__ = annotations
    run.__signature__ = inspect.Signature(parameters)
    run.__doc__ = (
        "Run an Adagio pipeline.\n\n"
        "Dynamic inputs and parameters are loaded from the pipeline file and exposed as CLI options.\n"
        "Use: adagio run --pipeline PATH --help"
    )
    return run
