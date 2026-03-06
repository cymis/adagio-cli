import inspect
import re
from pathlib import Path
from typing import Any, Annotated, Callable

from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from .args import ParamType, ShowParamsMode, dynamic_opt, to_identifier


def _spec_py_type(type_name: str) -> type:
    """Map pipeline type text to a Python type."""
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
    """Infer a Python type from a default value."""
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
    """Resolve the CLI parameter type from type text and default."""
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
        [
            Path,
            Path | None,
            dict[str, Any],
            list[tuple[str, str]],
            list[tuple[str, str]],
            list[str],
            list[str],
        ],
        None,
    ],
):
    """Build a dynamic run command from pipeline input and parameter specs."""
    input_bindings: list[tuple[str, str]] = []
    param_bindings: list[tuple[str, str]] = []
    required_inputs: list[str] = []
    required_params: list[str] = []
    seen_idents: set[str] = set()
    seen_opts: set[str] = {"--pipeline", "-p", "--arguments", "--show-params"}

    annotations: dict[str, Any] = {
        "pipeline": Annotated[
            Path,
            CliParameter(
                name=("--pipeline", "-p"),
                help="Path to the pipeline JSON file.",
            ),
        ]
    }

    annotations["arguments_file"] = Annotated[
        Path | None,
        CliParameter(
            name=("--arguments",),
            help="Path to a JSON arguments file. Values are applied before CLI overrides.",
        ),
    ]
    annotations["show_params"] = Annotated[
        ShowParamsMode,
        CliParameter(
            name=("--show-params",),
            help="Parameter display mode: all, missing, or required.",
        ),
    ]

    parameters: list[inspect.Parameter] = [
        inspect.Parameter(
            name="pipeline",
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=annotations["pipeline"],
        ),
        inspect.Parameter(
            name="arguments_file",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=annotations["arguments_file"],
        ),
        inspect.Parameter(
            name="show_params",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=ShowParamsMode.REQUIRED,
            annotation=annotations["show_params"],
        ),
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

        annotation_type = py_type | None if default is None else py_type
        annotations[ident] = Annotated[
            annotation_type,
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

    for spec in input_specs:
        original = spec.name
        ident = to_identifier(original, "input")
        if ident in seen_idents:
            raise ValueError(
                f"Duplicate pipeline input name after normalization: {original!r}."
            )
        seen_idents.add(ident)
        input_bindings.append((ident, original))
        if spec.required:
            required_inputs.append(original)

        type_text = spec.type
        opt = dynamic_opt(original, ParamType.INPUT)
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=str,
            help_text=(
                f"Pipeline input: {original}"
                + (f" ({type_text})" if type_text else "")
                + (" [required]" if spec.required else "")
            ),
            default=None,
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
        param_default = None
        param_type: Any = _resolve_param_type(spec.type, default)
        opt = dynamic_opt(original, ParamType.PARAM)
        if is_required:
            required_params.append(original)
        default_text = f" [default: {default}]" if default is not None else ""
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=param_type,
            help_text=(
                f"Pipeline parameter: {original}"
                + (" [required]" if is_required else "")
                + default_text
            ),
            default=param_default,
        )

    def run(
        pipeline: Path,
        arguments_file: Path | None = None,
        show_params: ShowParamsMode = ShowParamsMode.REQUIRED,
        **kwargs: Any,
    ) -> None:
        _ = show_params
        run_handler(
            pipeline,
            arguments_file,
            kwargs,
            input_bindings,
            param_bindings,
            required_inputs,
            required_params,
        )

    run.__annotations__ = annotations
    run.__signature__ = inspect.Signature(parameters)
    run.__doc__ = (
        "Run an Adagio pipeline.\n\n"
        "Dynamic inputs and parameters are loaded from the pipeline file and exposed as CLI options.\n"
        "Use: adagio run --pipeline PATH --help"
    )
    return run
