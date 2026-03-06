import inspect
from pathlib import Path
from typing import Any, Annotated, Callable

from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from .args import ParamType, dynamic_opt, to_identifier


def _spec_py_type(type_name: str) -> type:
    return {"str": str, "int": int, "float": float, "bool": bool}.get(type_name, str)


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
        py_type: type,
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
        opt = dynamic_opt(original, ParamType.PARAM)
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=bool(required and default is None),
            py_type=_spec_py_type(spec.type),
            help_text=f"Pipeline parameter: {original}",
            default=default if default is not None else inspect._empty,
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
