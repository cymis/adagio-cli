import inspect
from pathlib import Path
from typing import Any, Annotated, Callable

from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from .args import ParamType, ShowParamsMode, dynamic_opt, to_identifier


def _spec_py_type(type_name: str) -> type:
    return {"str": str, "int": int, "float": float, "bool": bool}.get(type_name, str)


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
        py_type: type,
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
        opt = dynamic_opt(original, ParamType.PARAM)
        is_required = bool(required and default is None)
        if is_required:
            required_params.append(original)
        default_text = f" [default: {default}]" if default is not None else ""
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=_spec_py_type(spec.type),
            help_text=(
                f"Pipeline parameter: {original}"
                + (" [required]" if is_required else "")
                + default_text
            ),
            default=None,
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
