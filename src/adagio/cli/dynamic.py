import inspect
import re
from pathlib import Path
from typing import Any, Annotated, Callable

from cyclopts import Group
from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from ..executors.cache_support import (
    CACHE_DIR_HELP,
    NO_RECYCLE_HELP,
    RECYCLE_POOL_HELP,
)
from .args import ParamType, ShowParamsMode, dynamic_opt, to_identifier


class _PipelineGroupFormatter:
    """Render pipeline options in one panel with nested subsections."""

    def __call__(self, console: Any, options: Any, panel: Any) -> None:
        from rich.console import Group as RichGroup
        from rich.console import NewLine
        from rich.text import Text

        from cyclopts.help.specs import PanelSpec, TableSpec, get_default_parameter_columns

        input_entries, parameter_entries = _split_pipeline_entries(panel.entries)
        renderables: list[Any] = []

        if panel.description:
            renderables.append(panel.description)

        def add_section(title: str, entries: list[Any]) -> None:
            if not entries:
                return
            if renderables:
                renderables.append(NewLine())
            renderables.append(Text(title, style="bold"))
            columns = get_default_parameter_columns(console, options, entries)
            renderables.append(TableSpec().build(columns, entries))

        add_section("Inputs", input_entries)
        add_section("Parameters", parameter_entries)

        if not renderables:
            return

        console.print(PanelSpec().build(RichGroup(*renderables), title=panel.title))


def _split_pipeline_entries(entries: list[Any]) -> tuple[list[Any], list[Any]]:
    input_entries: list[Any] = []
    parameter_entries: list[Any] = []

    for entry in entries:
        options = entry.all_options if hasattr(entry, "all_options") else ()
        long_name = next((name for name in options if name.startswith("--")), "")
        if long_name.startswith("--input-"):
            input_entries.append(entry)
        else:
            parameter_entries.append(entry)

    return input_entries, parameter_entries


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
    argument_inputs: dict[str, Any] | None = None,
    argument_params: dict[str, Any] | None = None,
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
    seen_opts: set[str] = {
        "--pipeline",
        "-p",
        "--arguments",
        "--show-params",
        "--cache-dir",
        "--use-cache",
        "--recycle-pool",
        "--no-recycle",
    }
    argument_inputs = argument_inputs or {}
    argument_params = argument_params or {}
    command_group = Group("Command Options", sort_key=0)
    pipeline_group = Group(
        "Pipeline",
        sort_key=1,
        help_formatter=_PipelineGroupFormatter(),
    )

    annotations: dict[str, Any] = {
        "pipeline": Annotated[
            Path,
            CliParameter(
                name=("--pipeline", "-p"),
                group=command_group,
                help="Path to the pipeline JSON file.",
            ),
        ]
    }

    annotations["arguments_file"] = Annotated[
        Path | None,
        CliParameter(
            name=("--arguments",),
            group=command_group,
            help="Path to a JSON arguments file. Values are applied before CLI overrides.",
        ),
    ]
    annotations["show_params"] = Annotated[
        ShowParamsMode,
        CliParameter(
            name=("--show-params",),
            group=command_group,
            help="Parameter display mode: all, missing, or required.",
        ),
    ]
    annotations["cache_dir"] = Annotated[
        Path | None,
        CliParameter(
            name=("--cache-dir", "--use-cache"),
            group=command_group,
            help=CACHE_DIR_HELP,
        ),
    ]
    annotations["recycle_pool"] = Annotated[
        str | None,
        CliParameter(
            name=("--recycle-pool",),
            group=command_group,
            help=RECYCLE_POOL_HELP,
        ),
    ]
    annotations["no_recycle"] = Annotated[
        bool,
        CliParameter(
            name=("--no-recycle",),
            group=command_group,
            help=NO_RECYCLE_HELP,
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
        inspect.Parameter(
            name="cache_dir",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=annotations["cache_dir"],
        ),
        inspect.Parameter(
            name="recycle_pool",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=annotations["recycle_pool"],
        ),
        inspect.Parameter(
            name="no_recycle",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=False,
            annotation=annotations["no_recycle"],
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
        group: Group | tuple[Group, ...],
    ) -> None:
        if opt in seen_opts:
            raise ValueError(f"Conflicting CLI option generated: {opt!r}.")
        seen_opts.add(opt)

        annotation_type = py_type | None if default is None else py_type
        annotations[ident] = Annotated[
            annotation_type,
            CliParameter(
                name=(opt,),
                group=group,
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
        argument_value = argument_inputs.get(original)
        display_required = bool(spec.required and _is_missing(argument_value))
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
                + (" [required]" if display_required else "")
            ),
            default=None,
            group=pipeline_group,
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
        argument_value = argument_params.get(original)
        has_argument_default = not _is_missing(argument_value)
        display_default = (
            default if default is not None else (argument_value if has_argument_default else None)
        )
        display_required = is_required and display_default is None
        param_default = None
        param_type: Any = _resolve_param_type(spec.type, default)
        opt = dynamic_opt(original, ParamType.PARAM)
        if is_required:
            required_params.append(original)
        default_text = f" [default: {display_default}]" if display_default is not None else ""
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=param_type,
            help_text=(
                f"Pipeline parameter: {original}"
                + (" [required]" if display_required else "")
                + default_text
            ),
            default=param_default,
            group=pipeline_group,
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


def _is_missing(value: Any) -> bool:
    return value is None or value == "<fill me>"
