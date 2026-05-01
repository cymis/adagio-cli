import inspect
import math
import re
import types
from pathlib import Path
from typing import Any, Annotated, Callable, Union, get_args, get_origin

from cyclopts import Group
from cyclopts import Parameter as CliParameter

from ..app.parsers.pipeline import Input as InputSpec
from ..app.parsers.pipeline import Output as OutputSpec
from ..app.parsers.pipeline import Parameter as ParamSpec
from ..executors.cache_support import (
    CACHE_DIR_HELP,
    REUSE_HELP,
)
from ..type_format import (
    compact_type_text,
    path_type_label,
    render_type_text,
    type_label_display_width,
)
from .args import ParamType, ShowParamsMode, dynamic_opt, to_identifier


class _PipelineGroupFormatter:
    """Render pipeline options in one aligned table."""

    def __init__(self, entry_metadata: dict[str, dict[str, Any]]):
        self.entry_metadata = entry_metadata

    def __call__(self, console: Any, options: Any, panel: Any) -> None:
        from rich.console import Group as RichGroup
        from rich.console import NewLine

        from cyclopts.help.specs import PanelSpec, TableSpec

        renderables: list[Any] = []

        if panel.description:
            renderables.append(panel.description)
        if not panel.entries:
            return

        if renderables:
            renderables.append(NewLine())
        columns = _get_pipeline_parameter_columns(
            console, panel.entries, self.entry_metadata
        )
        renderables.append(TableSpec().build(columns, panel.entries))
        console.print(PanelSpec().build(RichGroup(*renderables), title=panel.title))


def _entry_key(entry: Any) -> str:
    options = entry.all_options if hasattr(entry, "all_options") else ()
    return next((name for name in options if name.startswith("--")), "")


def _unwrap_optional_type(type_hint: Any) -> Any:
    origin = get_origin(type_hint)
    if origin not in (types.UnionType, Union):
        return type_hint

    args = [arg for arg in get_args(type_hint) if arg is not type(None)]
    return args[0] if len(args) == 1 else type_hint


def _pipeline_type_label(type_hint: Any) -> str:
    type_hint = _unwrap_optional_type(type_hint)
    if type_hint is bool:
        return "BOOLEAN"
    if type_hint is int:
        return "INTEGER"
    if type_hint is float:
        return "NUMBER"
    if type_hint is Path:
        return "PATH"
    return "TEXT"


def _display_type_label(*, spec_type: str | None, type_hint: Any, is_input: bool) -> str:
    if is_input:
        return path_type_label(spec_type)

    if spec_type:
        compact = compact_type_text(spec_type)
        if compact.startswith("["):
            return compact

    return _pipeline_type_label(type_hint)


def _output_path_help(description: str | None) -> str:
    cleaned = (description or "").strip()
    if cleaned:
        return f"{cleaned} Overrides --output-dir for this output."
    return "Overrides --output-dir for this output."


def _render_pipeline_type(
    entry: Any, entry_metadata: dict[str, dict[str, Any]], width: int
) -> Any:
    label = entry_metadata.get(_entry_key(entry), {}).get("type_label", "TEXT")
    return render_type_text(label, width)


def _render_pipeline_description(
    entry: Any, entry_metadata: dict[str, dict[str, Any]]
) -> Any:
    from rich.text import Text

    from cyclopts.help.inline_text import InlineText

    metadata = entry_metadata.get(_entry_key(entry), {})
    description = entry.description
    if description is None:
        description = InlineText(Text())
    elif not isinstance(description, InlineText):
        if hasattr(description, "__rich_console__"):
            description = InlineText(description)
        else:
            description = InlineText(Text(str(description)))

    default = metadata.get("default")
    if default is not None:
        description.append(Text(f"[default: {default}]", "dim"))

    if metadata.get("required"):
        description.append(Text("[required]", "dim red"))

    return description


def _get_pipeline_parameter_columns(
    console: Any,
    entries: list[Any],
    entry_metadata: dict[str, dict[str, Any]],
) -> tuple[Any, ...]:
    from cyclopts.help.specs import (
        ColumnSpec,
        NameRenderer,
    )

    max_width = math.ceil(console.width * 0.35)
    type_width = max(
        8,
        min(
            max(
                type_label_display_width(
                    entry_metadata.get(_entry_key(entry), {}).get("type_label", "TEXT")
                )
                for entry in entries
            ),
            max(28, min(70, math.ceil(console.width * 0.35))),
        ),
    )
    name_column = ColumnSpec(
        renderer=NameRenderer(max_width=max_width),
        header="Option",
        justify="left",
        style="cyan",
        max_width=max_width,
    )
    type_column = ColumnSpec(
        renderer=lambda entry: _render_pipeline_type(entry, entry_metadata, type_width),
        header="Type",
        justify="left",
        no_wrap=True,
        width=type_width,
        min_width=type_width,
        max_width=type_width,
    )
    description_column = ColumnSpec(
        renderer=lambda entry: _render_pipeline_description(entry, entry_metadata),
        header="Description",
        justify="left",
        overflow="fold",
    )

    return (name_column, type_column, description_column)


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


def _format_help_text(
    *,
    description: str | None = None,
) -> str:
    """Return plain description text for pipeline help rows."""
    return (description or "").strip()


def _is_required_param(spec: ParamSpec) -> bool:
    return bool(spec.required and spec.default is None)


def _is_collection_type(type_name: str) -> bool:
    return type_name.startswith("List[") or type_name.startswith("Collection[")


def build_dynamic_run(
    *,
    input_specs: list[InputSpec],
    param_specs: list[ParamSpec],
    output_specs: list[OutputSpec],
    visible_input_names: set[str] | None = None,
    visible_param_names: set[str] | None = None,
    visible_output_names: set[str] | None = None,
    argument_inputs: dict[str, Any] | None = None,
    argument_params: dict[str, Any] | None = None,
    run_handler: Callable[
        [
            Path,
            Path | None,
            Path | None,
            dict[str, Any],
            list[tuple[str, str]],
            list[tuple[str, str]],
            list[tuple[str, str]],
            str,
            list[str],
            list[str],
        ],
        None,
    ],
):
    """Build a dynamic run command from pipeline input, parameter, and output specs."""
    visible_input_names = (
        set(visible_input_names) if visible_input_names is not None else None
    )
    visible_param_names = set(visible_param_names) if visible_param_names is not None else None
    visible_output_names = (
        set(visible_output_names) if visible_output_names is not None else None
    )
    input_bindings: list[tuple[str, str]] = []
    param_bindings: list[tuple[str, str]] = []
    output_bindings: list[tuple[str, str]] = []
    required_inputs: list[str] = []
    required_params: list[str] = []
    seen_idents: set[str] = set()
    entry_metadata: dict[str, dict[str, Any]] = {}
    seen_opts: set[str] = {
        "--pipeline",
        "-p",
        "--arguments",
        "--config",
        "--show-params",
        "--cache-dir",
        "--reuse",
        "--no-reuse",
        "--output-dir",
    }
    argument_inputs = argument_inputs or {}
    argument_params = argument_params or {}
    command_group = Group("Command Options", sort_key=0)
    pipeline_group = Group(
        "Pipeline",
        sort_key=1,
        help_formatter=_PipelineGroupFormatter(entry_metadata),
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
    annotations["config_file"] = Annotated[
        Path | None,
        CliParameter(
            name=("--config",),
            group=command_group,
            help="Path to a TOML runtime config file.",
        ),
    ]
    annotations["cache_dir"] = Annotated[
        Path,
        CliParameter(
            name=("--cache-dir",),
            group=command_group,
            help=CACHE_DIR_HELP,
        ),
    ]
    annotations["reuse"] = Annotated[
        bool,
        CliParameter(
            name=("--reuse",),
            negative=("--no-reuse",),
            group=command_group,
            help=REUSE_HELP,
        ),
    ]
    annotations["output_dir"] = Annotated[
        Path | None,
        CliParameter(
            name=("--output-dir",),
            group=command_group,
            help="Directory for all pipeline outputs.",
        ),
    ]

    parameters: list[inspect.Parameter] = [
        inspect.Parameter(
            name="pipeline",
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=annotations["pipeline"],
        ),
        inspect.Parameter(
            name="cache_dir",
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=annotations["cache_dir"],
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
            name="config_file",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=annotations["config_file"],
        ),
        inspect.Parameter(
            name="reuse",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=True,
            annotation=annotations["reuse"],
        ),
        inspect.Parameter(
            name="output_dir",
            kind=inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=annotations["output_dir"],
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
        show: bool = True,
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
                show=show,
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

    required_input_specs = [spec for spec in input_specs if spec.required]
    optional_input_specs = [spec for spec in input_specs if not spec.required]
    required_param_specs = [spec for spec in param_specs if _is_required_param(spec)]
    optional_param_specs = [spec for spec in param_specs if not _is_required_param(spec)]

    def add_input_spec(spec: InputSpec) -> None:
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
        show = visible_input_names is None or original in visible_input_names
        entry_metadata[opt] = {
            "type_label": _display_type_label(
                spec_type=type_text, type_hint=str, is_input=True
            ),
            "default": None,
            "required": display_required,
        }
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=list[str] if _is_collection_type(spec.type) else str,
            help_text=_format_help_text(
                description=spec.description,
            ),
            default=None,
            group=pipeline_group,
            show=show,
        )

    def add_param_spec(spec: ParamSpec) -> None:
        original = spec.name
        ident = to_identifier(original, "param")
        if ident in seen_idents:
            raise ValueError(
                f"Duplicate pipeline parameter name after normalization: {original!r}."
            )
        seen_idents.add(ident)
        param_bindings.append((ident, original))

        default = spec.default
        is_required = _is_required_param(spec)
        argument_value = argument_params.get(original)
        has_argument_default = not _is_missing(argument_value)
        display_default = (
            default if default is not None else (argument_value if has_argument_default else None)
        )
        display_required = is_required and display_default is None
        param_default = None
        param_type: Any = _resolve_param_type(spec.type, default)
        opt = dynamic_opt(original, ParamType.PARAM)
        show = visible_param_names is None or original in visible_param_names
        if is_required:
            required_params.append(original)
        entry_metadata[opt] = {
            "type_label": _display_type_label(
                spec_type=spec.type, type_hint=param_type, is_input=False
            ),
            "default": display_default,
            "required": display_required,
        }
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=param_type,
            help_text=_format_help_text(
                description=spec.description,
            ),
            default=param_default,
            group=pipeline_group,
            show=show,
        )

    for spec in required_input_specs:
        add_input_spec(spec)
    for spec in required_param_specs:
        add_param_spec(spec)
    for spec in optional_input_specs:
        add_input_spec(spec)
    for spec in optional_param_specs:
        add_param_spec(spec)

    for spec in output_specs:
        original = spec.name
        ident = to_identifier(original, "output")
        if ident in seen_idents:
            raise ValueError(
                f"Duplicate pipeline output name after normalization: {original!r}."
            )
        seen_idents.add(ident)
        output_bindings.append((ident, original))
        opt = dynamic_opt(original, ParamType.OUTPUT)
        show = visible_output_names is None or original in visible_output_names
        entry_metadata[opt] = {
            "type_label": path_type_label(spec.type),
            "default": None,
            "required": False,
        }
        add_dynamic_option(
            ident=ident,
            opt=opt,
            required=False,
            py_type=str,
            help_text=_format_help_text(
                description=_output_path_help(spec.description),
            ),
            default=None,
            group=pipeline_group,
            show=show,
        )

    def run(
        pipeline: Path,
        arguments_file: Path | None = None,
        show_params: ShowParamsMode = ShowParamsMode.REQUIRED,
        config_file: Path | None = None,
        output_dir: Path | None = None,
        **kwargs: Any,
    ) -> None:
        _ = show_params
        kwargs["output_dir"] = output_dir
        run_handler(
            pipeline,
            arguments_file,
            config_file,
            kwargs,
            input_bindings,
            param_bindings,
            output_bindings,
            "output_dir",
            required_inputs,
            required_params,
        )

    run.__annotations__ = annotations
    run.__signature__ = inspect.Signature(parameters)
    run.__doc__ = (
        "Run an Adagio pipeline.\n\n"
        "Dynamic inputs, parameters, and outputs are loaded from the pipeline file and exposed as CLI options.\n"
        "Use: adagio run --pipeline PATH --help"
    )
    return run


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == "<fill me>" or value == [] or value == {}
