import json
from dataclasses import dataclass

from rich import box
from rich.console import Group, NewLine
from rich.panel import Panel
from rich.text import Text

from .cli.dynamic import _compact_type_text, _wrap_type_label
from .executors.common import plan_execution_order
from .model.pipeline import AdagioPipeline
from .model.task import (
    LiteralVal,
    MetadataVal,
    PluginActionTask,
    PromotedVal,
    RootInputTask,
)


@dataclass(frozen=True)
class _DisplayRef:
    label: str
    type_label: str | None = None
    description: str | None = None


_ENTRY_INDENT = "       "
_PIPELINE_SHOW_TYPE_WIDTH = 72


def render_pipeline_text(pipeline: AdagioPipeline) -> Text | Group:
    available_ids = {
        input_def.id: _DisplayRef(
            label=_pipeline_input_label(input_def.name),
            type_label=_format_spec_type(input_def.type),
            description=_clean_description(input_def.description),
        )
        for input_def in pipeline.signature.inputs
    }
    parameter_refs = {
        parameter.id: _DisplayRef(
            label=_pipeline_parameter_label(parameter.name),
            type_label=_format_spec_type(parameter.type),
            description=_clean_description(parameter.description),
        )
        for parameter in pipeline.signature.parameters
    }
    pipeline_output_refs = {
        output.id: _DisplayRef(
            label=f'pipeline output "{output.name}"',
            type_label=_format_spec_type(output.type),
            description=_clean_description(output.description),
        )
        for output in pipeline.signature.outputs
    }
    execution_plan = plan_execution_order(
        tasks=list(pipeline.iter_tasks()),
        scope=available_ids,
    )

    panels = []
    for task in execution_plan:
        if isinstance(task, RootInputTask):
            _record_root_input_outputs(task=task, available_ids=available_ids)
            continue

        if not isinstance(task, PluginActionTask):
            continue

        body = Text(no_wrap=False, overflow="fold")
        _append_section_header(body, "Inputs")
        _append_input_lines(body, task=task, available_ids=available_ids)
        _append_section_header(body, "Parameters")
        _append_parameter_lines(
            body,
            task=task,
            available_ids=available_ids,
            parameter_refs=parameter_refs,
        )
        _append_section_header(body, "Outputs")
        _append_output_lines(
            body,
            task=task,
            pipeline_output_refs=pipeline_output_refs,
        )
        panels.append(
            Panel(
                body,
                title=f"{task.plugin}.{task.action}",
                title_align="left",
                border_style="cyan",
                box=box.ROUNDED,
                expand=True,
            )
        )

        for output_name, output in task.outputs.items():
            pipeline_output_ref = pipeline_output_refs.get(output.id)
            available_ids[output.id] = _DisplayRef(
                label=f"{task.plugin}.{task.action}.{output_name}",
                type_label=(
                    pipeline_output_ref.type_label
                    if pipeline_output_ref is not None
                    else None
                ),
                description=(
                    pipeline_output_ref.description
                    if pipeline_output_ref is not None
                    else None
                ),
            )

    if not panels:
        return Text("No plugin actions found.", style="dim")

    renderables = []
    for index, panel in enumerate(panels):
        if index:
            renderables.append(NewLine())
        renderables.append(panel)
    return Group(*renderables)


def _append_section_header(rendered: Text, title: str) -> None:
    rendered.append(f"   {title}:\n", style="bold cyan")


def _append_input_lines(
    rendered: Text,
    *,
    task: PluginActionTask,
    available_ids: dict[str, _DisplayRef],
) -> None:
    if not task.inputs:
        _append_none_line(rendered)
        return

    for input_name, source in task.inputs.items():
        if source.kind == "archive-collection":
            labels = [
                available_ids.get(item.id, _unknown_reference(item.id)).label
                for item in source.items
            ]
            _append_entry_line(
                rendered,
                name=input_name,
                type_label="list",
                value_text=f"[{', '.join(labels)}]",
                description=None,
            )
            continue
        reference = available_ids.get(source.id, _unknown_reference(source.id))
        _append_entry_line(
            rendered,
            name=input_name,
            type_label=reference.type_label,
            value_text=reference.label,
            description=reference.description,
        )


def _append_parameter_lines(
    rendered: Text,
    *,
    task: PluginActionTask,
    available_ids: dict[str, _DisplayRef],
    parameter_refs: dict[str, _DisplayRef],
) -> None:
    if not task.parameters:
        _append_none_line(rendered)
        return

    for parameter_name, value in task.parameters.items():
        rendered_value, display = _render_parameter_value(
            task=task,
            parameter_name=parameter_name,
            value=value,
            available_ids=available_ids,
            parameter_refs=parameter_refs,
        )
        _append_entry_line(
            rendered,
            name=parameter_name,
            type_label=display.type_label if display is not None else None,
            value_text=rendered_value,
            description=display.description if display is not None else None,
        )


def _append_output_lines(
    rendered: Text,
    *,
    task: PluginActionTask,
    pipeline_output_refs: dict[str, _DisplayRef],
) -> None:
    if not task.outputs:
        _append_none_line(rendered)
        return

    for output_name, output in task.outputs.items():
        pipeline_output_ref = pipeline_output_refs.get(output.id)
        value_text = _output_annotation(output_name=output_name, output_id=output.id)
        _append_entry_line(
            rendered,
            name=output_name,
            type_label=(
                pipeline_output_ref.type_label
                if pipeline_output_ref is not None
                else None
            ),
            value_text=value_text,
            description=(
                pipeline_output_ref.description
                if pipeline_output_ref is not None
                else None
            ),
        )


def _append_none_line(rendered: Text) -> None:
    rendered.append("     (none)\n", style="dim")


def _append_entry_line(
    rendered: Text,
    *,
    name: str,
    type_label: str | None,
    value_text: str | None,
    description: str | None,
) -> None:
    rendered.append("     - ")
    rendered.append(name, style="cyan")
    if value_text is not None:
        rendered.append(":", style="cyan")
    if type_label:
        rendered.append(" ")
        wrapped_type = _wrap_type_label(type_label, _PIPELINE_SHOW_TYPE_WIDTH)
        type_lines = wrapped_type.splitlines()
        rendered.append(type_lines[0], style="bold yellow")
        if len(type_lines) > 1:
            for line in type_lines[1:]:
                rendered.append("\n")
                rendered.append(_ENTRY_INDENT)
                rendered.append(line, style="bold yellow")
            if value_text:
                rendered.append("\n")
                rendered.append(_ENTRY_INDENT)
                rendered.append(value_text)
                value_text = None
    if value_text:
        rendered.append(" ")
        rendered.append(value_text)
    rendered.append("\n")
    if description:
        rendered.append(_ENTRY_INDENT)
        rendered.append(description, style="dim")
        rendered.append("\n")


def _render_parameter_value(
    *,
    task: PluginActionTask,
    parameter_name: str,
    value: object,
    available_ids: dict[str, _DisplayRef],
    parameter_refs: dict[str, _DisplayRef],
) -> tuple[str, _DisplayRef | None]:
    if isinstance(value, PromotedVal):
        display = parameter_refs.get(value.id)
        if display is not None:
            return display.label, display
        return _pipeline_parameter_label(value.id), None

    if isinstance(value, LiteralVal):
        return _render_literal(value.value), _literal_display(value.value)

    if isinstance(value, MetadataVal):
        source = task.inputs.get(parameter_name)
        source_ref = (
            available_ids.get(source.id, _unknown_reference(source.id))
            if source is not None
            else _DisplayRef(label=f'input "{parameter_name}"')
        )
        column_label, display = _render_metadata_column(
            column=value.column,
            parameter_refs=parameter_refs,
        )
        rendered_value = (
            f"metadata column from {source_ref.label} using {column_label}"
        )
        if display is not None:
            return rendered_value, display
        return rendered_value, _DisplayRef(
            label=rendered_value,
            description=source_ref.description,
        )

    return str(value), None


def _render_metadata_column(
    *,
    column: object,
    parameter_refs: dict[str, _DisplayRef],
) -> tuple[str, _DisplayRef | None]:
    if isinstance(column, PromotedVal):
        display = parameter_refs.get(column.id)
        if display is not None:
            return display.label, display
        return _pipeline_parameter_label(column.id), None

    value = getattr(column, "value", None)
    return _render_literal(value), _literal_display(value)


def _render_literal(value: object) -> str:
    return json.dumps(value, sort_keys=True)


def _literal_display(value: object) -> _DisplayRef:
    type_label = _format_literal_type(value)
    return _DisplayRef(label=_render_literal(value), type_label=type_label)


def _format_literal_type(value: object) -> str | None:
    if isinstance(value, bool):
        return "(Boolean)"
    if isinstance(value, int):
        return "(Int)"
    if isinstance(value, float):
        return "(Float)"
    if isinstance(value, str):
        return "(Str)"
    return None


def _record_root_input_outputs(
    *,
    task: RootInputTask,
    available_ids: dict[str, _DisplayRef],
) -> None:
    for name, output in task.outputs.items():
        source = task.inputs.get(name)
        if source is None:
            available_ids[output.id] = _DisplayRef(label=_pipeline_input_label(name))
            continue
        available_ids[output.id] = available_ids.get(
            source.id,
            _unknown_reference(source.id),
        )


def _output_annotation(*, output_name: str, output_id: str) -> str | None:
    _ = output_name
    _ = output_id
    return None


def _format_spec_type(type_text: str | None) -> str | None:
    cleaned = (type_text or "").strip()
    if not cleaned:
        return None
    return _compact_type_text(cleaned)


def _clean_description(description: str | None) -> str | None:
    cleaned = (description or "").strip()
    return cleaned or None


def _pipeline_input_label(name: str) -> str:
    return f'pipeline input "{name}"'


def _pipeline_parameter_label(name: str) -> str:
    return f'pipeline parameter "{name}"'


def _unknown_reference(identifier: str) -> _DisplayRef:
    return _DisplayRef(label=f'unknown reference "{identifier}"')
