import re
import textwrap

from rich.text import Text


TYPE_STYLE = "bold yellow"
SEMANTIC_TYPE_STYLE = "bold #84ad50"


def compact_type_text(type_text: str) -> str:
    cleaned = type_text.strip()
    if "Choices(" not in cleaned:
        return f"({cleaned})"

    match = re.search(r"Choices\((.*)\)", cleaned)
    if match is None:
        return f"({cleaned})"

    choices = [
        choice.strip().strip("'\"")
        for choice in match.group(1).split(",")
        if choice.strip()
    ]
    if not choices:
        return f"({cleaned})"
    return "[" + "|".join(choices) + "]"


def path_type_label(spec_type: str | None) -> str:
    cleaned = (spec_type or "").strip()
    if not cleaned:
        return "PATH"
    return f"PATH\n{cleaned}"


def render_type_text(label: str, width: int) -> Text:
    wrapped = wrap_type_label(label, width)
    lines = wrapped.split("\n")
    has_semantic_path_type = label.startswith("PATH\n")
    rendered = Text()
    for index, line in enumerate(lines):
        if index:
            rendered.append("\n")
        style = (
            SEMANTIC_TYPE_STYLE
            if has_semantic_path_type and index > 0
            else TYPE_STYLE
        )
        rendered.append(line, style=style)
    return rendered


def wrap_type_label(label: str, width: int) -> str:
    return "\n".join(
        line
        for raw_line in label.splitlines()
        for line in _wrap_type_label_line(raw_line, width)
    )


def type_label_display_width(label: str) -> int:
    return max((len(line) for line in label.splitlines()), default=0)


def _wrap_type_label_line(label: str, width: int) -> list[str]:
    if len(label) <= width:
        return [label]
    if label.startswith("[") and label.endswith("]"):
        return _wrap_choice_label(label, width)
    if " | " in label:
        return _wrap_union_type_label(label, width)
    return _wrap_long_type_label(label, width)


def _wrap_choice_label(label: str, width: int) -> list[str]:
    choices = [choice for choice in label[1:-1].split("|") if choice]
    if not choices:
        return [label]

    lines: list[str] = []
    current = "["

    for index, choice in enumerate(choices):
        is_last = index == len(choices) - 1
        separator = "" if current in ("[", " |") else "|"
        suffix = "]" if is_last else ""
        candidate = current + separator + choice + suffix

        if len(candidate) <= width or current in ("[", " |"):
            current = candidate
        else:
            lines.append(current)
            current = " |" + choice + suffix

    if not current.endswith("]"):
        current += "]"
    lines.append(current)
    return lines


def _wrap_union_type_label(label: str, width: int) -> list[str]:
    members = [member for member in label.split(" | ") if member]
    if not members:
        return [label]

    lines: list[str] = []
    current = ""
    for index, member in enumerate(members):
        has_next = index < len(members) - 1
        candidate = f"{current} | {member}" if current else member
        if current and len(candidate) <= width:
            current = candidate
            continue
        if current:
            _append_union_line(lines, current, width)
            current = ""
        if len(member) <= width:
            current = member
        else:
            lines.extend(_wrap_long_type_label(member, width))
            if has_next:
                _append_separator_to_last_line(lines, width)
    if current:
        lines.append(current)
    return lines


def _append_union_line(lines: list[str], line: str, width: int) -> None:
    if len(line) + 2 <= width:
        lines.append(f"{line} |")
    else:
        lines.append(line)


def _append_separator_to_last_line(lines: list[str], width: int) -> None:
    if lines and len(lines[-1]) + 2 <= width:
        lines[-1] = f"{lines[-1]} |"


def _wrap_long_type_label(label: str, width: int) -> list[str]:
    break_long_words = any(len(token) > width for token in label.split())
    return textwrap.wrap(
        label,
        width=width,
        subsequent_indent="  ",
        break_long_words=break_long_words,
        break_on_hyphens=False,
    ) or [label]
