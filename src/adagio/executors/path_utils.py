import os
from pathlib import Path
from typing import Iterable

from .container_support import is_uri


def resolve_host_path(*, source: str, cwd: Path) -> str:
    if is_uri(source):
        return source
    path = Path(source)
    if path.is_absolute():
        return str(path.resolve())
    return str((cwd / path).resolve())


def resolve_output_destination(
    *,
    output_name: str,
    output_names: Iterable[str],
    outputs: str | dict[str, str],
    source_path: Path,
) -> str:
    suffix = source_path.suffix

    if isinstance(outputs, str):
        return append_output_suffix(os.path.join(outputs, output_name), suffix)

    if isinstance(outputs, dict):
        raw_dest = outputs.get(output_name)
        if raw_dest is None:
            expected_outputs = ", ".join(sorted(output_names))
            provided_outputs = ", ".join(sorted(outputs.keys())) or "<none>"
            raise KeyError(
                "Missing destination for output "
                f"{output_name!r}. Expected output names: [{expected_outputs}]. "
                f"Provided output names: [{provided_outputs}]."
            )
        return append_output_suffix(raw_dest, suffix)

    raise TypeError("Unsupported outputs configuration.")


def append_output_suffix(destination: str, suffix: str) -> str:
    if suffix and not destination.endswith(suffix):
        return destination + suffix
    return destination
