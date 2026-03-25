import os
import sys
from pathlib import Path

from rich.console import Console

HOST_MOUNT_POINT = "/host"


def with_mounts(*, command: list[str], host_paths: list[Path]) -> list[str]:
    """Attach bind mounts for top-level host roots needed by this execution."""
    roots = mount_roots(host_paths)
    mount_flags: list[str] = []
    for root in roots:
        mount_flags.extend(
            [
                "-v",
                f"{root}:{containerize_path(root)}:rw",
            ]
        )
    return [*command[:3], *mount_flags, *command[3:]]


def with_apptainer_binds(*, command: list[str], host_paths: list[Path]) -> list[str]:
    """Attach bind mounts for top-level host roots needed by Apptainer/Singularity."""
    roots = mount_roots(host_paths)
    bind_flags: list[str] = []
    for root in roots:
        bind_flags.extend(
            [
                "--bind",
                f"{root}:{containerize_path(root)}:rw",
            ]
        )
    return [*command[:2], *bind_flags, *command[2:]]


def docker_tty_flags() -> list[str]:
    """Allocate Docker TTY when the current session is interactive."""
    if sys.stdin.isatty() and sys.stdout.isatty():
        return ["-t"]
    return []


def python_warning_env_assignments() -> list[str]:
    """Return runtime warning environment assignments for container execution."""
    filters = os.getenv("ADAGIO_PYTHONWARNINGS")
    if filters is None:
        filters = "ignore:pkg_resources is deprecated as an API:UserWarning"
    filters = filters.strip()
    if not filters:
        return []
    return [f"PYTHONWARNINGS={filters}"]


def python_warning_env_flags() -> list[str]:
    """Suppress known noisy runtime warnings in container mode."""
    flags: list[str] = []
    for assignment in python_warning_env_assignments():
        flags.extend(["-e", assignment])
    return flags


def mount_roots(paths: list[Path]) -> list[Path]:
    """Map paths to their first-level filesystem roots for portable bind mounts."""
    roots: set[Path] = set()
    for path in paths:
        parts = path.parts
        if len(parts) < 2:
            continue
        root = Path("/", parts[1])
        if root.exists():
            roots.add(root)
    return sorted(roots)


def containerize_host_value(value: str) -> str:
    """Map an absolute host path into the container mount."""
    if is_uri(value):
        return value
    as_path = Path(value)
    if as_path.is_absolute():
        return containerize_path(as_path)
    return value


def containerize_path(path: Path) -> str:
    """Convert an absolute host path to the mounted container path."""
    return f"{HOST_MOUNT_POINT}{path.resolve()}"


def host_path_from_container(value: str) -> Path:
    """Convert a mounted container path back to the original host path."""
    if not value.startswith(HOST_MOUNT_POINT):
        return Path(value)
    suffix = value[len(HOST_MOUNT_POINT) :]
    return Path(suffix).resolve()


def is_uri(value: str) -> bool:
    return "://" in value


def local_source_root() -> Path:
    """Return the local `adagio-cli/src` path for container PYTHONPATH."""
    return Path(__file__).resolve().parents[2]


def print_filtered_container_stderr(*, console: Console, stderr_text: str) -> None:
    """Print relevant stderr lines while dropping known noisy platform warnings."""
    if not stderr_text:
        return
    for line in stderr_text.splitlines():
        if is_docker_platform_warning(line):
            continue
        if not line.strip():
            continue
        console.print(line)


def is_docker_platform_warning(line: str) -> bool:
    return (
        "requested image's platform" in line
        and "does not match the detected host platform" in line
    )
