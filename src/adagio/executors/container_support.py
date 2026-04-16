import os
import shutil
import sys
from pathlib import Path

from rich.console import Console

HOST_MOUNT_POINT = "/host"
STAGED_CONTAINER_PYTHON_ROOT = ".adagio-container-python"


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
    source_root = _adagio_source_root()
    if source_root is None:
        raise RuntimeError("Adagio source root is unavailable from this installation.")
    return source_root


def container_python_root(*, work_path: Path, module_file: Path | None = None) -> Path:
    """Return an isolated Python root that exposes only the Adagio package."""
    source_root = _adagio_source_root(module_file=module_file)
    if source_root is not None:
        return source_root

    package_dir = _adagio_package_dir(module_file=module_file)
    staged_root = (work_path / STAGED_CONTAINER_PYTHON_ROOT).resolve()
    _stage_adagio_package(package_dir=package_dir, staged_root=staged_root)
    return staged_root


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


def _adagio_source_root(*, module_file: Path | None = None) -> Path | None:
    package_dir = _adagio_package_dir(module_file=module_file)
    candidate = package_dir.parent
    if candidate.name != "src":
        return None
    if not (candidate / "adagio" / "__init__.py").is_file():
        return None
    return candidate


def _adagio_package_dir(*, module_file: Path | None = None) -> Path:
    resolved = (module_file or Path(__file__)).resolve()
    return resolved.parents[1]


def _stage_adagio_package(*, package_dir: Path, staged_root: Path) -> None:
    staged_package_dir = staged_root / package_dir.name
    if staged_package_dir.exists():
        shutil.rmtree(staged_package_dir)
    staged_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        package_dir,
        staged_package_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
