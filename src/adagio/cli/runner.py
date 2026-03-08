import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from rich.console import Console

DEFAULT_CONTAINER_IMAGE = "sloth-adagio-cli:latest"
HOST_MOUNT_POINT = "/host"
DEFAULT_OUTPUT_DIRNAME = "adagio-outputs"


def run_pipeline_from_kwargs(
    pipeline: Path,
    arguments_file: Path | None,
    kwargs: dict[str, Any],
    input_bindings: list[tuple[str, str]],
    param_bindings: list[tuple[str, str]],
    required_inputs: list[str],
    required_params: list[str],
    *,
    console: Console,
) -> None:
    """Run a pipeline from resolved CLI keyword arguments."""
    from ..model.arguments import AdagioArgumentsFile
    from ..model.pipeline import AdagioPipeline

    data = json.loads(pipeline.read_text(encoding="utf-8"))
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    parsed_pipeline = AdagioPipeline.model_validate(pipeline_data)
    arguments = parsed_pipeline.signature.to_default_arguments()
    output_names = [output.name for output in parsed_pipeline.signature.outputs]

    input_names = {name for _, name in input_bindings}
    param_names = {name for _, name in param_bindings}
    output_name_set = set(output_names)

    if arguments_file is not None:
        file_data = json.loads(arguments_file.read_text(encoding="utf-8"))
        arguments_data = AdagioArgumentsFile.model_validate(file_data)

        unknown_inputs = sorted(set(arguments_data.inputs) - input_names)
        if unknown_inputs:
            raise SystemExit(
                "Unknown inputs in arguments file: " + ", ".join(unknown_inputs)
            )

        unknown_params = sorted(set(arguments_data.parameters) - param_names)
        if unknown_params:
            raise SystemExit(
                "Unknown parameters in arguments file: " + ", ".join(unknown_params)
            )

        unknown_outputs: list[str] = []
        if isinstance(arguments_data.outputs, dict):
            unknown_outputs = sorted(set(arguments_data.outputs) - output_name_set)
        if unknown_outputs:
            raise SystemExit(
                "Unknown outputs in arguments file: " + ", ".join(unknown_outputs)
            )

        arguments.inputs.update(arguments_data.inputs)
        arguments.parameters.update(arguments_data.parameters)
        if arguments_data.outputs is not None:
            arguments.outputs = arguments_data.outputs

    for ident, original in input_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.inputs[original] = str(value)

    for ident, original in param_bindings:
        value = kwargs.get(ident)
        if value is not None:
            arguments.parameters[original] = value

    missing_inputs = [
        name for name in required_inputs if _is_missing(arguments.inputs.get(name))
    ]
    missing_params = [
        name for name in required_params if _is_missing(arguments.parameters.get(name))
    ]
    if missing_inputs or missing_params:
        missing = [f"input:{name}" for name in missing_inputs] + [
            f"param:{name}" for name in missing_params
        ]
        raise SystemExit("Missing required arguments: " + ", ".join(missing))

    arguments.outputs = _resolve_output_destinations(
        outputs=arguments.outputs,
        output_names=output_names,
        cwd=Path.cwd().resolve(),
    )

    suppress_header = _is_truthy(os.getenv("ADAGIO_SUPPRESS_RUN_HEADER"))
    if not suppress_header:
        console.print(f"[bold]Pipeline:[/bold] {pipeline}")

    force_container = _is_truthy(os.getenv("ADAGIO_FORCE_CONTAINER"))
    local_qiime_error = _probe_local_qiime_error()
    if force_container or local_qiime_error is not None:
        if force_container:
            if not suppress_header:
                console.print("[bold]Executing pipeline[/bold] (container mode; forced)")
        else:
            if not suppress_header:
                console.print("[bold]Executing pipeline[/bold] (container mode)")
                console.print(
                    "[yellow]Local QIIME unavailable, falling back to Docker:[/yellow] "
                    f"{local_qiime_error}"
                )
        _execute_via_container(pipeline=pipeline, arguments=arguments, console=console)
        return

    from ..monitor.tty import RichMonitor
    from ..serial_execute import execute_serial

    if not suppress_header:
        console.print("[bold]Executing pipeline[/bold] (qiime serial mode)")
    execute_serial(
        pipeline=parsed_pipeline,
        arguments=arguments,
        monitor=RichMonitor(console=console),
    )


def _is_missing(value: Any) -> bool:
    """Treat placeholders and null values as missing."""
    return value is None or value == "<fill me>"


def _is_missing_output(value: Any) -> bool:
    if not isinstance(value, str):
        return True
    return value == "" or value == "<fill me>"


def _resolve_output_destinations(
    *,
    outputs: str | dict[str, str],
    output_names: list[str],
    cwd: Path,
) -> str | dict[str, str]:
    default_output_dir = (cwd / DEFAULT_OUTPUT_DIRNAME).resolve()
    if isinstance(outputs, str):
        if _is_missing_output(outputs):
            return str(default_output_dir)
        return outputs

    if not isinstance(outputs, dict):
        raise TypeError("Unsupported outputs configuration.")

    resolved = dict(outputs)
    for output_name in output_names:
        value = resolved.get(output_name)
        if _is_missing_output(value):
            resolved[output_name] = str((default_output_dir / output_name).resolve())
    return resolved


def _probe_local_qiime_error() -> str | None:
    """Return an error string if local QIIME cannot satisfy serial execution imports."""
    try:
        import qiime2  # noqa: F401
        from qiime2 import get_cache  # noqa: F401
        from qiime2.sdk import PluginManager  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    return None


def _execute_via_container(*, pipeline: Path, arguments: Any, console: Console) -> None:
    """Execute pipeline in the shared adagio-cli Docker image."""
    image = (os.getenv("ADAGIO_CONTAINER_IMAGE") or DEFAULT_CONTAINER_IMAGE).strip()
    host_cwd = Path.cwd().resolve()
    host_src_root = _local_source_root()
    host_paths = _collect_host_paths(
        pipeline=pipeline.resolve(),
        arguments=arguments,
        cwd=host_cwd,
    )
    host_paths.append(host_src_root)
    run_arguments = _to_container_run_arguments(arguments=arguments)

    with tempfile.TemporaryDirectory(prefix="adagio-runtime-") as temp_dir:
        temp_path = Path(temp_dir)
        args_path = temp_path / "arguments.json"
        host_paths.append(args_path.resolve())

        args_path.write_text(
            json.dumps(run_arguments, ensure_ascii=True),
            encoding="utf-8",
        )

        command = [
            "docker",
            "run",
            "--rm",
            *_docker_tty_flags(),
            "-e",
            f"PYTHONPATH={_containerize_path(host_src_root)}",
            "-e",
            "ADAGIO_SUPPRESS_RUN_HEADER=1",
            *_python_warning_env_flags(),
            "-w",
            _containerize_path(host_cwd),
            image,
            "python",
            "-m",
            "adagio.cli.main",
            "run",
            "--pipeline",
            _containerize_path(pipeline.resolve()),
            "--arguments",
            _containerize_path(args_path),
            "--show-params",
            "all",
        ]
        command = _with_mounts(command=command, host_paths=host_paths)

        console.print(f"[dim]Container image:[/dim] {image}")
        try:
            result = subprocess.run(
                command,
                check=False,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise SystemExit(
                "Docker is required for container fallback but was not found in PATH."
            ) from exc

        _print_filtered_container_stderr(console=console, stderr_text=result.stderr or "")

        if result.returncode != 0:
            raise SystemExit(result.returncode)


def _to_container_run_arguments(*, arguments: Any) -> dict[str, Any]:
    """Serialize `adagio run` arguments and rewrite absolute host paths."""
    data = arguments.model_dump() if hasattr(arguments, "model_dump") else dict(arguments)
    inputs = data.get("inputs", {})
    outputs = data.get("outputs")

    if isinstance(inputs, dict):
        data["inputs"] = {
            key: _containerize_host_value(value) if isinstance(value, str) else value
            for key, value in inputs.items()
        }

    if isinstance(outputs, str):
        data["outputs"] = (
            _containerize_host_value(outputs)
            if not _is_missing(outputs)
            else outputs
        )
    elif isinstance(outputs, dict):
        data["outputs"] = {
            key: _containerize_host_value(value)
            if isinstance(value, str) and not _is_missing(value)
            else value
            for key, value in outputs.items()
        }

    return {
        "version": 1,
        "inputs": data.get("inputs", {}),
        "parameters": data.get("parameters", {}),
        "outputs": data.get("outputs"),
    }


def _collect_host_paths(
    *, pipeline: Path, arguments: Any, cwd: Path
) -> list[Path]:
    """Collect absolute host paths that must be visible in the container."""
    data = arguments.model_dump() if hasattr(arguments, "model_dump") else dict(arguments)
    paths = [pipeline, cwd]

    inputs = data.get("inputs", {})
    if isinstance(inputs, dict):
        for value in inputs.values():
            if isinstance(value, str) and not _is_uri(value):
                as_path = Path(value)
                if as_path.is_absolute():
                    paths.append(as_path)

    outputs = data.get("outputs")
    if isinstance(outputs, str):
        if not _is_missing(outputs) and not _is_uri(outputs):
            as_path = Path(outputs)
            if as_path.is_absolute():
                paths.append(as_path)
    elif isinstance(outputs, dict):
        for value in outputs.values():
            if isinstance(value, str) and not _is_missing(value) and not _is_uri(value):
                as_path = Path(value)
                if as_path.is_absolute():
                    paths.append(as_path)

    return [path.resolve() for path in paths]


def _with_mounts(*, command: list[str], host_paths: list[Path]) -> list[str]:
    """Attach bind mounts for top-level host roots needed by this execution."""
    roots = _mount_roots(host_paths)
    mount_flags: list[str] = []
    for root in roots:
        mount_flags.extend(
            [
                "-v",
                f"{root}:{_containerize_path(root)}:rw",
            ]
        )
    return [*command[:3], *mount_flags, *command[3:]]


def _docker_tty_flags() -> list[str]:
    """Allocate Docker TTY when the current session is interactive."""
    if sys.stdin.isatty() and sys.stdout.isatty():
        return ["-t"]
    return []


def _python_warning_env_flags() -> list[str]:
    """Suppress known noisy runtime warnings in container mode."""
    filters = os.getenv("ADAGIO_PYTHONWARNINGS")
    if filters is None:
        filters = "ignore:pkg_resources is deprecated as an API:UserWarning"
    filters = filters.strip()
    if not filters:
        return []
    return ["-e", f"PYTHONWARNINGS={filters}"]


def _mount_roots(paths: list[Path]) -> list[Path]:
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


def _containerize_host_value(value: str) -> str:
    """Map an absolute host path into the container mount."""
    if _is_uri(value):
        return value
    as_path = Path(value)
    if as_path.is_absolute():
        return _containerize_path(as_path)
    return value


def _containerize_path(path: Path) -> str:
    """Convert absolute host path to mounted container path."""
    resolved = path.resolve()
    return f"{HOST_MOUNT_POINT}{resolved}"


def _is_uri(value: str) -> bool:
    return "://" in value


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _local_source_root() -> Path:
    """Return the local `adagio-cli/src` path for container PYTHONPATH."""
    return Path(__file__).resolve().parents[2]


def _print_filtered_container_stderr(*, console: Console, stderr_text: str) -> None:
    """Print relevant stderr lines while dropping known noisy platform warnings."""
    if not stderr_text:
        return
    for line in stderr_text.splitlines():
        if _is_docker_platform_warning(line):
            continue
        if not line.strip():
            continue
        console.print(line)


def _is_docker_platform_warning(line: str) -> bool:
    return (
        "requested image's platform" in line
        and "does not match the detected host platform" in line
    )
