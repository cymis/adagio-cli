import shutil
import subprocess
from pathlib import Path

from rich.console import Console

from .base import (
    TaskEnvironmentLauncher,
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from .cache_support import mount_path_for_cache
from .container_support import (
    container_python_root,
    containerize_host_value,
    containerize_path,
    host_path_from_container,
    is_uri,
    print_filtered_container_stderr,
    python_warning_env_assignments,
    with_apptainer_binds,
)
from .task_contract import (
    build_task_spec,
    parse_result_manifest,
    read_json_file,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)


class ApptainerTaskEnvironmentLauncher(TaskEnvironmentLauncher):
    kind = "apptainer"

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console: Console | None = None,
    ) -> TaskExecutionResult:
        image_path = _resolve_sif_image(environment.reference)
        runtime_executable = _resolve_runtime_executable()

        task = request.task
        archive_inputs = {
            name: containerize_host_value(value)
            for name, value in request.archive_inputs.items()
        }
        archive_collection_inputs = {
            name: [containerize_host_value(value) for value in values]
            for name, values in request.archive_collection_inputs.items()
        }
        metadata_inputs = {
            name: containerize_host_value(value)
            for name, value in request.metadata_inputs.items()
        }
        outputs = {
            name: containerize_path(Path(path))
            for name, path in request.outputs.items()
        }

        manifest_path = result_manifest_path(
            task_id=task.id, work_path=request.work_path
        )
        spec_path = task_spec_path(task_id=task.id, work_path=request.work_path)
        task_spec = build_task_spec(
            plugin=task.plugin,
            action=task.action,
            archive_inputs=archive_inputs,
            archive_collection_inputs=archive_collection_inputs,
            metadata_inputs=metadata_inputs,
            params=dict(request.params),
            metadata_column_kwargs=dict(request.metadata_column_kwargs),
            outputs=outputs,
            result_manifest=containerize_path(manifest_path),
            cache_path=(
                containerize_path(Path(request.cache_path))
                if request.cache_path is not None
                else None
            ),
            recycle_pool=request.recycle_pool,
        )
        write_json_file(spec_path, task_spec)

        python_root = container_python_root(work_path=request.work_path)
        command = [
            runtime_executable,
            "exec",
            "--cleanenv",
            "--no-home",
            "--pwd",
            containerize_path(request.cwd),
        ]

        host_paths = [request.cwd, request.work_path, python_root]
        for value in (
            list(request.archive_inputs.values())
            + [item for values in request.archive_collection_inputs.values() for item in values]
            + list(request.metadata_inputs.values())
        ):
            if is_uri(value):
                continue
            path = Path(value)
            if path.is_absolute():
                host_paths.append(path)
        if request.cache_path is not None:
            host_paths.append(mount_path_for_cache(Path(request.cache_path)))

        command = with_apptainer_binds(command=command, host_paths=host_paths)
        command.extend(
            [
                str(image_path),
                "env",
                f"PYTHONPATH={containerize_path(python_root)}",
                "PYTHONNOUSERSITE=1",
                *python_warning_env_assignments(),
                "python",
                "-m",
                "adagio.cli.task_exec",
                "--task",
                containerize_path(spec_path),
            ]
        )

        if console is not None:
            label = f"{Path(runtime_executable).name} {image_path}"
            if not getattr(console, "_adagio_inline_monitor_active", False):
                console.print(f"[dim]Task environment:[/dim] {label}")

        try:
            result = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise SystemExit(
                "Apptainer/Singularity is required for task environment execution "
                "but was not found in PATH. Ensure the job environment includes the "
                "Apptainer binary location."
            ) from exc

        if console is not None:
            print_filtered_container_stderr(
                console=console, stderr_text=result.stderr or ""
            )

        if result.returncode != 0:
            stdout_text = (result.stdout or "").strip()
            stderr_text = (result.stderr or "").strip()
            if stderr_text:
                detail = f" Runtime reported: {stderr_text}"
            elif stdout_text:
                detail = f" Container stdout: {stdout_text}"
            else:
                detail = ""
            raise RuntimeError(
                f"Task {task.id!r} ({task.plugin}.{task.action}) failed "
                f"while launching environment {str(image_path)!r} "
                f"with exit code {result.returncode}.{detail}"
            )

        if not manifest_path.exists():
            raise RuntimeError(
                f"Task {task.id!r} completed but did not write an output manifest."
            )

        output_manifest = read_json_file(manifest_path)
        reported_outputs, reused = parse_result_manifest(output_manifest)
        resolved_outputs = {}
        for output_name in request.outputs:
            actual_path = reported_outputs.get(output_name)
            if not isinstance(actual_path, str):
                raise RuntimeError(
                    f"Task {task.id!r} did not report output {output_name!r}."
                )
            resolved_outputs[output_name] = str(host_path_from_container(actual_path))

        return TaskExecutionResult(outputs=resolved_outputs, reused=reused)


def _resolve_runtime_executable() -> str:
    for candidate in ("apptainer", "singularity"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise SystemExit(
        "Apptainer/Singularity is required for task environment execution "
        "but was not found in PATH. Ensure the job environment includes the "
        "Apptainer binary location."
    )


def _resolve_sif_image(reference: str) -> Path:
    if is_uri(reference):
        raise RuntimeError(
            "Apptainer task environments currently support only local .sif image paths."
        )

    image_path = Path(reference).expanduser().resolve()
    if image_path.suffix.lower() != ".sif":
        raise RuntimeError(
            f"Apptainer task environments require a local .sif image path, got {reference!r}."
        )
    if not image_path.exists():
        raise RuntimeError(f"Apptainer image not found: {image_path}")
    if not image_path.is_file():
        raise RuntimeError(f"Apptainer image is not a file: {image_path}")
    return image_path
