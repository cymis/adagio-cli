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
    docker_tty_flags,
    host_path_from_container,
    is_uri,
    print_filtered_container_stderr,
    python_warning_env_flags,
    with_mounts,
)
from .task_contract import (
    parse_result_manifest,
    build_task_spec,
    read_json_file,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)


class DockerTaskEnvironmentLauncher(TaskEnvironmentLauncher):
    kind = "docker"

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console: Console | None = None,
    ) -> TaskExecutionResult:
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

        manifest_path = result_manifest_path(task_id=task.id, work_path=request.work_path)
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
        platform = None
        if environment.options is not None:
            raw_platform = environment.options.get("platform")
            if isinstance(raw_platform, str) and raw_platform:
                platform = raw_platform

        command = [
            "docker",
            "run",
            "--rm",
            *docker_tty_flags(),
            "-e",
            f"PYTHONPATH={containerize_path(python_root)}",
            "-e",
            "PYTHONNOUSERSITE=1",
            *python_warning_env_flags(),
            "-w",
            containerize_path(request.cwd),
        ]
        if platform:
            command.extend(["--platform", platform])
        command.extend([
            environment.reference,
            "python",
            "-m",
            "adagio.cli.task_exec",
            "--task",
            containerize_path(spec_path),
        ])

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

        command = with_mounts(command=command, host_paths=host_paths)

        if console is not None:
            label = f"docker {environment.reference}"
            if platform:
                label = f"docker --platform {platform} {environment.reference}"
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
                "Docker is required for task environment execution but was not found in PATH."
            ) from exc

        if console is not None:
            print_filtered_container_stderr(console=console, stderr_text=result.stderr or "")

        if result.returncode != 0:
            stdout_text = (result.stdout or "").strip()
            stderr_text = (result.stderr or "").strip()
            if stderr_text:
                detail = f" Docker reported: {stderr_text}"
            elif stdout_text:
                detail = f" Container stdout: {stdout_text}"
            else:
                detail = ""
            raise RuntimeError(
                f"Task {task.id!r} ({task.plugin}.{task.action}) failed "
                f"while launching environment {environment.reference!r} "
                f"with exit code {result.returncode}.{detail}"
            )

        if not manifest_path.exists():
            raise RuntimeError(
                f"Task {task.id!r} completed but did not write an output manifest."
            )

        output_manifest = read_json_file(manifest_path)
        reported_outputs, reused = parse_result_manifest(output_manifest)
        outputs = {}
        for output_name in request.outputs:
            actual_path = reported_outputs.get(output_name)
            if not isinstance(actual_path, str):
                raise RuntimeError(
                    f"Task {task.id!r} did not report output {output_name!r}."
                )
            outputs[output_name] = str(host_path_from_container(actual_path))

        return TaskExecutionResult(outputs=outputs, reused=reused)
