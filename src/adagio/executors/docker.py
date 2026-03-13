from __future__ import annotations

import json
import subprocess
from pathlib import Path

from rich.console import Console

from .base import (
    TaskEnvironmentLauncher,
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from .container_support import (
    containerize_host_value,
    containerize_path,
    docker_tty_flags,
    host_path_from_container,
    is_uri,
    local_source_root,
    print_filtered_container_stderr,
    python_warning_env_flags,
    with_mounts,
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
        metadata_inputs = {
            name: containerize_host_value(value)
            for name, value in request.metadata_inputs.items()
        }
        outputs = {
            name: containerize_path(Path(path))
            for name, path in request.outputs.items()
        }

        safe_id = task.id.replace("/", "_").replace(" ", "_")
        result_manifest_path = (request.work_path / f"{safe_id}_results.json").resolve()
        task_spec = {
            "plugin": task.plugin,
            "action": task.action,
            "archive_inputs": archive_inputs,
            "metadata_inputs": metadata_inputs,
            "params": dict(request.params),
            "metadata_column_kwargs": dict(request.metadata_column_kwargs),
            "outputs": outputs,
            "result_manifest": containerize_path(result_manifest_path),
        }

        task_spec_path = (request.work_path / f"{safe_id}_spec.json").resolve()
        task_spec_path.write_text(json.dumps(task_spec, ensure_ascii=True), encoding="utf-8")

        src_root = local_source_root()
        command = [
            "docker",
            "run",
            "--rm",
            *docker_tty_flags(),
            "-e",
            f"PYTHONPATH={containerize_path(src_root)}",
            *python_warning_env_flags(),
            "-w",
            containerize_path(request.cwd),
            environment.reference,
            "python",
            "-m",
            "adagio.cli.task_exec",
            "--task",
            containerize_path(task_spec_path),
        ]

        host_paths = [request.cwd, request.work_path, src_root]
        for value in list(request.archive_inputs.values()) + list(request.metadata_inputs.values()):
            if is_uri(value):
                continue
            path = Path(value)
            if path.is_absolute():
                host_paths.append(path)

        command = with_mounts(command=command, host_paths=host_paths)

        if console is not None:
            console.print(f"[dim]Task environment:[/dim] docker {environment.reference}")

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

        if not result_manifest_path.exists():
            raise RuntimeError(
                f"Task {task.id!r} completed but did not write an output manifest."
            )

        output_manifest = json.loads(result_manifest_path.read_text(encoding="utf-8"))
        outputs = {}
        for output_name in request.outputs:
            actual_path = output_manifest.get(output_name)
            if not isinstance(actual_path, str):
                raise RuntimeError(
                    f"Task {task.id!r} did not report output {output_name!r}."
                )
            outputs[output_name] = str(host_path_from_container(actual_path))

        return TaskExecutionResult(outputs=outputs)
