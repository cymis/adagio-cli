import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from rich.console import Console

from adagio.monitor.api import Monitor

from .base import (
    TaskEnvironmentLauncher,
    TaskEnvironmentSpec,
    TaskExecutionRequest,
    TaskExecutionResult,
)
from .container_support import (
    container_python_root,
    print_filtered_container_stderr,
    python_warning_env_assignments,
    record_container_output,
    signal_task_running,
)
from .task_contract import (
    build_task_spec,
    container_log_path,
    parse_result_manifest,
    read_json_file,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)


class CondaTaskEnvironmentLauncher(TaskEnvironmentLauncher):
    kind = "conda"

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console: Console | None = None,
        monitor: Monitor | None = None,
        task_id: str | None = None,
    ) -> TaskExecutionResult:
        task = request.task
        event_task_id = task_id if task_id is not None else task.id
        manifest_path = result_manifest_path(
            task_id=task.id, work_path=request.work_path
        )
        spec_path = task_spec_path(task_id=task.id, work_path=request.work_path)
        task_spec = build_task_spec(
            plugin=task.plugin,
            action=task.action,
            archive_inputs=dict(request.archive_inputs),
            archive_input_materializations=(
                dict(request.archive_input_materializations or {})
            ),
            archive_collection_inputs={
                name: list(values)
                for name, values in request.archive_collection_inputs.items()
            },
            metadata_inputs=dict(request.metadata_inputs),
            params=dict(request.params),
            metadata_column_kwargs=dict(request.metadata_column_kwargs),
            outputs=dict(request.outputs),
            result_manifest=str(manifest_path),
            cache_path=request.cache_path,
            recycle_pool=request.recycle_pool,
        )
        write_json_file(spec_path, task_spec)

        options = dict(environment.options or {})
        reference = _conda_prefix(environment=environment)
        conda_executable = _resolve_conda_executable(options=options, prefix=reference)
        python_executable = _conda_python_executable(reference=reference)
        python_root = container_python_root(work_path=request.work_path)
        # Keep Conda in the launch path even though the interpreter is invoked
        # explicitly. Packages such as OpenJDK rely on activate.d hooks to
        # configure their runtime. ``--no-capture-output`` keeps conda's
        # capture layer from re-buffering output the CLI pipes itself.
        command = [
            conda_executable,
            "run",
            "--no-capture-output",
            "-p",
            reference,
            python_executable,
            "-m",
            "adagio.cli.task_exec",
            "--task",
            str(spec_path),
        ]
        label = f"conda run -p {reference}"

        if console is not None:
            if not getattr(console, "_adagio_inline_monitor_active", False):
                console.print(f"[dim]Task environment:[/dim] {label}")

        if monitor is not None:
            # Conda has no image to pull; the environment is already resolved on
            # the host, so we jump straight to the container-start phase.
            monitor.starting_container(task_id=event_task_id, image_ref=reference)
        signal_task_running(monitor=monitor, event_task_id=event_task_id)
        run_started = time.monotonic()
        try:
            result = subprocess.run(
                command,
                check=False,
                cwd=request.cwd,
                env=_subprocess_env(python_root=python_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise SystemExit(
                "Conda is required for conda task environment execution but was "
                "not found in PATH. Set ADAGIO_CONDA_EXE or conda_executable in "
                "the runtime config."
            ) from exc
        run_seconds = time.monotonic() - run_started

        # Persist the conda subprocess output to a per-task log so it matches the
        # docker/apptainer launchers and can be copied out by ``--log-dir``.
        # Previously conda wrote no container log at all.
        log_path = container_log_path(task_id=task.id, work_path=request.work_path)
        record_container_output(
            log_path=log_path,
            stdout_text=result.stdout or "",
            stderr_text=result.stderr or "",
        )

        if console is not None:
            print_filtered_container_stderr(
                console=console, stderr_text=result.stderr or ""
            )

        if result.returncode != 0:
            stdout_text = (result.stdout or "").strip()
            stderr_text = (result.stderr or "").strip()
            if stderr_text:
                detail = f" Environment reported: {stderr_text}"
            elif stdout_text:
                detail = f" Environment stdout: {stdout_text}"
            else:
                detail = ""
            raise RuntimeError(
                f"Task {task.id!r} ({task.plugin}.{task.action}) failed "
                f"while launching conda environment {reference!r} "
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
            outputs[output_name] = actual_path

        return TaskExecutionResult(
            outputs=outputs,
            reused=reused,
            command=list(command),
            exit_code=result.returncode,
            image_ref=reference,
            log_path=str(log_path),
            timings={"run_seconds": run_seconds},
        )


def _conda_prefix(*, environment: TaskEnvironmentSpec) -> str:
    reference = environment.reference.strip()
    if not reference:
        raise RuntimeError(
            "Conda task environments require an environment path. "
            'Set prefix = "/path/to/env".'
        )
    # Already normalized by EnvironmentOverride.to_task_environment_override().
    return reference


def _conda_python_executable(*, reference: str) -> str:
    """Use the prefix's interpreter explicitly while retaining ``conda run`` hooks."""
    prefix = Path(reference)
    if os.name == "nt":
        return str(prefix / "python.exe")
    return str(prefix / "bin" / "python")


def _resolve_conda_executable(*, options: dict[str, Any], prefix: str) -> str:
    configured = options.get("conda_executable")
    if isinstance(configured, str) and configured.strip():
        resolved = _resolve_executable(configured.strip())
        if resolved is None:
            raise SystemExit(f"Configured conda executable not found: {configured}")
        return resolved

    for env_var in ("ADAGIO_CONDA_EXE", "CONDA_EXE"):
        candidate = os.getenv(env_var)
        if candidate:
            resolved = _resolve_executable(candidate)
            if resolved is not None:
                return resolved

    derived = _conda_executable_near_prefix(prefix)
    if derived is not None:
        return derived

    resolved = shutil.which("conda")
    if resolved is None:
        raise SystemExit(
            "Conda is required for conda task environment execution but was not "
            "found. Set ADAGIO_CONDA_EXE (or CONDA_EXE), set conda_executable "
            "in the runtime config, or install conda so it is on PATH."
        )
    return resolved


def _conda_executable_near_prefix(prefix: str) -> str | None:
    """Locate the conda install that owns the target prefix.

    Environments normally live under ``<root>/envs/<name>``; the base
    environment is the root itself. Either way the owning install's binary is
    derivable from the absolute prefix, so prefix-only references stay
    self-locating even on GUI launches with a minimal PATH.
    """
    prefix_path = Path(prefix)
    if prefix_path.parent.name == "envs":
        root = prefix_path.parent.parent
    else:
        root = prefix_path
    for candidate in (root / "condabin" / "conda", root / "bin" / "conda"):
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _resolve_executable(value: str) -> str | None:
    if _looks_like_path(value):
        path = Path(value).expanduser()
        return str(path) if path.exists() and path.is_file() else None
    return shutil.which(value)


def _subprocess_env(*, python_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    # The desktop sidecar may itself run from a bundled or virtual Python.
    # Those variables must not redirect the selected environment's interpreter.
    env.pop("PYTHONHOME", None)
    env.pop("VIRTUAL_ENV", None)
    env["PYTHONPATH"] = str(python_root)
    env["PYTHONNOUSERSITE"] = "1"

    for assignment in python_warning_env_assignments():
        name, _, value = assignment.partition("=")
        if name:
            env[name] = value

    return env


def _looks_like_path(value: str) -> bool:
    separators = [os.sep]
    if os.altsep is not None:
        separators.append(os.altsep)
    return any(separator in value for separator in separators)
