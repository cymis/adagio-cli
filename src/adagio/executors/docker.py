import subprocess
import time
from pathlib import Path

from rich.console import Console

from adagio.monitor.api import Monitor

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
    manifest_referenced_host_paths,
    print_filtered_container_stderr,
    python_warning_env_flags,
    record_container_output,
    with_mounts,
)
from .task_contract import (
    parse_result_manifest,
    build_task_spec,
    container_log_path,
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
        monitor: Monitor | None = None,
        task_id: str | None = None,
    ) -> TaskExecutionResult:
        task = request.task
        event_task_id = task_id if task_id is not None else task.id
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
            archive_input_materializations=(
                dict(request.archive_input_materializations or {})
            ),
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
        # A raw-manifest input points at fastqs by absolute host path; mount the
        # roots of those interior paths too (they may live off a different root
        # than the manifest/cwd/cache), so the in-container remap can resolve.
        host_paths.extend(
            manifest_referenced_host_paths(
                archive_inputs=request.archive_inputs,
                materializations=request.archive_input_materializations,
            )
        )

        command = with_mounts(command=command, host_paths=host_paths)

        if console is not None:
            label = f"docker {environment.reference}"
            if platform:
                label = f"docker --platform {platform} {environment.reference}"
            if not getattr(console, "_adagio_inline_monitor_active", False):
                console.print(f"[dim]Task environment:[/dim] {label}")

        # Pull the image up front (first run only) behind a single line, so the
        # `docker run` below stays quiet instead of dumping the layer-by-layer
        # pull log to the console. Safe because the executor runs tasks serially.
        pull_started = time.monotonic()
        if monitor is not None:
            monitor.pulling_image(
                task_id=event_task_id, image_ref=environment.reference
            )
        _ensure_docker_image(
            reference=environment.reference, platform=platform, console=console
        )
        pull_seconds = time.monotonic() - pull_started

        image_digest = _resolve_image_digest(reference=environment.reference)

        if monitor is not None:
            monitor.starting_container(
                task_id=event_task_id, image_ref=environment.reference
            )
        run_started = time.monotonic()
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
        run_seconds = time.monotonic() - run_started
        timings = {"pull_seconds": pull_seconds, "run_seconds": run_seconds}

        # Capture container output to a per-task log; keep the console clean on
        # success and surface it only when the task fails.
        log_path = container_log_path(task_id=task.id, work_path=request.work_path)
        record_container_output(
            log_path=log_path,
            stdout_text=result.stdout or "",
            stderr_text=result.stderr or "",
        )

        if result.returncode != 0:
            if console is not None:
                print_filtered_container_stderr(
                    console=console, stderr_text=result.stderr or ""
                )
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

        return TaskExecutionResult(
            outputs=outputs,
            reused=reused,
            command=list(command),
            exit_code=result.returncode,
            image_ref=environment.reference,
            image_digest=image_digest,
            log_path=str(log_path),
            timings=timings,
        )


def _resolve_image_digest(*, reference: str) -> str | None:
    """Best-effort resolve an image's repo digest (``sha256:…``).

    Uses ``docker image inspect --format '{{index .RepoDigests 0}}'`` (design
    §5.1). Returns the ``…@sha256:…`` digest string, or ``None`` when Docker is
    absent, the image is not present, or it has no repo digest (e.g. built
    locally). Never raises — enrichment must not break a run.
    """
    if is_uri(reference):
        return None
    try:
        result = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{index .RepoDigests 0}}",
                reference,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (FileNotFoundError, OSError):
        return None
    if result.returncode != 0:
        return None
    digest = (result.stdout or "").strip()
    return digest or None


def _ensure_docker_image(
    *, reference: str, platform: str | None, console: Console | None
) -> None:
    """Pull a missing image once, behind a single console line.

    Keeps the layer-by-layer pull log out of the run output: ``docker pull
    --quiet`` prints only the digest, and once the image is present ``docker
    run`` performs no pull at all. Only runs on the first use of an image; the
    executor is serial, so there is no concurrent-pull race to guard against.
    """
    if is_uri(reference):
        return
    try:
        present = (
            subprocess.run(
                ["docker", "image", "inspect", reference],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            ).returncode
            == 0
        )
    except FileNotFoundError as exc:
        raise SystemExit(
            "Docker is required for task environment execution but was not found in PATH."
        ) from exc
    if present:
        return

    if console is not None and not getattr(
        console, "_adagio_inline_monitor_active", False
    ):
        console.print(f"[dim]Pulling image[/dim] {reference} [dim](first run)…[/dim]")

    command = ["docker", "pull", "--quiet"]
    if platform:
        command.extend(["--platform", platform])
    command.append(reference)
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to pull image {reference!r}: {(result.stderr or '').strip()}"
        )
