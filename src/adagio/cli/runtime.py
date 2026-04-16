import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from rich.console import Console

from ..executors.base import TaskEnvironmentOverride
from ..executors.cache_support import (
    CACHE_DIR_HELP,
    REUSE_HELP,
    resolve_cache_config,
)
from ..model.arguments import AdagioArguments
from ..model.pipeline import AdagioPipeline
from ..monitor.composite import CompositeMonitor
from ..monitor.connected import ConnectedMonitor
from ..monitor.log import LogMonitor
from .config import load_run_config


def run_runtime(argv: list[str], *, console: Console) -> None:
    """Runtime entrypoint used by the runtime-adapter job container."""
    parser = argparse.ArgumentParser(
        prog="adagio runtime",
        description=(
            "Execute a pipeline from spec/config/arguments files. "
            "The config file may define default, per-plugin, and per-task environment overrides."
        ),
    )
    parser.add_argument("--spec", required=True, help="Path to pipeline spec JSON.")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to runtime config TOML.",
    )
    parser.add_argument(
        "--arguments", required=False, help="Path to run arguments JSON."
    )
    parser.add_argument("--job-id", required=False, help="Runtime job ID.")
    parser.add_argument(
        "--output-dir", required=False, help="Directory for output artifacts."
    )
    parser.add_argument(
        "--runtime-url", required=False, help="Runtime adapter API base URL."
    )
    parser.add_argument(
        "--cache-dir",
        required=True,
        help=CACHE_DIR_HELP,
    )
    parser.add_argument(
        "--reuse",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=REUSE_HELP,
    )
    parser.add_argument(
        "--connected",
        action="store_true",
        help="Emit execution status updates to the runtime-adapter.",
    )

    opts = parser.parse_args(argv)

    spec_data = _load_json(Path(opts.spec))
    run_config = load_run_config(Path(opts.config))
    runtime_arguments: Any = {}
    if opts.arguments:
        runtime_arguments = _load_json(Path(opts.arguments))
    if runtime_arguments is None:
        runtime_arguments = {}

    pipeline = _parse_pipeline(spec_data)
    output_dir = _resolve_output_dir(opts.output_dir, opts.job_id)
    arguments = _build_arguments(
        pipeline=pipeline,
        runtime_arguments=runtime_arguments,
        output_dir=output_dir,
    )
    _validate_required_arguments(pipeline, arguments)
    cache_config = resolve_cache_config(
        cwd=Path.cwd().resolve(),
        cache_dir=opts.cache_dir,
        reuse=opts.reuse,
    )

    connected = bool(
        opts.connected
        and opts.job_id
        and (opts.runtime_url or os.getenv("RUNTIME_URL"))
    )
    runtime_url = opts.runtime_url or os.getenv("RUNTIME_URL")

    log_monitor = LogMonitor(console=console)
    monitor = log_monitor
    if connected and runtime_url:
        monitor = CompositeMonitor(
            log_monitor,
            ConnectedMonitor(runtime_url=runtime_url, job_id=opts.job_id or ""),
        )

    if connected and runtime_url and opts.job_id:
        _post_job_event(
            runtime_url=runtime_url,
            job_id=opts.job_id,
            payload={"event": "job_status", "status": "running"},
        )

    from ..executors import select_default_executor

    executor = select_default_executor(
        default_override=_default_override(run_config),
        plugin_overrides=_named_overrides(
            run_config.plugins if run_config is not None else {}
        ),
        task_overrides=_named_overrides(
            run_config.tasks if run_config is not None else {}
        ),
    )

    try:
        executor.execute(
            pipeline=pipeline,
            arguments=arguments,
            console=console,
            monitor=monitor,
            cache_config=cache_config,
        )
    except Exception as exc:  # noqa: BLE001
        if connected and runtime_url and opts.job_id:
            _post_job_event(
                runtime_url=runtime_url,
                job_id=opts.job_id,
                payload={"event": "job_status", "status": "failed", "error": str(exc)},
            )
        raise
    else:
        if connected and runtime_url and opts.job_id:
            _post_job_event(
                runtime_url=runtime_url,
                job_id=opts.job_id,
                payload={"event": "job_status", "status": "succeeded"},
            )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_pipeline(data: Any) -> AdagioPipeline:
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    return AdagioPipeline.model_validate(pipeline_data)


def _resolve_output_dir(raw_output_dir: str | None, job_id: str | None) -> str:
    if raw_output_dir:
        output_dir = raw_output_dir
    elif job_id:
        output_dir = f"/storage/runtime_jobs/{job_id}/outputs"
    else:
        output_dir = "/storage/runtime_outputs"
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _default_override(run_config: Any) -> TaskEnvironmentOverride | None:
    if run_config is None:
        return None
    defaults = run_config.defaults
    if defaults.kind is None and defaults.image is None and defaults.platform is None:
        return None
    return TaskEnvironmentOverride(
        kind=defaults.kind,
        reference=defaults.image,
        platform=defaults.platform,
    )


def _named_overrides(
    raw_overrides: dict[str, Any],
) -> dict[str, TaskEnvironmentOverride] | None:
    resolved = {
        name: TaskEnvironmentOverride(
            kind=override.kind,
            reference=override.image,
            platform=override.platform,
        )
        for name, override in raw_overrides.items()
        if override.kind is not None
        or override.image is not None
        or override.platform is not None
    }
    return resolved or None


def _build_arguments(
    *,
    pipeline: AdagioPipeline,
    runtime_arguments: Any,
    output_dir: str,
) -> AdagioArguments:
    arguments = pipeline.signature.to_default_arguments()
    storage_root = "/storage"

    if isinstance(runtime_arguments, dict):
        if isinstance(runtime_arguments.get("inputs"), dict):
            _apply_named_arguments(
                arguments=arguments,
                runtime_arguments=runtime_arguments,
                storage_root=storage_root,
            )
        else:
            _apply_legacy_arguments(
                pipeline=pipeline,
                arguments=arguments,
                runtime_arguments=runtime_arguments,
                storage_root=storage_root,
            )

        resolved_outputs = _resolve_outputs(
            runtime_arguments.get("outputs"), storage_root=storage_root
        )
        if resolved_outputs is not None:
            arguments.outputs = resolved_outputs

    if _outputs_need_default(arguments.outputs):
        arguments.outputs = output_dir

    return arguments


def _apply_named_arguments(
    *, arguments: AdagioArguments, runtime_arguments: dict[str, Any], storage_root: str
) -> None:
    raw_inputs = runtime_arguments.get("inputs", {})
    if isinstance(raw_inputs, dict):
        for name, value in raw_inputs.items():
            arguments.inputs[name] = _resolve_input_path(
                value, storage_root=storage_root
            )

    raw_parameters = runtime_arguments.get("parameters", {})
    if isinstance(raw_parameters, dict):
        arguments.parameters.update(raw_parameters)


def _apply_legacy_arguments(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    runtime_arguments: dict[str, Any],
    storage_root: str,
) -> None:
    preprocessing = runtime_arguments.get("preprocessing", {})
    root_artifacts = (
        preprocessing.get("root_artifacts", [])
        if isinstance(preprocessing, dict)
        else []
    )
    token_lookup: dict[str, Any] = {}
    if isinstance(root_artifacts, list):
        for artifact in root_artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_id = artifact.get("id")
            token = artifact.get("token")
            if artifact_id is None:
                continue
            token_lookup[str(artifact_id)] = token

    for input_def in pipeline.signature.inputs:
        token = token_lookup.get(str(input_def.id))
        if token is None:
            continue
        arguments.inputs[input_def.name] = _resolve_input_path(
            token, storage_root=storage_root
        )

    named_inputs = runtime_arguments.get("inputs", {})
    if isinstance(named_inputs, dict):
        for name, value in named_inputs.items():
            arguments.inputs[name] = _resolve_input_path(
                value, storage_root=storage_root
            )

    task_arguments = runtime_arguments.get("arguments", {})
    if isinstance(task_arguments, dict):
        for step in task_arguments.values():
            if not isinstance(step, dict):
                continue
            params = step.get("parameters", {})
            if isinstance(params, dict):
                arguments.parameters.update(params)

    top_level_params = runtime_arguments.get("parameters", {})
    if isinstance(top_level_params, dict):
        arguments.parameters.update(top_level_params)


def _resolve_input_path(value: Any, *, storage_root: str) -> str:
    if isinstance(value, dict):
        path = value.get("path")
        if path is None:
            return str(value)
        return _normalize_path(path, storage_root=storage_root)
    if isinstance(value, str):
        return _normalize_path(value, storage_root=storage_root)
    return str(value)


def _resolve_outputs(value: Any, *, storage_root: str) -> str | dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_path(value, storage_root=storage_root)
    if isinstance(value, dict):
        resolved: dict[str, str] = {}
        for name, output in value.items():
            if isinstance(output, dict):
                resolved[name] = _resolve_input_path(output, storage_root=storage_root)
            elif isinstance(output, str):
                resolved[name] = _normalize_path(output, storage_root=storage_root)
            else:
                resolved[name] = str(output)
        return resolved
    return None


def _normalize_path(path: str, *, storage_root: str) -> str:
    if not path:
        return path
    if path.startswith("/") or "://" in path:
        return path
    return os.path.join(storage_root, path)


def _outputs_need_default(outputs: str | dict[str, str]) -> bool:
    if isinstance(outputs, str):
        return outputs == "" or outputs == "<fill me>"
    return any(value in {"", "<fill me>"} for value in outputs.values())


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == "<fill me>"


def _validate_required_arguments(
    pipeline: AdagioPipeline, arguments: AdagioArguments
) -> None:
    missing_inputs = [
        input_def.name
        for input_def in pipeline.signature.inputs
        if input_def.required and _is_missing(arguments.inputs.get(input_def.name))
    ]
    missing_params = [
        param.name
        for param in pipeline.signature.parameters
        if param.required
        and param.default is None
        and _is_missing(arguments.parameters.get(param.name))
    ]

    if missing_inputs or missing_params:
        missing = [f"input:{name}" for name in missing_inputs] + [
            f"param:{name}" for name in missing_params
        ]
        raise SystemExit("Missing required runtime arguments: " + ", ".join(missing))


def _post_job_event(*, runtime_url: str, job_id: str, payload: dict[str, Any]) -> None:
    base = runtime_url.rstrip("/")
    url = f"{base}/jobs/{job_id}/events"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except (urllib.error.URLError, TimeoutError):
        return None
