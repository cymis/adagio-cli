import argparse
import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from rich.console import Console

from ..executors.cache_support import (
    CACHE_DIR_HELP,
    RECYCLE_POOL_HELP,
    REUSE_HELP,
    resolve_cache_config,
)
from ..executors.path_utils import is_directory_path, resolve_output_destination
from ..bundled_config import apply_bundled_config, bundled_config
from ..model.arguments import AdagioArguments
from ..model.pipeline import AdagioPipeline
from ..bundled_config import unset_values
from ..monitor.composite import CompositeMonitor
from ..monitor.connected import ConnectedMonitor
from ..monitor.log import LogMonitor
from .config import (
    default_environment_override,
    load_run_config,
    named_environment_overrides,
)


def run_runtime(argv: list[str], *, console: Console) -> None:
    """Runtime entrypoint used by the runtime-adapter job container."""
    parser = argparse.ArgumentParser(
        prog="adagio runtime",
        description=(
            "Execute a pipeline from spec/config/arguments files. "
            "The config file may define environment overrides and per-task resource requests."
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
        "--output-dir", required=True, help="Directory for output artifacts."
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
        "--recycle-pool",
        required=False,
        default=None,
        help=RECYCLE_POOL_HELP,
    )
    parser.add_argument(
        "--no-reuse-nodes",
        required=False,
        default=None,
        help=(
            "Comma-separated node ids that must execute fresh. Other nodes keep "
            "ordinary cache eligibility unless --no-reuse disables it run-wide."
        ),
    )
    parser.add_argument(
        "--log-dir",
        required=False,
        default=None,
        help=(
            "Directory to copy each task's container log into before the temp "
            "work dir is torn down. Enables live tail + past-run log fetch."
        ),
    )
    parser.add_argument(
        "--targets",
        required=False,
        default=None,
        help=(
            "Comma-separated node ids to produce. The plan is pruned to the "
            "upstream closure needed for these nodes. Defaults to all nodes."
        ),
    )
    parser.add_argument(
        "--connected",
        action="store_true",
        help="Emit execution status updates to the runtime-adapter.",
    )

    opts = parser.parse_args(argv)

    spec_data = _load_json(Path(opts.spec))
    # A pipeline file saved before its values were set never runs, here or
    # through `adagio run`: a step would fail after the ones before it ran.
    missing = unset_values(spec_data)
    if missing:
        raise SystemExit(
            "This pipeline is incomplete. Set these values in Adagio first: " + "; ".join(missing)
        )
    run_config = load_run_config(Path(opts.config))
    runtime_arguments: Any = {}
    if opts.arguments:
        runtime_arguments = _load_json(Path(opts.arguments))
    if runtime_arguments is None:
        runtime_arguments = {}

    pipeline = _parse_pipeline(spec_data)
    output_dir = _resolve_output_dir(opts.output_dir)
    arguments = _build_arguments(
        pipeline=pipeline,
        runtime_arguments=runtime_arguments,
        output_dir=output_dir,
        spec_data=spec_data,
    )
    target_ids = _parse_targets(opts.targets)
    _validate_required_arguments(pipeline, arguments, target_ids=target_ids)
    _refuse_unusable_paths(pipeline, arguments, target_ids=target_ids)
    cache_config = resolve_cache_config(
        cwd=Path.cwd().resolve(),
        cache_dir=opts.cache_dir,
        reuse=opts.reuse,
        recycle_pool=opts.recycle_pool,
        no_reuse_nodes=_parse_node_ids(opts.no_reuse_nodes),
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

    # Run-level reproducibility header (design §5.1). adagio_version is cheap;
    # qiime/plugin versions are intentionally left empty (never import qiime2 in
    # the host process) and image digests are filled per-task on node events.
    reproducibility = _build_reproducibility()

    if connected and runtime_url and opts.job_id:
        # The adapter captures the reproducibility header from ``job_status``
        # events (its relay ignores unknown event types), so it rides on the
        # initial "running" post rather than only on the monitor event below.
        _post_job_event(
            runtime_url=runtime_url,
            job_id=opts.job_id,
            payload={
                "event": "job_status",
                "status": "running",
                "reproducibility": reproducibility,
            },
        )

    monitor.report_reproducibility(reproducibility=reproducibility)

    from ..executors import select_default_executor

    executor = select_default_executor(
        default_override=default_environment_override(run_config),
        plugin_overrides=named_environment_overrides(
            run_config.plugins if run_config is not None else {}
        ),
        task_overrides=named_environment_overrides(
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
            target_ids=target_ids,
            log_dir=opts.log_dir,
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


def _parse_node_ids(raw: str | None) -> set[str]:
    """Parse a comma-separated node-id argument."""
    if not raw:
        return set()
    return {part.strip() for part in raw.split(",") if part.strip()}


def _parse_targets(raw: str | None) -> set[str] | None:
    """Parse ``--targets id[,id...]`` into a set of node ids (None => all)."""
    ids = _parse_node_ids(raw)
    return ids or None


def _build_reproducibility() -> dict[str, Any]:
    """Assemble the run-level reproducibility header (design §5.1).

    ``adagio_version`` comes from installed metadata. QIIME / plugin versions are
    deliberately omitted here — importing ``qiime2`` in the host process is
    forbidden; those live inside the containers. Image digests are resolved
    per-task and surfaced on node events, so ``image_digests`` starts empty.
    """
    from .. import __version__

    return {
        "adagio_version": __version__,
        "qiime_version": None,
        "plugin_versions": {},
        "image_digests": {},
    }


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_pipeline(data: Any) -> AdagioPipeline:
    pipeline_data = data.get("spec", data) if isinstance(data, dict) else data
    return AdagioPipeline.model_validate(pipeline_data)


def _resolve_output_dir(raw_output_dir: str) -> str:
    output_dir = os.path.abspath(os.path.expanduser(raw_output_dir))
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _build_arguments(
    *,
    pipeline: AdagioPipeline,
    runtime_arguments: Any,
    output_dir: str,
    spec_data: Any = None,
) -> AdagioArguments:
    arguments = pipeline.signature.to_default_arguments()

    # A spec file that is a full .adg envelope brings its bundled config as
    # the lowest layer; the job's arguments override it below.
    bundled = bundled_config(spec_data)
    if bundled is not None:
        input_names, parameter_names, output_names = pipeline.signature.names()
        apply_bundled_config(
            arguments,
            bundled,
            input_names=input_names,
            parameter_names=parameter_names,
            output_names=output_names,
        )

    if isinstance(runtime_arguments, dict):
        if isinstance(runtime_arguments.get("inputs"), dict):
            _apply_named_arguments(
                arguments=arguments, runtime_arguments=runtime_arguments
            )
        else:
            _apply_legacy_arguments(
                pipeline=pipeline,
                arguments=arguments,
                runtime_arguments=runtime_arguments,
            )

        resolved_outputs = _resolve_outputs(runtime_arguments.get("outputs"))
        if resolved_outputs is not None:
            arguments.outputs = resolved_outputs

        resolved_publish = _resolve_outputs(runtime_arguments.get("publish"))
        if isinstance(resolved_publish, dict):
            output_names = {output.name for output in pipeline.signature.outputs}
            unknown = sorted(set(resolved_publish) - output_names)
            if unknown:
                raise SystemExit(
                    "Unknown publish outputs in arguments file: " + ", ".join(unknown)
                )
            arguments.publish = resolved_publish

    if _outputs_need_default(arguments.outputs):
        arguments.outputs = output_dir

    _expand_paths(arguments)
    return arguments


def _apply_named_arguments(
    *, arguments: AdagioArguments, runtime_arguments: dict[str, Any]
) -> None:
    raw_inputs = runtime_arguments.get("inputs", {})
    if isinstance(raw_inputs, dict):
        for name, value in raw_inputs.items():
            arguments.inputs[name] = _resolve_input_value(value)

    raw_parameters = runtime_arguments.get("parameters", {})
    if isinstance(raw_parameters, dict):
        arguments.parameters.update(raw_parameters)


def _apply_legacy_arguments(
    *,
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    runtime_arguments: dict[str, Any],
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
        arguments.inputs[input_def.name] = _resolve_input_path(token)

    named_inputs = runtime_arguments.get("inputs", {})
    if isinstance(named_inputs, dict):
        for name, value in named_inputs.items():
            arguments.inputs[name] = _resolve_input_value(value)

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


def _resolve_input_path(value: Any) -> str:
    if isinstance(value, dict):
        path = value.get("path")
        return str(value) if path is None else path
    return str(value)


def _resolve_input_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_resolve_input_value(item) for item in value]
    if isinstance(value, dict):
        path = value.get("path")
        if path is not None:
            return path
        return {str(key): _resolve_input_value(item) for key, item in value.items()}
    return str(value)


def _resolve_outputs(value: Any) -> str | dict[str, str] | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {name: _resolve_input_path(output) for name, output in value.items()}
    return None


def _expand_paths(arguments: AdagioArguments) -> None:
    """Expand ``~`` in every input, output and publish path.

    ``~`` is the home folder of whoever runs the job: the user on Adagio
    Desktop, the server account on a Runtime Server. The editor's output paths
    arrive as publish destinations; an empty one means none was given, and the
    output stays in the job's output folder.
    """
    arguments.inputs = {
        name: _map_paths(value, os.path.expanduser)
        for name, value in arguments.inputs.items()
    }
    if isinstance(arguments.outputs, str):
        arguments.outputs = os.path.expanduser(arguments.outputs)
    else:
        arguments.outputs = {
            name: os.path.expanduser(path) for name, path in arguments.outputs.items()
        }
    arguments.publish = {
        name: os.path.expanduser(path)
        for name, path in arguments.publish.items()
        if path not in _PLACEHOLDERS
    }


def _refuse_unusable_paths(
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    target_ids: set[str] | None = None,
) -> None:
    """Refuse relative paths and outputs that cannot be saved, before any step.

    The runtime runs each job in a folder of its own (under Adagio Desktop's or
    a Runtime Server's home), which is not a folder whoever typed the path had
    in mind, so a relative path has nothing sensible to be relative to. Saving
    happens once every step has run, so an output that cannot be written would
    otherwise lose all of that work.

    Only the paths this run uses are checked: for a partial run, the inputs its
    steps read and the outputs they produce, so a path on an unrelated branch
    never blocks it.
    """
    sig = pipeline.signature
    input_ids, _, output_ids = _closure_argument_ids(pipeline, target_ids)
    unused = {
        f"input:{item.name}"
        for item in sig.inputs
        if input_ids is not None and item.id not in input_ids
    } | {
        f"output:{item.name}"
        for item in sig.outputs
        if output_ids is not None and item.id not in output_ids
    }

    def checked(entries: list[tuple[str, str]]) -> list[tuple[str, str]]:
        # A URL is read or written by whatever serves it, not by this machine.
        return [
            (label, path)
            for label, path in entries
            if label not in unused and "://" not in path
        ]

    sources = checked(
        [
            (f"input:{name}", path)
            for name, value in arguments.inputs.items()
            for path in _paths_in(value)
            if path not in _PLACEHOLDERS
        ]
    )
    if isinstance(arguments.outputs, str):
        destinations = [("outputs", os.path.join(arguments.outputs, ""))]
    else:
        destinations = [
            (f"output:{name}", path) for name, path in arguments.outputs.items()
        ]
    destinations = checked(
        destinations
        + [(f"output:{name}", path) for name, path in arguments.publish.items()]
    )

    relative = [
        f"{label} ({path!r})"
        for label, path in sources + destinations
        if not os.path.isabs(path)
    ]
    if relative:
        raise SystemExit(
            "Relative paths cannot be resolved: "
            + ", ".join(dict.fromkeys(relative))
            + ". Start each path with / or ~/; a run works in a folder of its "
            "own, so a relative path has nothing to be relative to."
        )

    blocked = []
    output_names = [output.name for output in sig.outputs]
    for output in sig.outputs:
        label = f"output:{output.name}"
        if label in unused:
            continue
        # QIIME saves visualizations as .qzv and artifacts as .qza. Resolve
        # the same filename the save will use before checking permissions.
        suffix = ".qzv" if output.type == "Visualization" else ".qza"
        for paths in (arguments.outputs, arguments.publish):
            if isinstance(paths, dict) and output.name not in paths:
                continue
            path = resolve_output_destination(
                output_name=output.name,
                output_names=output_names,
                outputs=paths,
                suffix=suffix,
            )
            if "://" in path:
                continue
            blocker = _save_blocker(path)
            if blocker is not None:
                blocked.append(f"{label} ({path!r}: cannot write to {blocker})")
    if blocked:
        raise SystemExit(
            "Outputs cannot be saved on this machine: "
            + ", ".join(dict.fromkeys(blocked))
            + ". Choose a folder this machine can write to."
        )


def _save_blocker(path: str) -> str | None:
    """What would stop an output being saved at absolute ``path``, if anything.

    Saving copies over an existing file in place, so that file must itself be
    writable, whatever its folder allows. Anything new is made in the nearest
    folder that already exists, which must be a writable folder (a file there,
    ``/`` on a Mac, a Mac home folder on a Linux Runtime Server all fail).
    Nothing is created here.
    """
    if not is_directory_path(path) and os.path.isfile(path):
        return None if os.access(path, os.W_OK) else path
    existing = path if is_directory_path(path) else os.path.dirname(path)
    while not os.path.exists(existing):
        existing = os.path.dirname(existing)
    if os.path.isdir(existing) and os.access(existing, os.W_OK | os.X_OK):
        return None
    return existing


def _map_paths(value: Any, place: Callable[[str], str]) -> Any:
    """Apply ``place`` to each path in an input value (a path, list or map)."""
    if isinstance(value, list):
        return [_map_paths(item, place) for item in value]
    if isinstance(value, dict):
        return {key: _map_paths(item, place) for key, item in value.items()}
    if isinstance(value, str):
        return place(value)
    return value


def _paths_in(value: Any) -> Iterator[str]:
    """Each path in an input value (a path, list or map)."""
    if isinstance(value, list):
        for item in value:
            yield from _paths_in(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _paths_in(item)
    elif isinstance(value, str):
        yield value


_PLACEHOLDERS = {"", "<fill me>"}


def _outputs_need_default(outputs: str | dict[str, str]) -> bool:
    if isinstance(outputs, str):
        return outputs in _PLACEHOLDERS
    # An empty map means no destinations were provided (the run UI sends
    # ``outputs: {}``) → fall back to --output-dir, which yields one file per
    # pipeline output. A non-empty map still defaults only if it has holes.
    return not outputs or any(value in _PLACEHOLDERS for value in outputs.values())


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == "<fill me>" or value == [] or value == {}


def _validate_required_arguments(
    pipeline: AdagioPipeline,
    arguments: AdagioArguments,
    target_ids: set[str] | None = None,
) -> None:
    sig = pipeline.signature

    # For a partial run, only require the inputs/params the target closure
    # actually consumes — a downstream target must not demand root inputs that
    # feed unrelated branches (that SystemExit'd valid partial runs before any
    # node reported, leaving the editor stuck on yellow). None => require all.
    needed_input_ids, needed_param_ids, _ = _closure_argument_ids(pipeline, target_ids)

    missing_inputs = [
        input_def.name
        for input_def in sig.inputs
        if input_def.required
        and (needed_input_ids is None or input_def.id in needed_input_ids)
        and _is_missing(arguments.inputs.get(input_def.name))
    ]
    missing_params = [
        param.name
        for param in sig.parameters
        if param.required
        and param.default is None
        and (needed_param_ids is None or param.id in needed_param_ids)
        and _is_missing(arguments.parameters.get(param.name))
    ]

    if missing_inputs or missing_params:
        missing = [f"input:{name}" for name in missing_inputs] + [
            f"param:{name}" for name in missing_params
        ]
        raise SystemExit("Missing required runtime arguments: " + ", ".join(missing))


def _closure_argument_ids(
    pipeline: AdagioPipeline, target_ids: set[str] | None
) -> tuple[set[str] | None, set[str] | None, set[str] | None]:
    """Pipeline input, param and output ids the ``target_ids`` closure uses.

    Returns ``(None, None, None)`` for a full run (everything). For a partial
    run, walks the same target closure the executor runs (``prune_to_targets``)
    and collects the signature input ids (a RootInputTask's input src.id equals
    the pipeline input id) and promoted param ids those tasks reference, and
    the ids of the outputs they produce (the only ones the run can save).
    """
    if not target_ids:
        return None, None, None

    from ..executors.common import prune_to_targets
    from ..model.task import input_source_ids

    closure = prune_to_targets(
        execution_plan=list(pipeline.iter_tasks()), target_ids=target_ids
    )
    input_ids: set[str] = set()
    param_ids: set[str] = set()
    output_ids: set[str] = set()
    for task in closure:
        output_ids.update(output.id for output in task.outputs.values())
        for src in task.inputs.values():
            for source_id in input_source_ids(src):
                input_ids.add(source_id)
        for param in task.parameters.values():
            kind = getattr(param, "kind", None)
            if kind == "promoted":
                param_ids.add(param.id)
            elif kind == "metadata":
                column = getattr(param, "column", None)
                if getattr(column, "kind", None) == "promoted":
                    param_ids.add(column.id)
    return input_ids, param_ids, output_ids


def _post_job_event(*, runtime_url: str, job_id: str, payload: dict[str, Any]) -> None:
    base = runtime_url.rstrip("/")
    url = f"{base}/jobs/{job_id}/events"
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    # Connected posts gain optional Authorization from RUNTIME_TOKEN (design
    # §5.2), matching ConnectedMonitor's transport.
    token = os.getenv("RUNTIME_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except (urllib.error.URLError, TimeoutError):
        return None
