"""Internal exec-task subcommand: runs a single QIIME action inside a plugin container."""

import argparse
from collections.abc import Mapping
from contextlib import nullcontext
import os
import sys
import warnings
import zipfile
from pathlib import Path
from typing import Any

from adagio.executors.task_contract import (
    build_result_manifest,
    read_json_file,
    write_json_file,
)


def run_task_exec(argv: list[str]) -> None:
    """Entrypoint for the internal ``adagio exec-task`` subcommand."""
    parser = argparse.ArgumentParser(
        prog="adagio exec-task",
        description="Execute a single QIIME plugin action (internal use only).",
    )
    parser.add_argument("--task", required=True, help="Path to the task spec JSON file.")
    opts = parser.parse_args(argv)

    task_spec = read_json_file(Path(opts.task))
    _run_task(task_spec)


def _run_task(spec: dict[str, Any]) -> None:
    from qiime2 import Artifact, Cache, Metadata
    from qiime2.sdk import PluginManager

    plugin_name: str = spec["plugin"]
    action_name: str = spec["action"]
    archive_inputs: dict[str, str] = spec.get("archive_inputs", {})
    metadata_inputs: dict[str, str] = spec.get("metadata_inputs", {})
    params: dict[str, Any] = spec.get("params", {})
    metadata_column_kwargs: dict[str, dict[str, str]] = spec.get("metadata_column_kwargs", {})
    outputs: dict[str, str] = spec["outputs"]
    result_manifest: str | None = spec.get("result_manifest")
    cache_path: str | None = spec.get("cache_path")
    recycle_pool: str | None = spec.get("recycle_pool")

    plugin_manager = PluginManager()

    plugin = _resolve_key(plugin_manager.plugins, plugin_name)
    if plugin is None:
        available = ", ".join(sorted(plugin_manager.plugins.keys())[:20])
        raise KeyError(
            f"QIIME plugin {plugin_name!r} not found. "
            f"Available plugins (first 20): [{available}]"
        )

    action = _resolve_key(plugin.actions, action_name)
    if action is None:
        available = ", ".join(sorted(plugin.actions.keys())[:30])
        raise KeyError(
            f"QIIME action {plugin_name!r}.{action_name!r} not found. "
            f"Available actions (first 30): [{available}]"
        )

    cache = Cache(cache_path) if cache_path else None
    cache_context = cache if cache is not None else nullcontext()
    reused = False

    with cache_context:
        kwargs: dict[str, Any] = {}

        for name, path in archive_inputs.items():
            loaded = Artifact.load(path)
            kwargs[name] = _cache_loaded_input(cache=cache, value=loaded)

        loaded_metadata: dict[str, Metadata] = {}
        for name, path in metadata_inputs.items():
            if zipfile.is_zipfile(path):
                loaded_metadata[name] = Artifact.load(path).view(Metadata)
            else:
                loaded_metadata[name] = Metadata.load(path)

        for param_name, col_spec in metadata_column_kwargs.items():
            source_name: str = col_spec["source"]
            column_name: str = col_spec["column"]
            metadata = loaded_metadata.pop(source_name)
            kwargs[param_name] = metadata.get_column(column_name)

        for name, metadata in loaded_metadata.items():
            kwargs[name] = metadata

        for name, value in params.items():
            kwargs[name] = _coerce_param(action=action, name=name, value=value)

        _materialize_default_parameters(action=action, kwargs=kwargs)

        if recycle_pool is not None and cache is None:
            raise ValueError("A recycle pool requires a configured cache path.")

        recycle_context = (
            cache.create_pool(key=recycle_pool, reuse=True)
            if recycle_pool is not None and cache is not None
            else nullcontext()
        )
        with recycle_context:
            cached_results = _load_cached_results(cache=cache, action=action, kwargs=kwargs)
            if cached_results is not None:
                reused = True
                results = cached_results
            else:
                with action_output_context():
                    results = action(**kwargs)

    saved_outputs: dict[str, str] = {}
    for name, dest_path in outputs.items():
        artifact = getattr(results, name)
        saved_outputs[name] = artifact.save(dest_path)

    if result_manifest:
        write_json_file(
            Path(result_manifest),
            build_result_manifest(outputs=saved_outputs, reused=reused),
        )


def _cache_loaded_input(*, cache: Any, value: Any) -> Any:
    if cache is None:
        return value
    return cache.process_pool.save(value)


def _materialize_default_parameters(*, action: Any, kwargs: dict[str, Any]) -> None:
    signature = getattr(action, "signature", None)
    parameters = getattr(signature, "parameters", None)
    if not isinstance(parameters, Mapping):
        return

    for name, spec in parameters.items():
        has_default = getattr(spec, "has_default", None)
        if name in kwargs or not callable(has_default) or not has_default():
            continue
        kwargs[name] = spec.default


def _load_cached_results(*, cache: Any, action: Any, kwargs: dict[str, Any]) -> Any:
    if cache is None:
        return None

    named_pool = getattr(cache, "named_pool", None)
    if named_pool is None:
        return None

    named_pool.create_index()
    invocation = _build_invocation(action=action, kwargs=kwargs)
    if invocation not in named_pool.index:
        return None

    from qiime2.core.type.util import is_collection_type
    from qiime2.sdk import ResultCollection, Results

    try:
        cached_outputs = named_pool.index[invocation]
        loaded_outputs: dict[str, Any] = {}
        for name, output_spec in action.signature.outputs.items():
            if is_collection_type(output_spec.qiime_type):
                cached_collection = cached_outputs[name]
                collection_order = list(cached_collection.keys())
                if not _validate_collection_order(collection_order):
                    return None

                collection_order.sort(key=lambda x: x.idx)
                loaded_collection = ResultCollection()
                for elem_info in collection_order:
                    loaded_collection[elem_info.item_name] = named_pool.load(
                        cached_collection[elem_info]
                    )
                loaded_outputs[name] = loaded_collection
            else:
                loaded_outputs[name] = named_pool.load(cached_outputs[name])
    except KeyError:
        return None

    return Results(loaded_outputs.keys(), loaded_outputs.values())


def _build_invocation(*, action: Any, kwargs: dict[str, Any]) -> Any:
    from rachis.core.type.signature import HashableInvocation

    plugin = action.plugin_id.replace("_", "-")
    plugin_action = f"{plugin}:{action.id}"
    collated_inputs = action.signature.collate_inputs(**kwargs)
    callable_args = action.signature.coerce_user_input(**collated_inputs)
    arguments = []
    for name, value in callable_args.items():
        arguments.append({name: value})
    return HashableInvocation(plugin_action, arguments)


def _validate_collection_order(collection_order: list[Any]) -> bool:
    if not collection_order:
        return True
    if not all(
        elem.total == collection_order[0].total for elem in collection_order
    ) or len(collection_order) != collection_order[0].total:
        warnings.warn(
            "Incomplete collection found when recycling, collection will be remade"
        )
        return False
    return True


def _resolve_key(mapping: Any, requested: str) -> Any:
    if requested in mapping:
        return mapping[requested]
    canonical = _canonical(requested)
    for key in mapping:
        if _canonical(key) == canonical:
            return mapping[key]
    return None


def _canonical(value: str) -> str:
    return value.strip().replace("-", "_").replace(" ", "_").lower()


def _coerce_param(*, action: Any, name: str, value: Any) -> Any:
    if value is None:
        return None
    from collections.abc import Mapping

    signature = getattr(action, "signature", None)
    parameters = getattr(signature, "parameters", None)
    if not isinstance(parameters, Mapping) or name not in parameters:
        return value
    qiime_type = getattr(parameters[name], "qiime_type", None)
    if qiime_type is None:
        return value
    from qiime2.sdk.util import parse_primitive

    return parse_primitive(qiime_type, value)


class action_output_context:
    """Suppress plugin stdout/stderr noise unless explicitly enabled."""

    def __enter__(self):
        mode = os.getenv("ADAGIO_ACTION_STDIO", "").strip().lower()
        self._suppress = mode not in {"inherit", "show", "verbose", "1", "true", "yes"}
        if not self._suppress:
            return self

        self._saved_fds: list[tuple[int, int]] = []
        self._sink = open(os.devnull, "w", encoding="utf-8")
        self._warnings = warnings.catch_warnings()
        self._warnings.__enter__()
        warnings.filterwarnings(
            "ignore",
            message="pkg_resources is deprecated as an API.*",
            category=UserWarning,
        )
        for fd in (1, 2):
            saved = os.dup(fd)
            self._saved_fds.append((fd, saved))
            os.dup2(self._sink.fileno(), fd)
        return self

    def __exit__(self, exc_type, exc, tb):
        if not getattr(self, "_suppress", False):
            return False
        for fd, saved in reversed(self._saved_fds):
            try:
                os.dup2(saved, fd)
            finally:
                os.close(saved)
        self._warnings.__exit__(exc_type, exc, tb)
        self._sink.close()
        return False


if __name__ == "__main__":
    run_task_exec(sys.argv[1:])
