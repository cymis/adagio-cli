"""Internal exec-task subcommand: runs a single QIIME action inside a plugin container."""

import argparse
from contextlib import nullcontext
import os
import sys
import warnings
import zipfile
from pathlib import Path
from typing import Any

from adagio.executors.task_contract import read_json_file, write_json_file


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

        if recycle_pool is not None and cache is None:
            raise ValueError("A recycle pool requires a configured cache path.")

        recycle_context = (
            cache.create_pool(key=recycle_pool, reuse=True)
            if recycle_pool is not None and cache is not None
            else nullcontext()
        )
        with recycle_context:
            with action_output_context():
                results = action(**kwargs)

    saved_outputs: dict[str, str] = {}
    for name, dest_path in outputs.items():
        artifact = getattr(results, name)
        saved_outputs[name] = artifact.save(dest_path)

    if result_manifest:
        write_json_file(Path(result_manifest), saved_outputs)


def _cache_loaded_input(*, cache: Any, value: Any) -> Any:
    if cache is None:
        return value
    return cache.process_pool.save(value)


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
