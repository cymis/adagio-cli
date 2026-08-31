import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable

# Sentinel "plugin" name marking a task spec that imports raw data into a single
# artifact instead of running a QIIME plugin action. Used to materialize a
# data-import artifact that is exposed as a pipeline output (the import runs in a
# consuming action's environment, which already has the relevant QIIME type).
DATA_IMPORT_PLUGIN = "data-import"


def task_file_stem(task_id: str) -> str:
    return task_id.replace("/", "_").replace(" ", "_")


def build_task_outputs(
    *,
    task_id: str,
    output_names: Iterable[str],
    work_path: Path,
) -> dict[str, str]:
    stem = task_file_stem(task_id)
    return {
        name: str((work_path / f"{stem}_{name}").resolve())
        for name in output_names
    }


def build_task_metadata_outputs(
    *,
    task_id: str,
    output_names: Iterable[str],
    work_path: Path,
) -> dict[str, str]:
    stem = task_file_stem(task_id)
    return {
        name: str((work_path / f"{stem}_{name}_metadata.tsv").resolve())
        for name in output_names
    }


def task_spec_path(*, task_id: str, work_path: Path) -> Path:
    return (work_path / f"{task_file_stem(task_id)}_spec.json").resolve()


def result_manifest_path(*, task_id: str, work_path: Path) -> Path:
    return (work_path / f"{task_file_stem(task_id)}_results.json").resolve()


def container_log_path(*, task_id: str, work_path: Path) -> Path:
    """Per-task file capturing the container's stdout+stderr (incl. image pull)."""
    return (work_path / f"{task_file_stem(task_id)}_container.log").resolve()


def build_task_spec(
    *,
    plugin: str,
    action: str,
    archive_inputs: dict[str, str],
    archive_input_materializations: dict[str, dict[str, Any]] | None,
    archive_collection_inputs: dict[str, list[str]],
    metadata_inputs: dict[str, str],
    params: dict[str, Any],
    metadata_column_kwargs: dict[str, dict[str, str]],
    outputs: dict[str, str],
    result_manifest: str | None,
    cache_path: str | None,
    recycle_pool: str | None,
    metadata_outputs: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "plugin": plugin,
        "action": action,
        "archive_inputs": archive_inputs,
        "archive_input_materializations": archive_input_materializations or {},
        "archive_collection_inputs": archive_collection_inputs,
        "metadata_inputs": metadata_inputs,
        "params": params,
        "metadata_column_kwargs": metadata_column_kwargs,
        "outputs": outputs,
        "metadata_outputs": metadata_outputs or {},
        "result_manifest": result_manifest,
        "cache_path": cache_path,
        "recycle_pool": recycle_pool,
    }


def build_result_manifest(
    *,
    outputs: Mapping[str, str],
    reused: bool,
    metadata_outputs: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "outputs": dict(outputs),
        "metadata_outputs": dict(metadata_outputs or {}),
        "reused": reused,
    }


def parse_result_manifest(
    payload: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str], bool]:
    if "outputs" in payload:
        outputs = payload.get("outputs", {})
        metadata_outputs = payload.get("metadata_outputs", {})
        reused = bool(payload.get("reused", False))
        if not isinstance(outputs, dict):
            raise TypeError("Invalid task result manifest: 'outputs' must be an object.")
        if not isinstance(metadata_outputs, dict):
            raise TypeError(
                "Invalid task result manifest: 'metadata_outputs' must be an object."
            )
        return dict(outputs), dict(metadata_outputs), reused

    return dict(payload), {}, False


def read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
