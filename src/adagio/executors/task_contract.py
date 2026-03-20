import json
from pathlib import Path
from typing import Any, Iterable


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


def task_spec_path(*, task_id: str, work_path: Path) -> Path:
    return (work_path / f"{task_file_stem(task_id)}_spec.json").resolve()


def result_manifest_path(*, task_id: str, work_path: Path) -> Path:
    return (work_path / f"{task_file_stem(task_id)}_results.json").resolve()


def build_task_spec(
    *,
    plugin: str,
    action: str,
    archive_inputs: dict[str, str],
    metadata_inputs: dict[str, str],
    params: dict[str, Any],
    metadata_column_kwargs: dict[str, dict[str, str]],
    outputs: dict[str, str],
    result_manifest: str | None,
) -> dict[str, Any]:
    return {
        "plugin": plugin,
        "action": action,
        "archive_inputs": archive_inputs,
        "metadata_inputs": metadata_inputs,
        "params": params,
        "metadata_column_kwargs": metadata_column_kwargs,
        "outputs": outputs,
        "result_manifest": result_manifest,
    }


def read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
