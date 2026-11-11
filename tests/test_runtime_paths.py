"""How ``adagio runtime`` places the input, output and publish paths it is given.

A job runs in a folder of the runtime's own, on Adagio Desktop or a Runtime
Server, so ``~`` is expanded, absolute paths and URLs are kept, and a relative
path is refused before any step runs: it used to land under ``/storage``,
which neither a Mac nor a non-root Linux user can create, and the job only
failed once every step had run.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from rich.console import Console

from adagio.cli import runtime as runtime_cli
from adagio.model.pipeline import AdagioPipeline

TABLE_AST = {
    "type": "expression",
    "builtin": False,
    "name": "FeatureTable",
    "predicate": None,
    "fields": [],
}
INPUT_ID = "00000000-0000-0000-0000-000000000001"
OUTPUT_ID = "00000000-0000-0000-0000-000000000002"

SPEC = {
    "type": "pipeline",
    "signature": {
        "inputs": [
            {
                "id": INPUT_ID,
                "name": "table",
                "type": "FeatureTable[Frequency]",
                "ast": TABLE_AST,
                "required": True,
            }
        ],
        "parameters": [],
        "outputs": [
            {
                "id": OUTPUT_ID,
                "name": "rarefied",
                "type": "FeatureTable[Frequency]",
                "ast": TABLE_AST,
            }
        ],
    },
    "graph": [
        {
            "id": "task",
            "kind": "plugin-action",
            "plugin": "feature_table",
            "action": "rarefy",
            "inputs": {"table": {"kind": "archive", "id": INPUT_ID}},
            "parameters": {"sampling_depth": {"kind": "literal", "value": 10}},
            "outputs": {"rarefied_table": {"kind": "archive", "id": OUTPUT_ID}},
        }
    ],
}


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def _build(runtime_arguments, output_dir, *, spec_data=None):
    return runtime_cli._build_arguments(
        pipeline=AdagioPipeline.model_validate(SPEC),
        runtime_arguments=runtime_arguments,
        output_dir=str(output_dir),
        spec_data=spec_data,
    )


def test_tilde_is_expanded_in_input_output_and_publish_paths(home, tmp_path):
    arguments = _build(
        {
            "inputs": {"table": "~/data/table.qza"},
            "outputs": {"rarefied": "~/job/rarefied.qza"},
            "publish": {"rarefied": "~/Desktop/adagio-review/error-correction.qza"},
        },
        tmp_path / "outputs",
    )

    assert arguments.inputs == {"table": f"{home}/data/table.qza"}
    assert arguments.outputs == {"rarefied": f"{home}/job/rarefied.qza"}
    assert arguments.publish == {
        "rarefied": f"{home}/Desktop/adagio-review/error-correction.qza"
    }


def test_tilde_is_expanded_in_every_path_of_a_collection_input(home, tmp_path):
    listed = _build({"inputs": {"table": ["~/a.qza", "/b.qza"]}}, tmp_path)
    keyed = _build({"inputs": {"table": {"x": "~/x.qza", "y": "/y.qza"}}}, tmp_path)

    assert listed.inputs["table"] == [f"{home}/a.qza", "/b.qza"]
    assert keyed.inputs["table"] == {"x": f"{home}/x.qza", "y": "/y.qza"}


def test_a_directory_path_keeps_its_trailing_slash(home, tmp_path):
    # The trailing "/" is what makes an output path name a folder.
    arguments = _build(
        {"inputs": {"table": "/t.qza"}, "publish": {"rarefied": "~/review/"}},
        tmp_path,
    )

    assert arguments.publish == {"rarefied": f"{home}/review/"}


def test_absolute_paths_and_urls_are_kept_as_given(tmp_path):
    exports = tmp_path / "exports"
    arguments = _build(
        {
            "inputs": {"table": "https://example.org/table.qza"},
            "outputs": {"rarefied": str(tmp_path / "job" / "rarefied.qza")},
            "publish": {"rarefied": str(exports / "rarefied.qza")},
        },
        tmp_path / "outputs",
    )

    assert arguments.inputs == {"table": "https://example.org/table.qza"}
    assert arguments.outputs == {"rarefied": str(tmp_path / "job" / "rarefied.qza")}
    assert arguments.publish == {"rarefied": str(exports / "rarefied.qza")}
    # Checking that a folder can be written creates nothing.
    assert not exports.exists()


def test_relative_paths_are_refused_naming_each_one(tmp_path):
    with pytest.raises(SystemExit) as refused:
        _build(
            {
                "inputs": {"table": "data/table.qza"},
                "outputs": {"rarefied": "job/rarefied.qza"},
                "publish": {"rarefied": "results/rarefied.qza"},
            },
            tmp_path,
        )

    message = str(refused.value)
    assert "input:table ('data/table.qza')" in message
    assert "output:rarefied ('job/rarefied.qza')" in message
    assert "output:rarefied ('results/rarefied.qza')" in message
    assert "/storage" not in message


def test_a_relative_outputs_folder_is_refused(tmp_path):
    with pytest.raises(SystemExit, match=r"outputs \('results/'\)"):
        _build({"inputs": {"table": "/t.qza"}, "outputs": "results/"}, tmp_path)


def test_an_unset_input_is_still_reported_missing_not_relative(tmp_path):
    pipeline = AdagioPipeline.model_validate(SPEC)
    arguments = _build({"inputs": {}}, tmp_path)

    assert arguments.inputs == {"table": "<fill me>"}
    with pytest.raises(SystemExit, match="Missing required runtime arguments: input:table"):
        runtime_cli._validate_required_arguments(pipeline, arguments)


def test_an_empty_publish_path_publishes_nothing(tmp_path):
    arguments = _build(
        {"inputs": {"table": "/t.qza"}, "publish": {"rarefied": ""}}, tmp_path
    )

    assert arguments.publish == {}
    assert arguments.outputs == str(tmp_path)


def test_output_paths_bundled_in_the_pipeline_file_are_expanded(home, tmp_path):
    envelope = {
        "name": "p",
        "spec": SPEC,
        "configs": [{"outputs": {"rarefied": "~/bundled/rarefied.qza"}}],
    }

    arguments = _build({"inputs": {"table": "/t.qza"}}, tmp_path, spec_data=envelope)

    assert arguments.outputs == {"rarefied": f"{home}/bundled/rarefied.qza"}


def test_an_output_under_a_file_is_refused(tmp_path):
    blocker = tmp_path / "not-a-folder"
    blocker.write_text("x", encoding="utf-8")

    with pytest.raises(SystemExit) as refused:
        _build(
            {
                "inputs": {"table": "/t.qza"},
                "publish": {"rarefied": f"{blocker}/review/rarefied.qza"},
            },
            tmp_path,
        )

    assert "Outputs cannot be saved on this machine" in str(refused.value)
    assert f"cannot write in {blocker}" in str(refused.value)


@pytest.mark.skipif(os.geteuid() == 0, reason="root can write a read-only folder")
def test_an_output_in_a_read_only_folder_is_refused(tmp_path):
    # The original failure: /storage under a Mac's read-only "/".
    read_only = tmp_path / "read-only"
    read_only.mkdir()
    read_only.chmod(0o500)
    try:
        with pytest.raises(SystemExit, match=f"cannot write in {read_only}"):
            _build(
                {
                    "inputs": {"table": "/t.qza"},
                    "publish": {"rarefied": f"{read_only}/adagio/rarefied.qza"},
                },
                tmp_path,
            )
    finally:
        read_only.chmod(0o700)


class _Executor:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)


def _run(tmp_path, arguments_payload, *, output_dir):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps(SPEC), encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text("version = 1\n", encoding="utf-8")
    arguments = tmp_path / "arguments.json"
    arguments.write_text(json.dumps(arguments_payload), encoding="utf-8")
    argv = [
        "--spec",
        str(spec),
        "--config",
        str(config),
        "--arguments",
        str(arguments),
        "--cache-dir",
        str(tmp_path / "cache"),
    ]
    if output_dir is not None:
        argv += ["--output-dir", output_dir]
    runtime_cli.run_runtime(argv, console=Console())


def test_a_relative_publish_path_stops_the_job_before_any_step_runs(tmp_path):
    with patch("adagio.executors.select_default_executor") as select:
        with pytest.raises(SystemExit, match="output:rarefied"):
            _run(
                tmp_path,
                {
                    "inputs": {"table": "/t.qza"},
                    "publish": {"rarefied": "adagio-review/rarefied.qza"},
                },
                output_dir=str(tmp_path / "outputs"),
            )

    select.assert_not_called()


def test_tilde_in_the_output_dir_is_expanded(home, tmp_path):
    executor = _Executor()
    with patch("adagio.executors.select_default_executor", return_value=executor):
        _run(tmp_path, {"inputs": {"table": "/t.qza"}}, output_dir="~/adagio-outputs")

    assert executor.calls[0]["arguments"].outputs == f"{home}/adagio-outputs"
    assert Path(home, "adagio-outputs").is_dir()


def test_the_output_dir_is_required(tmp_path):
    with pytest.raises(SystemExit) as refused:
        _run(tmp_path, {"inputs": {"table": "/t.qza"}}, output_dir=None)

    assert refused.value.code == 2
