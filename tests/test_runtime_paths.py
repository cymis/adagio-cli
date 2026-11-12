"""How ``adagio runtime`` places the input, output and publish paths it is given.

A job runs in a folder of the runtime's own, on Adagio Desktop or a Runtime
Server, so ``~`` is expanded, absolute paths and URLs are kept, and a relative
path is refused before any step runs: it used to land under ``/storage``,
which neither a Mac nor a non-root Linux user can create, and the job only
failed once every step had run.
"""

import json
import os
import re
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from rich.console import Console

from adagio.cli import runtime as runtime_cli
from adagio.executors.serial_runner import SerialExecutionState
from adagio.executors.task_environments import _save_outputs
from adagio.model.pipeline import AdagioPipeline

TABLE_AST = {
    "type": "expression",
    "builtin": False,
    "name": "FeatureTable",
    "predicate": None,
    "fields": [],
}


def _table(element_id, name, **extra):
    return {
        "id": element_id,
        "name": name,
        "type": "FeatureTable[Frequency]",
        "ast": TABLE_AST,
        **extra,
    }


def _rarefy(task_id, input_id, output_id):
    return {
        "id": task_id,
        "kind": "plugin-action",
        "plugin": "feature_table",
        "action": "rarefy",
        "inputs": {"table": {"kind": "archive", "id": input_id}},
        "parameters": {"sampling_depth": {"kind": "literal", "value": 10}},
        "outputs": {"rarefied_table": {"kind": "archive", "id": output_id}},
    }


def _spec(*branches):
    """A pipeline of independent branches: (task id, input name, output name)."""
    inputs, outputs, graph = [], [], []
    for number, (task_id, input_name, output_name) in enumerate(branches):
        input_id = f"00000000-0000-0000-0000-{2 * number + 1:012d}"
        output_id = f"00000000-0000-0000-0000-{2 * number + 2:012d}"
        inputs.append(_table(input_id, input_name, required=True))
        outputs.append(_table(output_id, output_name))
        graph.append(_rarefy(task_id, input_id, output_id))
    return {
        "type": "pipeline",
        "signature": {"inputs": inputs, "parameters": [], "outputs": outputs},
        "graph": graph,
    }


SPEC = _spec(("task", "table", "rarefied"))
TWO_BRANCHES = _spec(("A", "table_a", "a"), ("B", "table_b", "b"))


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def _build(runtime_arguments, output_dir, *, spec=SPEC, spec_data=None):
    return runtime_cli._build_arguments(
        pipeline=AdagioPipeline.model_validate(spec),
        runtime_arguments=runtime_arguments,
        output_dir=str(output_dir),
        spec_data=spec_data,
    )


def _check(runtime_arguments, output_dir, *, spec=SPEC, targets=None):
    """Build the arguments and run the up-front path check on them."""
    arguments = _build(runtime_arguments, output_dir, spec=spec)
    runtime_cli._refuse_unusable_paths(
        AdagioPipeline.model_validate(spec), arguments, target_ids=targets
    )
    return arguments


def _refusal(runtime_arguments, output_dir, **options):
    with pytest.raises(SystemExit) as refused:
        _check(runtime_arguments, output_dir, **options)
    return str(refused.value)


def test_tilde_is_expanded_in_input_output_and_publish_paths(home, tmp_path):
    arguments = _check(
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
    listed = _check({"inputs": {"table": ["~/a.qza", "/b.qza"]}}, tmp_path)
    keyed = _check({"inputs": {"table": {"x": "~/x.qza", "y": "/y.qza"}}}, tmp_path)

    assert listed.inputs["table"] == [f"{home}/a.qza", "/b.qza"]
    assert keyed.inputs["table"] == {"x": f"{home}/x.qza", "y": "/y.qza"}


def test_a_directory_path_keeps_its_trailing_slash(home, tmp_path):
    # The trailing "/" is what makes an output path name a folder.
    arguments = _check(
        {"inputs": {"table": "/t.qza"}, "publish": {"rarefied": "~/review/"}},
        tmp_path,
    )

    assert arguments.publish == {"rarefied": f"{home}/review/"}


def test_absolute_paths_and_urls_are_kept_as_given(tmp_path):
    exports = tmp_path / "exports"
    arguments = _check(
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
    message = _refusal(
        {
            "inputs": {"table": "data/table.qza"},
            "outputs": {"rarefied": "job/rarefied.qza"},
            "publish": {"rarefied": "results/rarefied.qza"},
        },
        tmp_path,
    )

    assert "input:table ('data/table.qza')" in message
    assert "output:rarefied ('job/rarefied.qza')" in message
    assert "output:rarefied ('results/rarefied.qza')" in message
    assert "/storage" not in message


def test_a_relative_outputs_folder_is_refused(tmp_path):
    message = _refusal({"inputs": {"table": "/t.qza"}, "outputs": "results/"}, tmp_path)

    assert "outputs ('results/')" in message


def test_a_partial_run_checks_only_the_paths_it_uses(tmp_path):
    # Branch B has a relative input and a relative output path; running only
    # branch A uses neither, so they must not block it.
    arguments = {
        "inputs": {"table_a": "/a.qza", "table_b": "b.qza"},
        "publish": {"b": "results/b.qza"},
    }

    _check(arguments, tmp_path, spec=TWO_BRANCHES, targets={"A"})
    message = _refusal(arguments, tmp_path, spec=TWO_BRANCHES, targets={"B"})

    assert "input:table_b ('b.qza')" in message
    assert "output:b ('results/b.qza')" in message
    assert "table_a" not in message


def test_an_unset_input_is_still_reported_missing_not_relative(tmp_path):
    pipeline = AdagioPipeline.model_validate(SPEC)
    arguments = _check({"inputs": {}}, tmp_path)

    assert arguments.inputs == {"table": "<fill me>"}
    with pytest.raises(SystemExit, match="Missing required runtime arguments: input:table"):
        runtime_cli._validate_required_arguments(pipeline, arguments)


def test_an_empty_publish_path_publishes_nothing(tmp_path):
    arguments = _check(
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

    message = _refusal(
        {
            "inputs": {"table": "/t.qza"},
            "publish": {"rarefied": f"{blocker}/review/rarefied.qza"},
        },
        tmp_path,
    )

    assert "Outputs cannot be saved on this machine" in message
    assert f"cannot write to {blocker}" in message


def _saves(destination: Path, tmp_path: Path) -> bool:
    """Whether saving an output over ``destination`` works, as the save does it."""
    source = tmp_path / "produced.qza"
    source.write_text("new", encoding="utf-8")
    try:
        shutil.copy2(source, destination)
    except PermissionError:
        return False
    return True


root_can_write_anything = pytest.mark.skipif(
    os.geteuid() == 0, reason="root can write a read-only file or folder"
)


@root_can_write_anything
def test_an_output_in_a_read_only_folder_is_refused(tmp_path):
    # The original failure: /storage under a Mac's read-only "/".
    read_only = tmp_path / "read-only"
    read_only.mkdir()
    read_only.chmod(0o500)
    try:
        message = _refusal(
            {
                "inputs": {"table": "/t.qza"},
                "publish": {"rarefied": f"{read_only}/adagio/rarefied.qza"},
            },
            tmp_path,
        )
    finally:
        read_only.chmod(0o700)

    assert f"cannot write to {read_only}" in message


@root_can_write_anything
def test_an_existing_read_only_output_file_is_refused(tmp_path):
    # Its folder is writable, but the save overwrites the file in place.
    existing = tmp_path / "rarefied.qza"
    existing.write_text("old", encoding="utf-8")
    existing.chmod(0o400)

    with pytest.raises(SystemExit, match=re.escape(f"cannot write to {existing}")):
        _check(
            {"inputs": {"table": "/t.qza"}, "publish": {"rarefied": str(existing)}},
            tmp_path / "outputs",
        )
    assert not _saves(existing, tmp_path)


@root_can_write_anything
def test_an_existing_writable_output_file_in_a_read_only_folder_is_accepted(tmp_path):
    read_only = tmp_path / "read-only"
    read_only.mkdir()
    existing = read_only / "rarefied.qza"
    existing.write_text("old", encoding="utf-8")
    read_only.chmod(0o500)
    try:
        _check(
            {"inputs": {"table": "/t.qza"}, "publish": {"rarefied": str(existing)}},
            tmp_path / "outputs",
        )
        assert _saves(existing, tmp_path)
    finally:
        read_only.chmod(0o700)


output_destinations = [
    ("publish", "directory"),
    ("publish", "prefix"),
    ("outputs", "directory"),
    ("outputs", "prefix"),
    ("outputs", "all"),
]


def _configured_output(tmp_path, section, form, suffix):
    folder = tmp_path / "exports"
    folder.mkdir()
    destination = folder / f"rarefied{suffix}"
    path = str(folder) + "/" if form != "prefix" else str(folder / "rarefied")
    arguments = {
        "inputs": {"table": "/table.qza"},
        section: path if form == "all" else {"rarefied": path},
    }
    spec = _spec(("task", "table", "rarefied"))
    if suffix == ".qzv":
        spec["signature"]["outputs"][0].update(
            type="Visualization", ast={**TABLE_AST, "name": "Visualization"}
        )
    return spec, arguments, destination


def _save_produced_output(tmp_path, spec, arguments, suffix):
    source = tmp_path / f"produced{suffix}"
    source.write_text("new", encoding="utf-8")
    signature = AdagioPipeline.model_validate(spec).signature
    state = SerialExecutionState(
        cwd=tmp_path,
        work_path=tmp_path,
        params={},
        scope={signature.outputs[0].id: str(source)},
        cache_config=None,
    )
    _save_outputs(sig=signature, arguments=arguments, state=state, monitor=None)


@root_can_write_anything
@pytest.mark.parametrize(("section", "form"), output_destinations)
@pytest.mark.parametrize("suffix", [".qza", ".qzv"])
def test_a_read_only_resolved_output_is_refused_before_execution(
    tmp_path, section, form, suffix
):
    spec, arguments, destination = _configured_output(tmp_path, section, form, suffix)
    destination.write_text("old", encoding="utf-8")
    destination.chmod(0o400)
    try:
        with patch("adagio.executors.select_default_executor") as select:
            with pytest.raises(
                SystemExit, match=re.escape(f"cannot write to {destination}")
            ):
                _run(tmp_path, arguments, output_dir=str(tmp_path / "outputs"), spec=spec)
        select.assert_not_called()
        built = _build(arguments, tmp_path / "outputs", spec=spec)
        with pytest.raises(PermissionError):
            _save_produced_output(tmp_path, spec, built, suffix)
    finally:
        destination.chmod(0o600)


@root_can_write_anything
@pytest.mark.parametrize(("section", "form"), output_destinations)
@pytest.mark.parametrize("suffix", [".qza", ".qzv"])
def test_a_writable_resolved_output_in_a_read_only_folder_can_be_saved(
    tmp_path, section, form, suffix
):
    spec, arguments, destination = _configured_output(tmp_path, section, form, suffix)
    destination.write_text("old", encoding="utf-8")
    destination.parent.chmod(0o500)
    try:
        built = _check(arguments, tmp_path / "outputs", spec=spec)
        _save_produced_output(tmp_path, spec, built, suffix)
        assert destination.read_text(encoding="utf-8") == "new"
    finally:
        destination.parent.chmod(0o700)


@root_can_write_anything
def test_an_existing_prefix_file_does_not_decide_output_permissions(tmp_path):
    spec, arguments, destination = _configured_output(tmp_path, "publish", "prefix", ".qza")
    prefix = destination.with_suffix("")
    prefix.write_text("leave alone", encoding="utf-8")
    prefix.chmod(0o400)
    try:
        built = _check(arguments, tmp_path / "outputs", spec=spec)
        _save_produced_output(tmp_path, spec, built, ".qza")
        assert destination.read_text(encoding="utf-8") == "new"
        assert prefix.read_text(encoding="utf-8") == "leave alone"
    finally:
        prefix.chmod(0o600)


class _Executor:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)


def _run(tmp_path, arguments_payload, *, output_dir, spec=SPEC, extra_argv=()):
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(spec), encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text("version = 1\n", encoding="utf-8")
    arguments = tmp_path / "arguments.json"
    arguments.write_text(json.dumps(arguments_payload), encoding="utf-8")
    argv = [
        "--spec",
        str(spec_file),
        "--config",
        str(config),
        "--arguments",
        str(arguments),
        "--cache-dir",
        str(tmp_path / "cache"),
        *extra_argv,
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


def test_a_selected_branch_runs_despite_a_relative_path_elsewhere(tmp_path):
    # The editor's "Run selected" sends every output path, used or not.
    executor = _Executor()
    with patch("adagio.executors.select_default_executor", return_value=executor):
        _run(
            tmp_path,
            {
                "inputs": {"table_a": "/a.qza", "table_b": "/b.qza"},
                "publish": {"b": "results/b.qza"},
            },
            output_dir=str(tmp_path / "outputs"),
            spec=TWO_BRANCHES,
            extra_argv=["--targets", "A"],
        )

    assert executor.calls[0]["target_ids"] == {"A"}


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
