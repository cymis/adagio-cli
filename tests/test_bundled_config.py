"""The configuration bundled in a pipeline file is the lowest argument layer."""

import json
from types import SimpleNamespace

import pytest

from adagio.bundled_config import apply_bundled_config, bundled_config
from adagio.model.arguments import AdagioArguments

INT_AST = {
    "type": "expression",
    "builtin": True,
    "name": "Int",
    "predicate": None,
    "fields": [],
}
TABLE_AST = {
    "type": "expression",
    "builtin": False,
    "name": "FeatureTable",
    "predicate": None,
    "fields": [],
}


def _spec(*, legacy_default=None):
    parameter = {
        "id": "00000000-0000-0000-0000-000000000002",
        "name": "depth",
        "required": True,
        "type": "Int",
        "ast": INT_AST,
    }
    if legacy_default is not None:
        parameter["required"] = False
        parameter["default"] = legacy_default
    return {
        "type": "pipeline",
        "signature": {
            "inputs": [
                {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": "table",
                    "type": "FeatureTable[Frequency]",
                    "ast": TABLE_AST,
                    "required": True,
                }
            ],
            "parameters": [parameter],
            "outputs": [],
        },
        "graph": [
            {
                "id": "task",
                "kind": "plugin-action",
                "plugin": "feature_table",
                "action": "rarefy",
                "inputs": {
                    "table": {
                        "kind": "archive",
                        "id": "00000000-0000-0000-0000-000000000001",
                    }
                },
                "outputs": {},
                "parameters": {
                    "sampling_depth": {
                        "kind": "promoted",
                        "id": "00000000-0000-0000-0000-000000000002",
                    }
                },
            }
        ],
    }


def _envelope(configs, spec=None):
    return {
        "name": "p",
        "spec": spec or _spec(),
        "layout": {},
        "configs": configs,
        "version": 1,
    }


class TestBundledConfig:
    def test_reads_the_first_bundled_config(self):
        data = _envelope(
            [
                {
                    "name": "a",
                    "inputs": {"table": "/a.qza"},
                    "parameters": {"depth": 1},
                },
                {"name": "b", "parameters": {"depth": 2}},
            ]
        )
        assert bundled_config(data) == {
            "inputs": {"table": "/a.qza"},
            "parameters": {"depth": 1},
            "outputs": {},
        }

    def test_lifts_legacy_defaults_from_a_file_without_configs(self):
        spec = _spec(legacy_default=120)
        spec["meta"] = {
            "editor_variables": [
                {"id": "x", "name": "unused", "kind": "Int", "value": 7},
                {"id": "y", "name": "reads", "kind": "Path or URL", "value": "/x"},
            ]
        }
        assert bundled_config({"spec": spec}) == {
            "inputs": {},
            "parameters": {"depth": 120, "unused": 7},
            "outputs": {},
        }
        # A bare spec reads the same way as an envelope.
        assert bundled_config(spec) == bundled_config({"spec": spec})

    def test_no_values_is_no_config(self):
        assert bundled_config(_envelope([])) is None
        assert bundled_config(_spec()) is None
        assert bundled_config("not a pipeline") is None

    def test_apply_keeps_only_names_the_signature_has(self):
        arguments = AdagioArguments(
            inputs={"table": "<fill me>"}, parameters={"depth": "<fill me>"}, outputs={}
        )
        apply_bundled_config(
            arguments,
            {
                "inputs": {"table": "/t.qza", "other": "/o.qza"},
                "parameters": {"depth": 5, "unused_variable": 9, "blank": ""},
                "outputs": {"table_out": "/results", "nope": "/x"},
            },
            input_names={"table"},
            parameter_names={"depth"},
            output_names={"table_out"},
        )
        assert arguments.inputs == {"table": "/t.qza"}
        assert arguments.parameters == {"depth": 5}
        assert arguments.outputs == {"table_out": "/results"}


def _capture_run(monkeypatch):
    requests = []
    from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult
    from adagio.executors.task_environments import TaskEnvironmentExecutor

    def launch(*, environment, request, console=None, **kwargs):
        requests.append(request)
        return TaskExecutionResult(outputs={})

    executor = TaskEnvironmentExecutor(
        environment_resolver=SimpleNamespace(
            resolve=lambda **kwargs: TaskEnvironmentSpec(kind="test", reference="test")
        ),
        launchers={"test": SimpleNamespace(launch=launch)},
    )
    monkeypatch.setattr(
        "adagio.executors.select_default_executor", lambda **kw: executor
    )
    return requests


def _run(argv):
    from adagio.cli.main import main

    with pytest.raises(SystemExit) as exit_status:
        main(argv)
    return exit_status.value.code


class TestRunLayers:
    def test_bundled_config_supplies_the_values_a_run_needs(
        self, monkeypatch, tmp_path
    ):
        requests = _capture_run(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(
            json.dumps(
                _envelope(
                    [
                        {
                            "inputs": {"table": "/shared/table.qza"},
                            "parameters": {"depth": 5},
                        }
                    ]
                )
            )
        )
        monkeypatch.chdir(tmp_path)

        assert _run(["run", str(path), "--cache-dir", str(tmp_path / "cache")]) == 0
        assert requests[0].archive_inputs == {"table": "/shared/table.qza"}
        assert requests[0].params == {"sampling_depth": 5}

    def test_arguments_file_overrides_bundled_and_flags_override_both(
        self, monkeypatch, tmp_path
    ):
        requests = _capture_run(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(
            json.dumps(
                _envelope(
                    [{"inputs": {"table": "/bundled.qza"}, "parameters": {"depth": 1}}]
                )
            )
        )
        arguments = tmp_path / "args.json"
        arguments.write_text(
            json.dumps({"inputs": {"table": "/file.qza"}, "parameters": {}})
        )
        monkeypatch.chdir(tmp_path)

        assert (
            _run(
                [
                    "run",
                    str(path),
                    "--cache-dir",
                    str(tmp_path / "cache"),
                    "--arguments",
                    str(arguments),
                    "--param-depth",
                    "3",
                ]
            )
            == 0
        )
        assert requests[0].archive_inputs == {"table": "/file.qza"}
        assert requests[0].params == {"sampling_depth": 3}

    def test_no_bundled_config_makes_the_variables_required_again(
        self, monkeypatch, tmp_path
    ):
        _capture_run(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(
            json.dumps(
                _envelope(
                    [
                        {
                            "inputs": {"table": "/shared/table.qza"},
                            "parameters": {"depth": 5},
                        }
                    ]
                )
            )
        )
        monkeypatch.chdir(tmp_path)

        assert (
            _run(
                [
                    "run",
                    str(path),
                    "--cache-dir",
                    str(tmp_path / "cache"),
                    "--no-bundled-config",
                ]
            )
            == 1
        )

    def test_a_bundled_value_for_an_unused_variable_is_not_an_error(
        self, monkeypatch, tmp_path
    ):
        requests = _capture_run(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(
            json.dumps(
                _envelope(
                    [
                        {
                            "inputs": {"table": "/shared/table.qza"},
                            "parameters": {"depth": 5, "unused_variable": 9},
                        }
                    ]
                )
            )
        )
        monkeypatch.chdir(tmp_path)

        assert _run(["run", str(path), "--cache-dir", str(tmp_path / "cache")]) == 0
        assert requests[0].params == {"sampling_depth": 5}

    def test_legacy_signature_defaults_still_apply(self, monkeypatch, tmp_path):
        requests = _capture_run(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(json.dumps({"spec": _spec(legacy_default=120)}))
        monkeypatch.chdir(tmp_path)

        assert (
            _run(
                [
                    "run",
                    str(path),
                    "--cache-dir",
                    str(tmp_path / "cache"),
                    "--input-table",
                    "/t.qza",
                ]
            )
            == 0
        )
        assert requests[0].params == {"sampling_depth": 120}


def test_pipeline_config_command_prints_the_bundled_config(tmp_path, capsys):
    from adagio.cli.main import main

    path = tmp_path / "pipeline.adg"
    path.write_text(json.dumps(_envelope([{"parameters": {"depth": 5}}])))

    with pytest.raises(SystemExit) as exit_status:
        main(["pipeline", "config", str(path)])
    assert exit_status.value.code == 0
    assert json.loads(capsys.readouterr().out) == {
        "version": 1,
        "inputs": {},
        "parameters": {"depth": 5},
        "outputs": {},
    }

    path.write_text(json.dumps(_envelope([])))
    with pytest.raises(SystemExit, match="bundles no configuration"):
        main(["pipeline", "config", str(path)])


def test_runtime_applies_the_bundled_config_under_job_arguments(tmp_path):
    from adagio.cli import runtime as runtime_cli
    from adagio.model.pipeline import AdagioPipeline

    data = _envelope(
        [{"inputs": {"table": "/bundled.qza"}, "parameters": {"depth": 2}}]
    )
    pipeline = AdagioPipeline.model_validate(data["spec"])

    arguments = runtime_cli._build_arguments(
        pipeline=pipeline,
        runtime_arguments={"inputs": {}, "parameters": {"depth": 4}},
        output_dir=str(tmp_path),
        spec_data=data,
    )

    assert arguments.inputs == {"table": "/bundled.qza"}
    assert arguments.parameters == {"depth": 4}
