"""Environments saved with a pipeline are the lowest layer under ``--config``."""

import json
from types import SimpleNamespace

import pytest

from adagio.bundled_config import saved_run_config
from adagio.executors.base import TaskEnvironmentOverride, TaskExecutionResult
from adagio.executors.defaults import (
    ConfigurableTaskEnvironmentResolver,
    EnvironmentLayer,
)
from adagio.model.task import PluginActionTask

TABLE_AST = {
    "type": "expression",
    "builtin": False,
    "name": "FeatureTable",
    "predicate": None,
    "fields": [],
}


def _spec():
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
            "parameters": [],
            "outputs": [],
        },
        "graph": [
            {
                "id": "rarefy-node",
                "name": "rarefy",
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
                "parameters": {},
            }
        ],
    }


def _envelope(execution=None):
    config = {"inputs": {"table": "/shared/table.qza"}}
    if execution is not None:
        config["execution"] = execution
    return {"name": "p", "spec": _spec(), "layout": {}, "configs": [config]}


SAVED = {
    "version": 1,
    "plugins": {"feature_table": {"kind": "docker", "image": "saved/image:1"}},
    "resources": {"tasks": {"rarefy-node": {"cpus": 2, "memory": "4 GiB"}}},
}


def _task():
    return PluginActionTask.model_validate(_spec()["graph"][0])


def _capture_environments(monkeypatch):
    environments = []

    def launch(*, environment, request, console=None, **kwargs):
        environments.append(environment)
        return TaskExecutionResult(outputs={})

    fake = SimpleNamespace(launch=launch)
    monkeypatch.setattr(
        "adagio.executors.builtin_task_environment_launchers",
        lambda: {"docker": fake, "conda": fake},
    )
    return environments


def _run(argv):
    from adagio.cli.main import main

    with pytest.raises(SystemExit) as exit_status:
        main(argv)
    return exit_status.value.code


class TestSavedRunConfig:
    def test_reads_the_execution_of_the_first_config(self):
        config = saved_run_config(_envelope(SAVED))
        assert config is not None
        assert config.plugins["feature_table"].image == "saved/image:1"
        assert config.resources.tasks["rarefy-node"].cpus == 2

    def test_a_file_without_execution_has_none(self):
        assert saved_run_config(_envelope()) is None
        assert saved_run_config({"spec": _spec()}) is None
        assert saved_run_config(_spec()) is None

    def test_malformed_execution_is_a_readable_error(self):
        with pytest.raises(SystemExit, match="saved with this pipeline"):
            saved_run_config(_envelope({"plugins": {"x": {"bogus": 1}}}))


class TestLayers:
    def test_a_higher_layer_wins_even_over_a_lower_task_override(self):
        resolver = ConfigurableTaskEnvironmentResolver(
            plugin_overrides={
                "feature_table": TaskEnvironmentOverride(
                    kind="conda", reference="/envs/ft"
                )
            },
            lower_layers=[
                EnvironmentLayer(
                    source="pipeline file",
                    task_overrides={
                        "rarefy-node": TaskEnvironmentOverride(
                            kind="docker", reference="saved/image:1"
                        )
                    },
                )
            ],
        )
        environment = resolver.resolve(task=_task())
        assert (environment.kind, environment.reference) == ("conda", "/envs/ft")
        assert resolver.source_of(task=_task()) == "--config"

    def test_the_lower_layer_applies_when_nothing_overrides_it(self):
        resolver = ConfigurableTaskEnvironmentResolver(
            lower_layers=[
                EnvironmentLayer(
                    source="pipeline file",
                    plugin_overrides={
                        "feature_table": TaskEnvironmentOverride(
                            kind="docker", reference="saved/image:1"
                        )
                    },
                )
            ],
        )
        assert resolver.resolve(task=_task()).reference == "saved/image:1"
        assert resolver.source_of(task=_task()) == "pipeline file"


class TestRun:
    def test_saved_environments_run_without_a_config_file(
        self, monkeypatch, tmp_path, capsys
    ):
        environments = _capture_environments(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(json.dumps(_envelope(SAVED)))
        monkeypatch.chdir(tmp_path)

        assert _run(["run", str(path), "--cache-dir", str(tmp_path / "cache")]) == 0
        assert environments[0].reference == "saved/image:1"
        out = capsys.readouterr().out
        assert "feature_table: docker saved/image:1 (pipeline file)" in out

    def test_config_file_overrides_a_saved_environment(
        self, monkeypatch, tmp_path, capsys
    ):
        environments = _capture_environments(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(json.dumps(_envelope(SAVED)))
        config = tmp_path / "runtime.toml"
        config.write_text(
            '[plugins]\nfeature_table = { kind = "docker", image = "mine:2" }\n'
        )
        monkeypatch.chdir(tmp_path)

        assert (
            _run(
                [
                    "run",
                    str(path),
                    "--cache-dir",
                    str(tmp_path / "cache"),
                    "--config",
                    str(config),
                ]
            )
            == 0
        )
        assert environments[0].reference == "mine:2"
        assert "feature_table: docker mine:2 (--config)" in capsys.readouterr().out

    def test_a_file_without_environments_still_needs_a_config(
        self, monkeypatch, tmp_path, capsys
    ):
        environments = _capture_environments(monkeypatch)
        path = tmp_path / "pipeline.adg"
        path.write_text(json.dumps(_envelope()))
        monkeypatch.chdir(tmp_path)

        from adagio.cli.main import main

        with pytest.raises(RuntimeError, match="no usable execution environment"):
            main(["run", str(path), "--cache-dir", str(tmp_path / "cache")])
        assert environments == []
        assert "feature_table: not configured" in capsys.readouterr().out
