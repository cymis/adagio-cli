"""A pipeline file saved before all its values were set opens, but never runs."""

import json
from types import SimpleNamespace

import pytest

from adagio.bundled_config import unset_values
from adagio.executors.base import TaskExecutionResult


def _envelope(incomplete=None):
    spec = {
        "type": "pipeline",
        "signature": {"inputs": [], "parameters": [], "outputs": []},
        "graph": [
            {
                "id": "rarefy-node",
                "name": "rarefy",
                "kind": "plugin-action",
                "plugin": "feature_table",
                "action": "rarefy",
                "inputs": {},
                "outputs": {},
                "parameters": {},
            }
        ],
    }
    envelope = {
        "name": "p",
        "spec": spec,
        "layout": {},
        "configs": [
            {
                "execution": {
                    "version": 1,
                    "plugins": {"feature_table": {"kind": "docker", "image": "saved/image:1"}},
                }
            }
        ],
    }
    if incomplete is not None:
        envelope["incomplete"] = incomplete
    return envelope


def test_unset_values_reads_the_list_and_ignores_anything_else():
    assert unset_values(_envelope(["Missing argument for `sampling_depth`."])) == [
        "Missing argument for `sampling_depth`."
    ]
    assert unset_values(_envelope()) == []
    assert unset_values(_envelope("not a list")) == []
    assert unset_values({"incomplete": ["", 3]}) == []
    assert unset_values([]) == []


@pytest.mark.parametrize("incomplete", [["Missing argument for `sampling_depth` in [demo]."]])
def test_run_refuses_an_incomplete_file_before_any_step(
    monkeypatch, tmp_path, capsys, incomplete
):
    launched = []
    fake = SimpleNamespace(
        launch=lambda **kwargs: launched.append(kwargs) or TaskExecutionResult(outputs={})
    )
    monkeypatch.setattr(
        "adagio.executors.builtin_task_environment_launchers",
        lambda: {"docker": fake, "conda": fake},
    )
    path = tmp_path / "pipeline.adg"
    path.write_text(json.dumps(_envelope(incomplete)))
    monkeypatch.chdir(tmp_path)

    from adagio.cli.main import main

    with pytest.raises(SystemExit) as exit_status:
        main(["run", str(path), "--cache-dir", str(tmp_path / "cache")])

    assert exit_status.value.code == 1
    assert launched == []
    out = capsys.readouterr().out
    assert "This pipeline is incomplete" in out
    # Shown as written, brackets included.
    assert "Missing argument for `sampling_depth` in [demo]." in out


def test_a_complete_file_still_runs(monkeypatch, tmp_path):
    launched = []
    fake = SimpleNamespace(
        launch=lambda **kwargs: launched.append(kwargs) or TaskExecutionResult(outputs={})
    )
    monkeypatch.setattr(
        "adagio.executors.builtin_task_environment_launchers",
        lambda: {"docker": fake, "conda": fake},
    )
    path = tmp_path / "pipeline.adg"
    path.write_text(json.dumps(_envelope([])))
    monkeypatch.chdir(tmp_path)

    from adagio.cli.main import main

    with pytest.raises(SystemExit) as exit_status:
        main(["run", str(path), "--cache-dir", str(tmp_path / "cache")])

    assert exit_status.value.code == 0
    assert len(launched) == 1
