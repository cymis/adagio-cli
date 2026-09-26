"""Plan inspection works without scientific imports or scheduler commands."""

import json
import subprocess
import sys
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def plan(tmp_path, config, *extra):
    args = tmp_path / "args.json"
    args.write_text(
        json.dumps({"inputs": {"table": "/shared/table.qza"}, "parameters": {}})
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "adagio.cli.main",
            "runtime",
            "--spec",
            str(FIXTURES / "slurm-branch.adg"),
            "--config",
            str(config),
            "--arguments",
            str(args),
            "--cache-dir",
            "/shared/cache",
            "--output-dir",
            str(tmp_path / "outputs"),
            "--plan-only",
            *extra,
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_plan_only_resolves_target_closure_without_launching(tmp_path):
    result = plan(tmp_path, FIXTURES / "slurm-run-v1.json", "--targets", "B")
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert [node["node_id"] for node in report["tasks"]] == ["A", "B"]
    assert report["executor"]["kind"] == "slurm"
    assert all(node["resources"]["cpus"] == 2 for node in report["tasks"])
    assert report["tasks"][0]["environment"]["kind"] == "apptainer"
    assert not (tmp_path / "outputs").exists()


def test_plan_only_reports_environments_the_executor_cannot_place(tmp_path):
    config = json.loads((FIXTURES / "slurm-run-v1.json").read_text())
    config["defaults"] = {"kind": "docker", "image": "example/plugin:latest"}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    result = plan(tmp_path, path)
    assert result.returncode != 0
    assert "Docker is unsupported" in result.stderr + result.stdout
