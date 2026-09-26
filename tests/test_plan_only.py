"""Plan inspection must work without scientific imports or scheduler commands."""

import json
import subprocess
import sys
from pathlib import Path


def test_plan_only_resolves_target_closure_without_launching(tmp_path):
    config = Path(__file__).parent / "fixtures/slurm-run-v1.json"
    spec = Path(__file__).parent / "fixtures/slurm-branch.adg"
    args = tmp_path / "args.json"
    args.write_text(
        json.dumps({"inputs": {"table": "/shared/table.qza"}, "parameters": {}})
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "adagio.cli.main",
            "runtime",
            "--spec",
            str(spec),
            "--config",
            str(config),
            "--arguments",
            str(args),
            "--cache-dir",
            "/shared/cache",
            "--targets",
            "B",
            "--plan-only",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert [node["node_id"] for node in plan["tasks"]] == ["A", "B"]
    assert plan["executor"]["kind"] == "slurm"
    assert all(node["resources"]["cpus"] == 2 for node in plan["tasks"])
