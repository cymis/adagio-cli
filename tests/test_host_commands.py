"""The commands a supervising runtime calls: ``capabilities`` and ``cleanup``."""

import json
import subprocess
import sys


def adagio(*args):
    return subprocess.run(
        [sys.executable, "-m", "adagio.cli.main", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_capabilities_reports_config_version_and_executors():
    result = adagio("capabilities")
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["config_version"] == 1
    local = report["executors"]["local"]
    assert local["available"] is True
    assert set(local["task_environments"]) == {"apptainer", "conda", "docker"}
    slurm = report["executors"]["slurm"]
    assert slurm["task_environments"] == ["apptainer", "conda"]
    assert slurm["available"] or "not found on PATH" in slurm["reason"]


def test_cleanup_without_a_record_has_nothing_to_do(tmp_path):
    result = adagio("cleanup", str(tmp_path / "run-record.json"))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"complete": True, "errors": []}


def test_cleanup_reports_an_unreadable_record(tmp_path):
    record = tmp_path / "run-record.json"
    record.write_text("{}")
    result = adagio("cleanup", str(record))
    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert not report["complete"]
    assert "Unrecognized run record" in report["errors"][0]
