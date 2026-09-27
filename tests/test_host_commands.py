"""The commands a supervising runtime calls: ``capabilities`` and ``cleanup``."""

import json
import subprocess
import sys
import time

from adagio.execution.backends.run_record import owning_run


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


def test_cleanup_leaves_a_run_alone_while_its_owner_lives(tmp_path):
    record = tmp_path / "run-record.json"
    record.write_text("{}")
    with owning_run(record):
        result = adagio("cleanup", str(record))
    assert result.returncode == 75, result.stderr
    report = json.loads(result.stdout)
    assert report["owner_alive"] is True and not report["complete"]
    assert "pid" in report["errors"][0]
    assert record.read_text() == "{}"


WATCHED = """
import signal, subprocess, sys, time
from adagio.cli.runtime import _terminate_when_stdin_closes

TASK = (
    "import signal, sys, time\\n"
    "signal.signal(signal.SIGTERM, lambda *a: (open(sys.argv[1], 'w').write('stopped'), sys.exit(0)))\\n"
    "print('ready', flush=True)\\n"
    "time.sleep(60)"
)

def stop(signum, frame):
    print("terminated", flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, stop)
_terminate_when_stdin_closes()
# Children read /dev/null, not the supervisor's pipe.
print(repr(subprocess.run(["cat"], capture_output=True, timeout=5).stdout), flush=True)
task = subprocess.Popen([sys.executable, "-c", TASK, sys.argv[1]], stdout=subprocess.PIPE, text=True)
task.stdout.readline()
print("task running", flush=True)
time.sleep(60)
"""


def test_a_closed_stdin_terminates_the_run_and_its_tasks(tmp_path):
    marker = tmp_path / "task-stopped"
    process = subprocess.Popen(
        [sys.executable, "-c", WATCHED, str(marker)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,  # as the runtime server starts it
    )
    assert process.stdout.readline() == "b''\n"
    assert process.stdout.readline() == "task running\n"
    time.sleep(0.5)
    assert process.poll() is None  # an open pipe keeps the run going
    started = time.monotonic()
    process.stdin.close()  # as when the supervisor dies
    assert process.stdout.readline() == "terminated\n"
    assert process.wait(timeout=10) == 0
    assert time.monotonic() - started < 5
    # The running task was signalled too, as the supervisor would have.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if marker.exists() and marker.read_text() == "stopped":
            break
        time.sleep(0.05)
    assert marker.read_text() == "stopped"
