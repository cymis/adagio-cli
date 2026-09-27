"""Actual Slurm + unchanged feature-table acceptance; run inside test submit host.

Requires make_fixture.py first. Saves scheduler identities, statuses and output
validation separately from deterministic mocked scheduler unit tests. A run
that succeeds removes its shared work directory, so jobs are counted from
Slurm accounting; failed and interrupted runs keep their submission registry.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

root = Path("/workspace/acceptance-output")
base_config = json.loads((root / "config.json").read_text())
base_spec = json.loads((root / "branch.adg").read_text())
report = {}


def adagio_cleanup(record):
    result = subprocess.run(
        [sys.executable, "-m", "adagio.cli.main", "cleanup", str(record)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, json.loads(result.stdout)


def run_case(
    name, *, config=None, spec=None, signal_number=None, targets=None, supervised=False
):
    case = root / name
    case.mkdir(exist_ok=True)
    existing_registries = set((case / "work").glob("*/submissions.json"))
    started = time.strftime("%Y-%m-%dT%H:%M:%S")
    cfg = json.loads(json.dumps(config or base_config))
    cfg["executor"]["work_dir"] = str(case / "work")
    (case / "config.json").write_text(json.dumps(cfg))
    (case / "pipeline.adg").write_text(json.dumps(spec or base_spec))
    cmd = [
        sys.executable,
        "-m",
        "adagio.cli.main",
        "runtime",
        "--spec",
        str(case / "pipeline.adg"),
        "--config",
        str(case / "config.json"),
        "--arguments",
        str(root / "arguments.json"),
        "--cache-dir",
        str(root / "cache"),
        "--recycle-pool",
        "slurm-acceptance",
        "--output-dir",
        str(case / "outputs"),
    ]
    if targets:
        cmd += ["--targets", targets]
    # Run records are locked, so they live on local disk; /workspace is a bind
    # mount that accepts locks without enforcing them.
    record = Path("/tmp/adagio-acceptance") / name / "run-record.json"
    record.parent.mkdir(parents=True, exist_ok=True)
    if supervised:
        # As the runtime server runs it: stdin is a pipe the server holds open.
        cmd += ["--exit-with-stdin", "--run-record", str(record)]
    with (case / "driver.log").open("w") as log:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if supervised else None,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            # Whatever this script inherited (nohup ignores SIGHUP).
            preexec_fn=lambda: signal.signal(signal.SIGHUP, signal.SIG_DFL),
        )
        if signal_number or supervised:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                registries = sorted(
                    set((case / "work").glob("*/submissions.json"))
                    - existing_registries
                )
                if registries and any(
                    a.get("job_id")
                    for a in json.loads(registries[0].read_text())["attempts"].values()
                ):
                    break
                if process.poll() is not None:
                    raise RuntimeError("Coordinator exited before cancellation test")
                time.sleep(0.1)
            if supervised:
                # While the run's own process lives, cleanup must not touch it.
                owned = adagio_cleanup(record)
                assert owned[0] == 75 and owned[1]["owner_alive"], owned
                process.stdin.close()  # as when the server dies
            else:
                process.send_signal(signal_number)
        code = process.wait(timeout=180)
        if supervised:
            # The run cancelled its own jobs; cleanup then finds nothing left.
            assert adagio_cleanup(record) == (0, {"complete": True, "errors": []})
            assert not record.exists()
            assert not record.with_name(record.name + ".lock").exists()
    registries = sorted(
        set((case / "work").glob("*/submissions.json")) - existing_registries
    )
    registry = json.loads(registries[0].read_text()) if registries else {"attempts": {}}
    accounting = [
        line.split("|")
        for line in subprocess.run(
            [
                "sacct",
                "-n",
                "-P",
                "-X",
                f"--starttime={started}",
                "--format=JobIDRaw,JobName,State,ExitCode,AllocCPUS,ReqMem,Start,End",
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.splitlines()
        if line.split("|")[1].startswith("adagio-")
    ]
    report[name] = {
        "command": cmd,
        "exit_code": code,
        "registry": registry,
        "accounting": accounting,
    }
    (root / "acceptance-report.json").write_text(json.dumps(report, indent=2))
    return code, registry, accounting


def main():
    code, registry, jobs = run_case("cache-rerun")
    assert code == 0
    assert len(jobs) == 4 and not registry["attempts"]
    code, registry, jobs = run_case("targeted", targets="B")
    assert code == 0
    assert len(jobs) == 2
    one = json.loads(json.dumps(base_config))
    one["executor"]["max_in_flight"] = 1
    code, registry, jobs = run_case("bounded-one", config=one)
    assert code == 0
    failure = json.loads(json.dumps(base_spec))
    failure["graph"][1]["parameters"]["min_frequency"]["value"] = 1000000
    failure_config = json.loads(json.dumps(base_config))
    failure_config["resources"]["tasks"]["C"]["cpus"] = 10
    code, registry, jobs = run_case("fail-fast", config=failure_config, spec=failure)
    assert code != 0
    assert "D" not in {a["node_id"] for a in registry["attempts"].values()}
    for name, number in [
        ("sigint", signal.SIGINT),
        ("sigterm", signal.SIGTERM),
        ("sighup", signal.SIGHUP),
    ]:
        code, registry, jobs = run_case(name, signal_number=number)
        assert code != 0
        assert all(job[2].startswith(("CANCELLED", "COMPLETED")) for job in jobs), jobs
    code, registry, jobs = run_case("server-gone", supervised=True)
    assert code != 0
    assert all(job[2].startswith(("CANCELLED", "COMPLETED")) for job in jobs), jobs
    local = json.loads(json.dumps(base_config))
    local["executor"] = {"kind": "local"}
    # The local executor shares the same cache and scientific worker path.
    code, registry, jobs = run_case("local", config=local)
    assert code == 0 and not jobs
    print(root / "acceptance-report.json")


if __name__ == "__main__":
    main()
