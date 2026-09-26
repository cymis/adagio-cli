"""Actual Slurm + unchanged feature-table acceptance; run inside test submit host.

Requires make_fixture.py first. Saves scheduler identities, statuses and output
validation separately from deterministic mocked scheduler unit tests.
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


def run_case(name, *, config=None, spec=None, signal_number=None, targets=None):
    case = root / name
    case.mkdir(exist_ok=True)
    existing_registries = set((case / "work").glob("*/submissions.json"))
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
    with (case / "driver.log").open("w") as log:
        process = subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, env=os.environ.copy()
        )
        if signal_number:
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
            process.send_signal(signal_number)
        code = process.wait(timeout=180)
    registries = sorted(
        set((case / "work").glob("*/submissions.json")) - existing_registries
    )
    registry = json.loads(registries[0].read_text()) if registries else {"attempts": {}}
    ids = [a["job_id"] for a in registry["attempts"].values() if a.get("job_id")]
    accounting = (
        subprocess.run(
            [
                "sacct",
                "-n",
                "-P",
                "-X",
                "-j",
                ",".join(ids),
                "--format=JobIDRaw,JobName,State,ExitCode,AllocCPUS,ReqMem,Start,End",
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout
        if ids
        else ""
    )
    report[name] = {
        "command": cmd,
        "exit_code": code,
        "registry": registry,
        "accounting": accounting,
    }
    (root / "acceptance-report.json").write_text(json.dumps(report, indent=2))
    return code, registry


def main():
    code, registry = run_case("cache-rerun")
    assert code == 0
    assert len(registry["attempts"]) == 4
    code, registry = run_case("targeted", targets="B")
    assert code == 0
    assert {a["node_id"] for a in registry["attempts"].values()} == {"A", "B"}
    one = json.loads(json.dumps(base_config))
    one["executor"]["max_in_flight"] = 1
    code, registry = run_case("bounded-one", config=one)
    assert code == 0
    failure = json.loads(json.dumps(base_spec))
    failure["graph"][1]["parameters"]["min_frequency"]["value"] = 1000000
    failure_config = json.loads(json.dumps(base_config))
    failure_config["resources"]["tasks"]["C"]["cpus"] = 10
    code, registry = run_case("fail-fast", config=failure_config, spec=failure)
    assert code != 0
    assert "D" not in {a["node_id"] for a in registry["attempts"].values()}
    code, registry = run_case("sigint", signal_number=signal.SIGINT)
    assert code != 0
    code, registry = run_case("sigterm", signal_number=signal.SIGTERM)
    assert code != 0
    serial = json.loads(json.dumps(base_config))
    serial["executor"] = {"kind": "serial"}
    # Serial shares the same cache and scientific worker path.
    code, registry = run_case("serial", config=serial)
    assert code == 0
    print(root / "acceptance-report.json")


if __name__ == "__main__":
    main()
