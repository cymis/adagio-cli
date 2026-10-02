"""Opt-in cache regression and timing test against a real QIIME Docker image.

Set ADAGIO_DOCKER_TEST_IMAGE to an installed image containing feature-table.
All inputs are synthetic and the cache is isolated in pytest's temporary path.
"""

import json
import os
import subprocess
import time
import zipfile
from pathlib import Path

import pytest

from adagio.executors.base import TaskEnvironmentSpec
from adagio.executors.cache_support import ExecutionCacheConfig
from adagio.executors.docker import DockerTaskEnvironmentLauncher
from adagio.executors.task_environments import TaskEnvironmentExecutor
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor

IMAGE = os.environ.get("ADAGIO_DOCKER_TEST_IMAGE")
pytestmark = pytest.mark.skipif(
    not IMAGE, reason="Set ADAGIO_DOCKER_TEST_IMAGE to run real Docker cache tests"
)


def _endpoint(name, kind):
    return {
        "id": name,
        "name": name,
        "type": f"FeatureTable[{kind}]",
        "ast": {
            "type": "expression",
            "builtin": False,
            "name": "FeatureTable",
            "predicate": None,
            "fields": [
                {
                    "type": "expression",
                    "builtin": False,
                    "name": kind,
                    "predicate": None,
                    "fields": [],
                }
            ],
        },
    }


def _pipeline(min_frequency=1):
    return AdagioPipeline.model_validate(
        {
            "type": "pipeline",
            "signature": {
                "inputs": [{**_endpoint("table", "Frequency"), "required": True}],
                "parameters": [],
                "outputs": [
                    _endpoint("filtered", "Frequency"),
                    _endpoint("relative", "RelativeFrequency"),
                ],
            },
            "graph": [
                {
                    "id": "filter",
                    "kind": "plugin-action",
                    "plugin": "feature-table",
                    "action": "filter_samples",
                    "inputs": {"table": {"kind": "archive", "id": "table"}},
                    "parameters": {
                        "min_frequency": {"kind": "literal", "value": min_frequency}
                    },
                    "outputs": {
                        "filtered_table": {"kind": "archive", "id": "filtered"}
                    },
                },
                {
                    "id": "relative",
                    "kind": "plugin-action",
                    "plugin": "feature-table",
                    "action": "relative_frequency",
                    "inputs": {"table": {"kind": "archive", "id": "filtered"}},
                    "parameters": {},
                    "outputs": {
                        "relative_frequency_table": {
                            "kind": "archive",
                            "id": "relative",
                        }
                    },
                },
            ],
        }
    )


class _Resolver:
    def resolve(self, *, task):
        return TaskEnvironmentSpec(
            kind="docker", reference=IMAGE, options={"platform": "linux/amd64"}
        )


class _Monitor(Monitor):
    def __init__(self, name):
        self.name = name
        self.started = time.monotonic()
        self.phases = []
        self.tasks = []

    def update_task_phase(self, *, task_id, phase):
        elapsed = time.monotonic() - self.started
        self.phases.append(
            {"task_id": task_id, "phase": phase, "elapsed_seconds": elapsed}
        )
        print(f"{self.name}/{task_id}: {phase} ({elapsed:.2f}s)", flush=True)

    def finish_task(self, *, task_id, status="completed", error=None, **details):
        self.tasks.append(
            {"task_id": task_id, "status": status, "error": error, **details}
        )


def _artifact_id(path):
    with zipfile.ZipFile(path) as archive:
        assert archive.testzip() is None
        return archive.namelist()[0].split("/")[0]


def test_real_cache_hit_miss_invalidation_and_disabled_reuse(tmp_path):
    # Import once so warm runs receive the identical QIIME artifact identities.
    generate = """
import biom
import numpy as np
from qiime2 import Artifact
data = np.random.default_rng(0).integers(1, 20, size=(2000, 50))
features = [f'f{i}' for i in range(data.shape[0])]
samples = [f's{i}' for i in range(data.shape[1])]
Artifact.import_data('FeatureTable[Frequency]', biom.Table(data, features, samples)).save('/test/table.qza')
data[:, 0] = 0
Artifact.import_data('FeatureTable[Frequency]', biom.Table(data, features, samples)).save('/test/changed-table.qza')
"""
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--platform",
            "linux/amd64",
            "-v",
            f"{tmp_path}:/test",
            IMAGE,
            "python",
            "-c",
            generate,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )
    executor = TaskEnvironmentExecutor(
        environment_resolver=_Resolver(),
        launchers={"docker": DockerTaskEnvironmentLauncher()},
    )
    image = json.loads(
        subprocess.check_output(["docker", "image", "inspect", IMAGE], text=True)
    )[0]
    report = {
        "image": IMAGE,
        "image_id": image["Id"],
        "platform": "linux/amd64",
        "input_bytes": (tmp_path / "table.qza").stat().st_size,
        "runs": [],
    }
    report_path = tmp_path / "cache-profile.json"

    def run(name, *, min_frequency=1, input_name="table.qza", reuse=True):
        monitor = _Monitor(name)
        root = tmp_path / name
        started = time.monotonic()
        try:
            executor.execute(
                pipeline=_pipeline(min_frequency),
                arguments=AdagioArguments(
                    inputs={"table": str(tmp_path / input_name)},
                    parameters={},
                    outputs=str(root / "outputs"),
                ),
                cache_config=ExecutionCacheConfig(
                    cache_dir=tmp_path / "cache",
                    recycle_pool="cache-integration" if reuse else None,
                ),
                monitor=monitor,
                log_dir=str(root / "logs"),
            )
        finally:
            result = {
                "name": name,
                "wall_seconds": time.monotonic() - started,
                "phases": monitor.phases,
                "tasks": monitor.tasks,
            }
            report["runs"].append(result)
            report_path.write_text(json.dumps(report, indent=2))
        assert len(monitor.tasks) == 2
        for task in monitor.tasks:
            phases = [
                event["phase"]
                for event in monitor.phases
                if event["task_id"] == task["task_id"]
            ]
            cached = name.startswith("warm")
            assert task["status"] == ("cached" if cached else "completed"), task
            assert task["reused"] is cached
            if cached:
                assert "checking_cache" in phases and "using_cache" in phases
                assert "running" not in phases
                assert task["timings"]["action_seconds"] == 0
            else:
                assert "running" in phases
                assert task["timings"]["action_seconds"] > 0
            if not reuse:
                assert "checking_cache" not in phases and "using_cache" not in phases
            log = Path(task["log_path"]).read_text()
            assert "Adagio task timings" in log
            assert "input_signature_seconds:" in log
            assert "output_publish_seconds:" in log
        result["output_ids"] = {
            name: _artifact_id(root / "outputs" / f"{name}.qza")
            for name in ("filtered", "relative")
        }
        report_path.write_text(json.dumps(report, indent=2))
        return result

    cold = run("cold")
    for index in range(3):
        warm = run(f"warm-{index + 1}")
        assert warm["output_ids"] == cold["output_ids"]
    changed_param = run("changed-parameter", min_frequency=2)
    changed_input = run("changed-input", input_name="changed-table.qza")
    disabled = run("reuse-disabled", reuse=False)
    for fresh in (changed_param, changed_input, disabled):
        assert all(
            fresh["output_ids"][key] != cold["output_ids"][key]
            for key in cold["output_ids"]
        )
    print(f"Cache profile: {report_path}", flush=True)
