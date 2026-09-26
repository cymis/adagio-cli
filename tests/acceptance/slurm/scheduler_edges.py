"""Real allocation accounting without a worker manifest; no mocked commands."""

import json
import time
from pathlib import Path
from types import SimpleNamespace

from adagio.cli.config import ExecutorConfig, TaskResourceRequirements
from adagio.execution.slurm import TERMINAL, SlurmBackend
from adagio.executors.base import TaskExecutionRequest
from adagio.executors.prepared import PreparedInvocation

root = Path("/workspace/acceptance-output/scheduler-edges")
root.mkdir(exist_ok=True)
work = root / "attempt"
work.mkdir(exist_ok=True)
source = work / ".adagio-container-python/adagio/cli/task_exec.py"
source.parent.mkdir(parents=True, exist_ok=True)
source.write_text("")
spec = work / "spec.json"
spec.write_text("{}")
request = TaskExecutionRequest(
    task=SimpleNamespace(id="missing-result"),
    cwd=root,
    work_path=work,
    archive_inputs={},
    archive_collection_inputs={},
    metadata_inputs={},
    params={},
    metadata_column_kwargs={},
    outputs={"out": str(work / "absent")},
)
p = PreparedInvocation(
    command=["/bin/true"],
    env={},
    cwd=root,
    spec_path=spec,
    manifest_path=work / "results.json",
    log_path=work / "task.log",
    request=request,
    image_ref="/bin/true",
)
b = SlurmBackend(
    config=ExecutorConfig(
        kind="slurm", work_dir=str(root), slurm={"partition": "test"}
    ),
    run_dir=root / "run",
)
h = b.submit(p, TaskResourceRequirements(cpus=1, memory="32 MiB"))
for _ in range(120):
    b.poll([h])
    if h.state in TERMINAL:
        break
    time.sleep(0.5)
try:
    b.collect(h)
except RuntimeError as error:
    assert "did not write an output manifest" in str(error)
    (root / "report.json").write_text(
        json.dumps(
            {
                "job_id": h.job_id,
                "state": h.state,
                "exit_code": h.exit_code,
                "expected_error": str(error),
            },
            indent=2,
        )
    )
    print(error)
else:
    raise AssertionError("Missing manifest incorrectly accepted")
