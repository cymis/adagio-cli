"""Use fixed scientific parameters to verify real cache hits under Slurm."""

import json
import re

from run_acceptance import base_spec, root, run_case

spec = json.loads(json.dumps(base_spec))
spec["graph"][0]["parameters"]["random_seed"] = {"kind": "literal", "value": 42}
spec["graph"][2]["parameters"]["max_frequency"] = {"kind": "literal", "value": 1000000}
for name in ["deterministic-first", "deterministic-rerun"]:
    code, registry, jobs = run_case(name, spec=spec)
    assert code == 0
    statuses = dict(
        re.findall(
            r"finished task id=(\S+) status=(\w+)",
            (root / name / "driver.log").read_text(),
        )
    )
    (root / name / "reuse-evidence.json").write_text(json.dumps(statuses, indent=2))
    print(name, statuses, flush=True)
    if name.endswith("rerun"):
        assert len(statuses) == 4 and set(statuses.values()) == {"cached"}
