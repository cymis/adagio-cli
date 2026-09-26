"""Use fixed scientific parameters to verify real cache hits under Slurm."""

import json
from pathlib import Path

from run_acceptance import base_spec, root, run_case

spec = json.loads(json.dumps(base_spec))
spec["graph"][0]["parameters"]["random_seed"] = {"kind": "literal", "value": 42}
spec["graph"][2]["parameters"]["max_frequency"] = {"kind": "literal", "value": 1000000}
for name in ["deterministic-first", "deterministic-rerun"]:
    code, registry = run_case(name, spec=spec)
    assert code == 0
    manifests = list(Path(registry["run_dir"]).rglob("*results.json"))
    reused = {p.name: json.loads(p.read_text())["reused"] for p in manifests}
    (root / name / "reuse-evidence.json").write_text(json.dumps(reused, indent=2))
    print(name, reused, flush=True)
    if name.endswith("rerun"):
        assert len(reused) == 4 and all(reused.values())
