"""Generate raw BIOM publication fixture; invoke inside the scientific image."""

import json
from pathlib import Path

from biom import Table
from biom.util import biom_open
from qiime2 import Artifact

root = Path("/workspace/acceptance-output")
table = Artifact.load(str(root / "table.qza")).view(Table)
with biom_open(str(root / "table.biom"), "w") as stream:
    table.to_hdf5(stream, "adagio-slurm-acceptance")
pipeline = json.loads((root / "branch.adg").read_text())
archive = pipeline["signature"]["inputs"][0]
archive["id"] = "raw"
archive["name"] = "raw"
imported = {
    "id": "Import",
    "kind": "built-in",
    "name": "data-import",
    "inputs": {"source": {"kind": "archive", "id": "raw"}},
    "parameters": {
        "semantic_type": {"kind": "literal", "value": "FeatureTable[Frequency]"},
        "input_format": {"kind": "literal", "value": "BIOMV210Format"},
    },
    "outputs": {"artifact": {"kind": "archive", "id": "imported"}},
}
a = pipeline["graph"][0]
a["inputs"]["table"]["id"] = "imported"
a["parameters"]["random_seed"] = {"kind": "literal", "value": 42}
pipeline["graph"] = [imported, a]
pipeline["signature"]["outputs"] = [
    {"id": id, "name": name, "type": archive["type"], "ast": archive["ast"]}
    for id, name in [("imported", "imported"), ("A-out", "rarefied")]
]
(root / "import.adg").write_text(json.dumps(pipeline))
(root / "import-arguments.json").write_text(
    json.dumps({"inputs": {"raw": str(root / "table.biom")}, "parameters": {}})
)
config = json.loads((root / "apptainer-config.json").read_text())
config["executor"]["work_dir"] = str(root / "import-work")
(root / "import-config.json").write_text(json.dumps(config))
