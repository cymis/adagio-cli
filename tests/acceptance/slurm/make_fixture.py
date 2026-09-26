"""Generate a tiny real feature-table pipeline with unchanged installed actions."""

import json
from pathlib import Path

import numpy as np
from biom import Table
from qiime2 import Artifact
from qiime2.sdk import PluginManager

root = Path("/workspace/acceptance-output")
root.mkdir(exist_ok=True)
input_file = root / "table.qza"
Artifact.import_data(
    "FeatureTable[Frequency]",
    Table(
        np.array([[9, 5, 6], [3, 7, 6], [6, 6, 6]]),
        ["f1", "f2", "f3"],
        ["s1", "s2", "s3"],
    ),
).save(str(input_file))
ast = {
    "type": "expression",
    "builtin": False,
    "name": "FeatureTable",
    "predicate": None,
    "fields": [
        {
            "type": "expression",
            "builtin": False,
            "name": "Frequency",
            "predicate": None,
            "fields": [],
        }
    ],
}


def action(id, name, inputs, params):
    return {
        "id": id,
        "kind": "plugin-action",
        "plugin": "feature_table",
        "action": name,
        "inputs": inputs,
        "parameters": {k: {"kind": "literal", "value": v} for k, v in params.items()},
        "outputs": {
            "rarefied_table"
            if name == "rarefy"
            else "merged_table"
            if name == "merge"
            else "filtered_table": {"kind": "archive", "id": id + "-out"}
        },
    }


def src(id):
    return {"kind": "archive", "id": id}


a = action("A", "rarefy", {"table": src("input")}, {"sampling_depth": 10})
b = action("B", "filter_samples", {"table": src("A-out")}, {"min_frequency": 1})
c = action("C", "filter_features", {"table": src("A-out")}, {"min_frequency": 1})
d = action(
    "D",
    "merge",
    {
        "tables": {
            "kind": "archive-collection",
            "style": "list",
            "items": [{"key": "b", "id": "B-out"}, {"key": "c", "id": "C-out"}],
        }
    },
    {"overlap_method": "sum"},
)
pipeline = {
    "type": "pipeline",
    "signature": {
        "inputs": [
            {
                "id": "input",
                "name": "table",
                "type": "FeatureTable[Frequency]",
                "ast": ast,
                "required": True,
            }
        ],
        "parameters": [],
        "outputs": [
            {
                "id": "D-out",
                "name": "merged",
                "type": "FeatureTable[Frequency]",
                "ast": ast,
            }
        ],
    },
    "graph": [a, b, c, d],
}
(root / "branch.adg").write_text(json.dumps(pipeline))
(root / "arguments.json").write_text(
    json.dumps(
        {
            "inputs": {"table": str(input_file)},
            "parameters": {},
            "outputs": str(root / "outputs"),
        }
    )
)
config = {
    "version": 1,
    "defaults": {
        "kind": "conda",
        "prefix": "/opt/conda/envs/qiime2-tiny-2026.7",
        "conda_executable": "/opt/test-conda/bin/conda",
    },
    "executor": {
        "kind": "slurm",
        "work_dir": str(root / "work"),
        "max_in_flight": 2,
        "slurm": {"partition": "test", "account": "test", "time_limit": "00:03:00"},
    },
    "resources": {
        "defaults": {"cpus": 1, "memory": "512 MiB"},
        "tasks": {
            "B": {"cpus": 2, "memory": "768 MiB"},
            "C": {"cpus": 3, "memory": "1 GiB"},
            "D": {"cpus": 1, "memory": "640 MiB"},
        },
    },
}
(root / "config.json").write_text(json.dumps(config))
versions = {key: str(plugin.version) for key, plugin in PluginManager().plugins.items()}
(root / "environment.json").write_text(json.dumps(versions, indent=2))
print(root)
