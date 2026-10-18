"""Run with the taxa plugin installed and the patched adagio package on PYTHONPATH."""

from io import BytesIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from biom import Table
import numpy as np
import pandas as pd
from qiime2 import Artifact
from qiime2.sdk import PluginManager

from adagio.cli.task_exec import _run_task


URL = "https://example.org/table.qza"


class Response(BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.headers = {"Content-Length": str(len(payload))}

    def geturl(self):
        return URL


with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    action = PluginManager().plugins["taxa"].actions["filter_table"]
    display_type = str(action.signature.inputs["table"].qiime_type)
    assert "¹" in display_type, display_type
    table = Artifact.import_data(
        "FeatureTable[Frequency]",
        Table(np.array([[1, 2], [3, 4]]), ["f1", "f2"], ["s1", "s2"]),
    )
    taxonomy = Artifact.import_data(
        "FeatureData[Taxonomy]",
        pd.DataFrame(
            {"Taxon": ["k__Bacteria; p__Firmicutes", "k__Bacteria; p__Proteobacteria"]},
            index=pd.Index(["f1", "f2"], name="Feature ID"),
        ),
    )
    table_path = table.save(str(root / "table.qza"))
    taxonomy_path = taxonomy.save(str(root / "taxonomy.qza"))
    payload = Path(table_path).read_bytes()
    output_name = next(iter(action.signature.outputs))
    calls = []

    def open_response(request, timeout):
        calls.append(request.full_url)
        return Response(payload)

    def run(name, source):
        work = root / name
        work.mkdir()
        spec = {
            "plugin": "taxa",
            "action": "filter_table",
            "archive_inputs": {"table": source, "taxonomy": taxonomy_path},
            "remote_input_types": {"table": display_type},
            "params": {"include": "Firmicutes"},
            "outputs": {output_name: str(work / "filtered")},
            "result_manifest": str(work / "results.json"),
        }
        _run_task(spec)
        manifest = json.loads((work / "results.json").read_text())
        return Artifact.load(manifest["outputs"][output_name]).view(Table)

    with patch(
        "adagio.remote_inputs.build_opener",
        return_value=SimpleNamespace(open=open_response),
    ):
        local = run("local", table_path)
        remote = run("remote", URL)
        assert local == remote
        assert list(remote.ids(axis="observation")) == ["f1"]
        assert calls == [URL]
        payload = Path(taxonomy_path).read_bytes()
        for name, source in (("invalid-local", taxonomy_path), ("invalid-remote", URL)):
            try:
                run(name, source)
            except TypeError as error:
                assert "requires an argument of type FeatureTable" in str(error), str(error)
            else:
                raise AssertionError("Taxonomy was accepted as a feature table")

    print("PASS: taxa.filter_table produces the same result for URL and local inputs; wrong types are rejected.")
