"""Run in a QIIME/Rachis environment: PYTHONPATH=src python this_file.py.

Uses real artifacts, actions and the framework cache, with an in-memory HTTPS
response so the test is independent of an external data server.
"""

import os

os.environ["QIIMETEST"] = "1"

from io import BytesIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import zipfile

from qiime2 import Artifact
from qiime2.sdk import PluginManager
from adagio.cli.task_exec import _run_task

URL = "https://example.org/ints.qza"


class Response(BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.headers = {"Content-Length": str(len(payload))}

    def geturl(self):
        return URL


with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    plugin = PluginManager().plugins["dummy-plugin"]
    concatenate = plugin.actions["concatenate_ints"]
    ancestor = Artifact.import_data("IntSequence1", [1, 2, 3])
    third_input = Artifact.import_data("IntSequence2", [4])
    third_path = third_input.save(str(root / "third.qza"))
    original = concatenate(ancestor, ancestor, third_input, 4, 5).concatenated_ints
    archive = Path(original.save(str(root / "original.qza")))
    payload = archive.read_bytes()
    calls = []

    def open_response(request, timeout):
        calls.append(request.full_url)
        return Response(payload)

    def run(run_name):
        work = root / run_name
        work.mkdir()
        spec = {
            "plugin": "dummy-plugin",
            "action": "concatenate_ints",
            "archive_inputs": {"ints1": URL, "ints2": URL, "ints3": third_path},
            "remote_input_types": {name: "IntSequence1" for name in ["ints1", "ints2"]},
            "params": {"int1": 6, "int2": 7},
            "outputs": {"concatenated_ints": str(work / "result")},
            "result_manifest": str(work / "results.json"),
            "cache_path": str(root / "cache"),
            "recycle_pool": "remote-test",
        }
        _run_task(spec)
        return json.loads((work / "results.json").read_text())

    with patch(
        "adagio.remote_inputs.build_opener",
        return_value=SimpleNamespace(open=open_response),
    ):
        first = run("run1")
        assert len(calls) == 1  # Two references, one download.
        result = Artifact.load(first["outputs"]["concatenated_ints"])
        assert first["input_downloads"][0]["uuid"] == str(original.uuid)
        with zipfile.ZipFile(first["outputs"]["concatenated_ints"]) as output:
            members = output.namelist()
            assert any(f"/artifacts/{original.uuid}/" in member for member in members)
            assert any(f"/artifacts/{ancestor.uuid}/" in member for member in members)
        staged = next((root / "run1" / ".adagio-downloads").glob("*.qza"))
        assert staged.read_bytes() == payload
        assert Artifact.load(staged).uuid == original.uuid

        second = run("run2")
        assert len(calls) == 2  # Fresh download before cache lookup on another run.
        assert second["reused"] is True
        assert Artifact.load(second["outputs"]["concatenated_ints"]).uuid == result.uuid

        replacement = Artifact.import_data("IntSequence1", [100])
        payload = Path(replacement.save(str(root / "replacement.qza"))).read_bytes()
        third = run("run3")
        assert len(calls) == 3
        assert third["reused"] is False
        assert Artifact.load(third["outputs"]["concatenated_ints"]).uuid != result.uuid

    print(
        "PASS: unchanged bytes/UUID, complete ancestral provenance, one transfer per run, "
        "cache reuse after fetching, and changed URL content invalidates action reuse."
    )
