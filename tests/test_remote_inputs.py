from concurrent.futures import ThreadPoolExecutor
import errno
from hashlib import sha256
from http.client import IncompleteRead
from io import BytesIO
import json
from pathlib import Path
import ssl
import sys
from types import ModuleType, SimpleNamespace
from urllib.error import HTTPError

import pytest

from adagio.cli.task_exec import _stage_remote_inputs
from adagio.executors.signature import compute_input_signature
from adagio.remote_inputs import (
    LocalWriteError,
    RemoteArtifactStore,
    _HTTPSRedirectHandler,
    parse_artifact_url,
)

URL = "https://example.org/download?id=123"
PAYLOAD = b"archive bytes"
INFO = {"uuid": "original-artifact-uuid", "type": "FeatureTable[Frequency]"}


class Response(BytesIO):
    def __init__(self, payload=PAYLOAD, length=None, url=URL):
        super().__init__(payload)
        self.headers = {
            "Content-Length": str(len(payload) if length is None else length)
        }
        self.url = url

    def geturl(self):
        return self.url

    def read(self, size=-1):
        assert size > 0, "Downloads must stream bounded chunks"
        return super().read(size)


def network(monkeypatch, responses):
    calls = []

    def open(request, timeout):
        calls.append(request.full_url)
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(
        "adagio.remote_inputs.build_opener", lambda *args: SimpleNamespace(open=open)
    )
    monkeypatch.setattr("adagio.remote_inputs.time.sleep", lambda _: None)
    return calls


def validate(path):
    assert Path(path).read_bytes() == PAYLOAD
    return INFO


def test_shared_run_download_once_and_records_content(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response()]))
    store = RemoteArtifactStore(tmp_path)
    path, record = store.resolve(URL, validate=validate)
    assert Path(path).read_bytes() == PAYLOAD
    assert record["sha256"] == sha256(PAYLOAD).hexdigest()
    assert record["uuid"] == INFO["uuid"]
    assert record["url"] == "https://example.org/download"
    assert "id=123" not in json.dumps(record)
    assert store.resolve(URL, validate=validate) == (path, record)
    assert calls == [URL]
    assert not list(tmp_path.glob("*.partial"))


def test_concurrent_consumers_share_one_transfer(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response()]))
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda _: RemoteArtifactStore(tmp_path).resolve(URL, validate=validate),
                range(4),
            )
        )
    assert all(result == results[0] for result in results)
    assert len(calls) == 1


def test_new_run_fetches_changed_content(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response(), Response(b"changed")]))
    _, old = RemoteArtifactStore(tmp_path / "run1").resolve(
        URL, validate=lambda _: INFO
    )
    _, new = RemoteArtifactStore(tmp_path / "run2").resolve(
        URL, validate=lambda _: INFO
    )
    assert old["sha256"] != new["sha256"]
    assert len(calls) == 2

    def signature(record):
        return compute_input_signature(
            params={},
            inputs={"data": URL},
            env="env",
            remote_digests={record["source_id"]: "sha256:" + record["sha256"]},
        )

    assert signature(old) != signature(new)


def test_checksum_pin_and_conflicting_consumer(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response()]))
    store = RemoteArtifactStore(tmp_path)
    pin = sha256(PAYLOAD).hexdigest()
    store.resolve(URL + "#sha256=" + pin, validate=validate)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        store.resolve(URL + "#sha256=" + "0" * 64, validate=validate)
    assert len(calls) == 1


def test_failed_checksum_never_publishes(monkeypatch, tmp_path):
    network(monkeypatch, iter([Response()]))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        RemoteArtifactStore(tmp_path).resolve(
            URL + "#sha256=" + "0" * 64, validate=validate
        )
    assert not list(tmp_path.glob("*.qza"))
    assert not list(tmp_path.glob("*.json"))
    assert not list(tmp_path.glob("*.partial"))


def test_invalid_archive_never_publishes(monkeypatch, tmp_path):
    network(monkeypatch, iter([Response()]))

    def reject(_):
        raise ValueError("Not an archive")

    with pytest.raises(ValueError, match="Not an archive"):
        RemoteArtifactStore(tmp_path).resolve(URL, validate=reject)
    assert not list(tmp_path.glob("*.qza"))
    assert not list(tmp_path.glob("*.partial"))


def test_partial_download_retries_from_start(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response(b"partial", length=100), Response()]))
    path, _ = RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert Path(path).read_bytes() == PAYLOAD
    assert len(calls) == 2


@pytest.mark.parametrize("error", [TimeoutError(), IncompleteRead(b"")])
def test_retries_are_bounded_and_partial_files_removed(monkeypatch, tmp_path, error):
    calls = network(monkeypatch, iter([error] * 3))
    with pytest.raises(RuntimeError, match="Cannot download input"):
        RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert len(calls) == 3
    assert not list(tmp_path.glob("*.partial"))


class BrokenBody(Response):
    def read(self, size=-1):
        raise ssl.SSLError("record layer failure")


def test_tls_failure_mid_body_is_retried(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([BrokenBody(), Response()]))
    path, _ = RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert Path(path).read_bytes() == PAYLOAD
    assert len(calls) == 2


def test_insufficient_space_fails_before_transfer(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response()]))
    monkeypatch.setattr(
        "adagio.remote_inputs.shutil.disk_usage", lambda _: SimpleNamespace(free=1)
    )
    with pytest.raises(LocalWriteError, match="Not enough disk space") as exc:
        RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert "id=123" not in str(exc.value)
    assert len(calls) == 1
    assert not list(tmp_path.glob("*.partial"))


def test_disk_full_is_not_retried(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([Response()] * 3))
    real_open = Path.open

    class FullDisk:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.handle.close()

        def write(self, chunk):
            raise OSError(errno.ENOSPC, "No space left on device")

    def open_(self, mode="r", *args, **kwargs):
        handle = real_open(self, mode, *args, **kwargs)
        return FullDisk(handle) if mode == "wb" else handle

    monkeypatch.setattr(Path, "open", open_)
    with pytest.raises(LocalWriteError, match="No space left on device"):
        RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert len(calls) == 1
    assert not list(tmp_path.glob("*.partial"))


def test_http_404_does_not_retry_or_leak_query(monkeypatch, tmp_path):
    calls = network(monkeypatch, iter([HTTPError(URL, 404, "missing", {}, None)]))
    with pytest.raises(RuntimeError, match="HTTP 404") as exc:
        RemoteArtifactStore(tmp_path).resolve(URL, validate=validate)
    assert "id=123" not in str(exc.value)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/a",
        "ftp://example.org/a",
        "https://user:password@example.org/a",
        "https://example.org/a#wrong",
        "https://example.org/a#sha256=123",
    ],
)
def test_unsupported_sources_rejected(url):
    with pytest.raises(ValueError):
        parse_artifact_url(url)


def test_redirect_cannot_downgrade_to_http():
    with pytest.raises(ValueError, match="HTTPS"):
        _HTTPSRedirectHandler().redirect_request(
            None, None, 302, "", {}, "http://example.org/a"
        )


@pytest.fixture
def fake_qiime(monkeypatch):
    class Type:
        def __init__(self, text):
            self.text = text
            if text.startswith(("List[", "Collection[")):
                self.fields = [Type(text[text.index("[") + 1 : -1])]

        def __le__(self, other):
            return self.text == other.text

    artifact = SimpleNamespace(
        uuid=INFO["uuid"], type=INFO["type"], validate=lambda **kw: None,
        view=lambda kind: SimpleNamespace()
    )
    loads = []

    def load(path):
        loads.append(path)
        assert Path(path).read_bytes() == PAYLOAD
        return artifact

    qiime = ModuleType("qiime2")
    qiime.Artifact = SimpleNamespace(load=load)

    def load_metadata(path):
        if Path(path).read_bytes() != b"id\tbarcode-sequence\ns1\tACGT\n":
            raise ValueError("Invalid metadata TSV.")
        return SimpleNamespace()

    qiime.Metadata = SimpleNamespace(load=load_metadata)
    import zipfile
    is_zipfile = zipfile.is_zipfile
    monkeypatch.setattr(
        "adagio.cli.task_exec.zipfile.is_zipfile",
        lambda path: Path(path).read_bytes() == PAYLOAD or is_zipfile(path),
    )
    util = ModuleType("qiime2.sdk.util")
    util.parse_type = Type
    monkeypatch.setitem(sys.modules, "qiime2", qiime)
    monkeypatch.setitem(sys.modules, "qiime2.sdk.util", util)
    return loads


def task_spec(tmp_path):
    return {
        "archive_inputs": {"data": URL},
        "result_manifest": str(tmp_path / "results.json"),
        "remote_input_types": {"data": INFO["type"]},
    }


def test_worker_stages_scalar_collection_and_artifact_metadata(
    monkeypatch, tmp_path, fake_qiime
):
    calls = network(monkeypatch, iter([Response()]))
    spec = task_spec(tmp_path)
    spec["archive_collection_inputs"] = {"tables": [URL, "/local/file.qza"]}
    spec["metadata_inputs"] = {"metadata": URL}
    staged, records = _stage_remote_inputs(spec)
    assert (
        staged["archive_inputs"]["data"]
        == staged["archive_collection_inputs"]["tables"][0]
    )
    assert staged["archive_collection_inputs"]["tables"][1] == "/local/file.qza"
    assert staged["metadata_inputs"]["metadata"] == staged["archive_inputs"]["data"]
    assert len(records) == 3
    assert len(calls) == 1
    assert spec["archive_inputs"]["data"] == URL  # No mutation of the plan.
    assert len(fake_qiime) == 3


def test_worker_accepts_remote_metadata_and_preserves_bytes(monkeypatch, tmp_path, fake_qiime):
    payload = b"id\tbarcode-sequence\ns1\tACGT\n"
    calls = network(monkeypatch, iter([Response(payload=payload)]))
    spec = task_spec(tmp_path)
    spec["archive_inputs"] = {}
    spec["metadata_inputs"] = {"barcodes": URL}
    staged, records = _stage_remote_inputs(spec)
    path = Path(staged["metadata_inputs"]["barcodes"])
    assert path.suffix == ".tsv"
    assert path.read_bytes() == payload
    assert records[0]["type"] == "Metadata"
    assert records[0]["sha256"] == sha256(payload).hexdigest()
    assert "uuid" not in records[0]
    assert _stage_remote_inputs(spec) == (staged, records)
    assert len(calls) == 1
    # Sharing the URL cache cannot make TSV data valid for an artifact input.
    with pytest.raises(ValueError, match="QIIME .qza artifact"):
        _stage_remote_inputs(task_spec(tmp_path))


def test_worker_rejects_invalid_remote_metadata(monkeypatch, tmp_path, fake_qiime):
    network(monkeypatch, iter([Response(payload=b"<html>Not metadata</html>")]))
    spec = task_spec(tmp_path)
    spec["archive_inputs"] = {}
    spec["metadata_inputs"] = {"barcodes": URL}
    with pytest.raises(ValueError, match="metadata TSV") as error:
        _stage_remote_inputs(spec)
    assert "barcodes" in str(error.value)
    assert "Invalid metadata TSV" in str(error.value)
    assert not list((tmp_path / ".adagio-downloads").glob("*.tsv"))


def test_worker_rejects_declared_type_mismatch(monkeypatch, tmp_path, fake_qiime):
    network(monkeypatch, iter([Response()]))
    spec = task_spec(tmp_path)
    spec["remote_input_types"]["data"] = "FeatureData[Sequence]"
    with pytest.raises(ValueError, match="expected FeatureData"):
        _stage_remote_inputs(spec)


def test_worker_accepts_collection_member_type(monkeypatch, tmp_path, fake_qiime):
    network(monkeypatch, iter([Response()]))
    spec = task_spec(tmp_path)
    spec["remote_input_types"]["data"] = "Collection[FeatureTable[Frequency]]"
    spec["archive_collection_inputs"] = {"data": [URL]}
    spec["archive_inputs"] = {}
    _stage_remote_inputs(spec)


def test_raw_url_rejected_before_network_or_qiime(tmp_path):
    spec = task_spec(tmp_path)
    spec["archive_input_materializations"] = {"data": {"mode": "raw"}}
    with pytest.raises(ValueError, match="not raw imports"):
        _stage_remote_inputs(spec)


def test_local_inputs_do_not_need_manifest_or_qiime():
    staged, downloads = _stage_remote_inputs(
        {"archive_inputs": {"data": "/local/file.qza"}}
    )
    assert staged["archive_inputs"]["data"] == "/local/file.qza"
    assert not downloads


@pytest.mark.parametrize("from_file", [False, True])
def test_cli_url_reaches_executor_unchanged(monkeypatch, tmp_path, from_file):
    from adagio.cli.main import main
    from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionResult
    from adagio.executors.task_environments import TaskEnvironmentExecutor

    ast = {
        "type": "expression",
        "builtin": False,
        "name": "FeatureTable",
        "predicate": None,
        "fields": [],
    }
    pipeline = {
        "type": "pipeline",
        "signature": {
            "inputs": [
                {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": "table",
                    "type": INFO["type"],
                    "ast": ast,
                    "required": True,
                }
            ],
            "parameters": [],
            "outputs": [],
        },
        "graph": [
            {
                "id": "task",
                "kind": "plugin-action",
                "plugin": "feature_table",
                "action": "summarize",
                "inputs": {
                    "table": {
                        "kind": "archive",
                        "id": "00000000-0000-0000-0000-000000000001",
                    }
                },
                "outputs": {},
                "parameters": {},
            }
        ],
    }
    path = tmp_path / "pipeline.adg"
    path.write_text(json.dumps(pipeline))
    requests = []

    def launch(*, environment, request, console=None, **kwargs):
        requests.append(request)
        return TaskExecutionResult(outputs={})

    executor = TaskEnvironmentExecutor(
        environment_resolver=SimpleNamespace(
            resolve=lambda **kwargs: TaskEnvironmentSpec(kind="test", reference="test")
        ),
        launchers={"test": SimpleNamespace(launch=launch)},
    )
    monkeypatch.setattr(
        "adagio.executors.select_default_executor", lambda **kw: executor
    )
    # Input resolution on the orchestration host must perform no network I/O.
    monkeypatch.setattr(
        "adagio.remote_inputs.build_opener",
        lambda *args: pytest.fail("Host attempted download"),
    )
    monkeypatch.chdir(tmp_path)
    argv = ["run", str(path), "--cache-dir", str(tmp_path / "cache")]
    if from_file:
        arguments = tmp_path / "args.json"
        arguments.write_text(json.dumps({"inputs": {"table": URL}}))
        argv += ["--arguments", str(arguments)]
    else:
        argv += ["--input-table", URL]
    with pytest.raises(SystemExit) as exit_status:
        main(argv)
    assert exit_status.value.code == 0
    assert requests[0].archive_inputs == {"table": URL}
    assert requests[0].remote_input_types == {"table": INFO["type"]}
