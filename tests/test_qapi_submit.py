import json
import os
import unittest
from unittest.mock import patch

from adagio.cli import qapi as qapi_cli
from adagio.qapi.client import submit_qapi_payload


class _FakeResponse:
    def __init__(self, body: object = None, status: int = 200) -> None:
        self.status = status
        self._body = "" if body is None else json.dumps(body)

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._body.encode("utf-8")


class QapiSubmitTests(unittest.TestCase):
    def test_submit_qapi_payload_adds_bearer_token_header(self) -> None:
        seen_headers: dict[str, str | None] = {}

        def fake_urlopen(request, timeout=60):
            seen_headers["authorization"] = request.get_header("Authorization")
            seen_headers["content_type"] = request.get_header("Content-type")
            return _FakeResponse({"message": "ok"})

        with patch("adagio.qapi.client.urlopen", side_effect=fake_urlopen):
            url, status, response = submit_qapi_payload(
                {"qiime_version": "2024.10.0", "schema_version": "0.1.0", "data": {"dada2": {"methods": {}}}},
                action_url="https://adagiodata.com/api/v1",
                submission_token="token-123",
            )

        self.assertEqual(url, "https://adagiodata.com/api/v1/qapi/")
        self.assertEqual(status, 200)
        self.assertEqual(response, {"message": "ok"})
        self.assertEqual(seen_headers["authorization"], "Bearer token-123")
        self.assertEqual(seen_headers["content_type"], "application/json")

    def test_submit_qapi_payload_reads_submission_token_from_env(self) -> None:
        seen_authorization: dict[str, str | None] = {}

        def fake_urlopen(request, timeout=60):
            seen_authorization["value"] = request.get_header("Authorization")
            return _FakeResponse({"message": "ok"})

        with patch.dict(os.environ, {"QAPI_SUBMISSION_TOKEN": "env-token"}, clear=False):
            with patch("adagio.qapi.client.urlopen", side_effect=fake_urlopen):
                submit_qapi_payload(
                    {
                        "qiime_version": "2024.10.0",
                        "schema_version": "0.1.0",
                        "data": {"feature-table": {"methods": {}}},
                    },
                    action_url="https://adagiodata.com/api/v1",
                )

        self.assertEqual(seen_authorization["value"], "Bearer env-token")

    def test_build_qapi_passes_submission_token_to_client(self) -> None:
        with patch(
            "adagio.cli.qapi.generate_qapi_payload",
            return_value={
                "qiime_version": "2024.10.0",
                "schema_version": "0.1.0",
                "data": {"dada2": {"methods": {}}},
            },
        ), patch("adagio.cli.qapi.submit_qapi_payload") as submit_mock:
            submit_mock.return_value = (
                "https://adagiodata.com/api/v1/qapi/",
                200,
                {"message": "ok"},
            )

            qapi_cli.build_qapi(
                action_url="https://adagiodata.com/api/v1",
                submission_token="token-456",
            )

        submit_mock.assert_called_once()
        self.assertEqual(submit_mock.call_args.kwargs["submission_token"], "token-456")
