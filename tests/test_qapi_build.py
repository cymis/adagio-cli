import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rich.console import Console

from adagio.cli import qapi as qapi_cli
from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import ConvertToMetadataTask
from adagio.qapi.build import (
    CONVERT_TO_METADATA_ACTION_ID,
    CONVERT_TO_METADATA_ACTION_NAME,
    _build_convert_to_metadata_action,
    _iter_public_qiime_actions,
    build_plugin_metadata,
)


class QapiBuildTests(unittest.TestCase):
    def test_plugin_metadata_prefers_short_description_and_humanizes_name(self) -> None:
        metadata = build_plugin_metadata(
            SimpleNamespace(
                name="mystery-stew",
                version="0.8.0.dev12",
                short_description="Experimental actions.",
                description="Long description.",
            ),
            "mystery_stew",
        )

        self.assertEqual(
            metadata,
            {
                "display_name": "Mystery Stew",
                "description": "Experimental actions.",
                "version": "0.8.0.dev12",
            },
        )

    def test_list_qapi_plugins_writes_json_listing(self) -> None:
        payload = {
            "qiime_version": "2026.1.0",
            "plugins": [
                {
                    "name": "mystery_stew",
                    "display_name": "Mystery Stew",
                    "description": "Experimental actions.",
                    "version": "0.8.0.dev12",
                    "action_count": 8,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "plugins.json"
            with patch(
                "adagio.cli.qapi.generate_qapi_plugin_index",
                return_value=payload,
            ):
                qapi_cli.list_qapi_plugins(output=output)

            self.assertEqual(json.loads(output.read_text()), payload)

    def test_build_qapi_no_submit_writes_payload_without_client_call(self) -> None:
        payload = {
            "qiime_version": "2026.1.0",
            "schema_version": "0.1.0",
            "data": {"example": {"methods": {}}},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "qapi.json"
            with (
                patch(
                    "adagio.cli.qapi.generate_qapi_payload",
                    return_value=payload,
                ),
                patch("adagio.cli.qapi.submit_qapi_payload") as submit_mock,
            ):
                qapi_cli.build_qapi(output=output, no_submit=True)

            self.assertEqual(json.loads(output.read_text()), payload)
            submit_mock.assert_not_called()

    def test_build_qapi_persists_an_explicit_conda_default(self) -> None:
        payload = {
            "qiime_version": "2026.1.0",
            "schema_version": "0.1.0",
            "data": {
                "example": {"methods": {}},
                "second": {"methods": {}},
            },
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "qapi.json"
            with patch(
                "adagio.cli.qapi.generate_qapi_payload",
                return_value=payload,
            ):
                qapi_cli.build_qapi(
                    output=output,
                    no_submit=True,
                    default_conda_prefix=Path("/opt/conda/envs/q2-example"),
                )

            generated = json.loads(output.read_text())
            for plugin in generated["data"].values():
                self.assertEqual(
                    plugin["default_environment"],
                    {
                        "kind": "conda",
                        "prefix": "/opt/conda/envs/q2-example",
                    },
                )

    def test_build_qapi_rejects_ambiguous_or_relative_defaults(self) -> None:
        with self.assertRaisesRegex(SystemExit, "either"):
            qapi_cli._add_default_environment(
                {"data": {"example": {"methods": {}}}},
                default_docker_image="example:latest",
                default_conda_prefix=Path("/opt/conda/envs/example"),
            )

        with self.assertRaisesRegex(SystemExit, "absolute path"):
            qapi_cli._add_default_environment(
                {"data": {"example": {"methods": {}}}},
                default_docker_image=None,
                default_conda_prefix=Path("relative/env"),
            )

    def test_iter_public_qiime_actions_skips_private_action_names(self) -> None:
        public_action = SimpleNamespace(id="public_action")
        skipped_actions: list[str] = []

        actions = {
            "public_action": public_action,
            "_private_by_key": SimpleNamespace(id="private_by_key"),
            "-private_by_key": SimpleNamespace(id="private_by_key"),
            "private_by_id": SimpleNamespace(id="_private_by_id"),
            "private_by_hyphen_id": SimpleNamespace(id="-private_by_hyphen_id"),
        }

        public_actions = list(
            _iter_public_qiime_actions(
                actions,
                plugin_name="example",
                on_skipped_private_action=skipped_actions.append,
            )
        )

        self.assertEqual(public_actions, [("public_action", public_action)])
        self.assertEqual(
            skipped_actions,
            [
                "example._private_by_key",
                "example.-private_by_key",
                "example._private_by_id",
                "example.-private_by_hyphen_id",
            ],
        )

    def test_build_qapi_submits_payload_after_private_actions_are_skipped(self) -> None:
        output = io.StringIO()
        original_console = qapi_cli.console
        qapi_cli.console = Console(file=output, force_terminal=False, color_system=None)

        def fake_generate_qapi_payload(*, on_skipped_private_action, **kwargs):
            on_skipped_private_action("example._private_action")
            return {
                "qiime_version": "2024.10.0",
                "schema_version": "0.1.0",
                "data": {
                    "example": {
                        "methods": {
                            "public_action": {
                                "id": "public_action",
                            },
                        },
                    },
                },
            }

        try:
            with (
                patch(
                    "adagio.cli.qapi.generate_qapi_payload",
                    side_effect=fake_generate_qapi_payload,
                ),
                patch("adagio.cli.qapi.submit_qapi_payload") as submit_mock,
            ):
                submit_mock.return_value = (
                    "https://adagiodata.com/api/v1/qapi/",
                    200,
                    {"message": "ok"},
                )

                qapi_cli.build_qapi(action_url="https://adagiodata.com/api/v1")
        finally:
            qapi_cli.console = original_console

        submit_mock.assert_called_once()
        submitted_payload = submit_mock.call_args.args[0]
        self.assertEqual(
            submitted_payload["data"]["example"]["methods"],
            {"public_action": {"id": "public_action"}},
        )
        self.assertIn("Skipped 1 private QIIME action", output.getvalue())
        self.assertIn("example._private_action", output.getvalue())

    def test_build_convert_to_metadata_action_uses_union_input(self) -> None:
        feature_table_ast = {
            "name": "FeatureTable",
            "type": "expression",
            "fields": [
                {
                    "name": "Frequency",
                    "type": "expression",
                    "fields": [],
                    "builtin": False,
                    "predicate": None,
                }
            ],
            "builtin": False,
            "predicate": None,
        }
        alpha_ast = {
            "name": "SampleData",
            "type": "expression",
            "fields": [
                {
                    "name": "AlphaDiversity",
                    "type": "expression",
                    "fields": [],
                    "builtin": False,
                    "predicate": None,
                }
            ],
            "builtin": False,
            "predicate": None,
        }

        action = _build_convert_to_metadata_action(
            [
                ("SampleData[AlphaDiversity]", alpha_ast),
                ("FeatureTable[Frequency]", feature_table_ast),
            ]
        )

        self.assertIsNotNone(action)
        self.assertEqual(action["id"], CONVERT_TO_METADATA_ACTION_ID)
        self.assertEqual(action["name"], CONVERT_TO_METADATA_ACTION_NAME)
        self.assertEqual(action["inputs"][0]["name"], "data")
        self.assertEqual(
            action["inputs"][0]["type"],
            "FeatureTable[Frequency] | SampleData[AlphaDiversity]",
        )
        self.assertEqual(action["inputs"][0]["ast"]["type"], "union")
        self.assertEqual(action["outputs"][0]["type"], "Metadata")

    def test_convert_to_metadata_task_aliases_input_to_output(self) -> None:
        pipeline = AdagioPipeline.model_validate(
            {
                "type": "pipeline",
                "signature": {
                    "inputs": [
                        {
                            "id": "input-data",
                            "name": "data",
                            "type": "FeatureTable[Frequency]",
                            "ast": {
                                "name": "FeatureTable",
                                "type": "expression",
                                "fields": [],
                                "builtin": False,
                                "predicate": None,
                            },
                            "required": True,
                            "description": None,
                        }
                    ],
                    "parameters": [],
                    "outputs": [],
                },
                "graph": [
                    {
                        "id": "convert-node",
                        "kind": "built-in",
                        "name": CONVERT_TO_METADATA_ACTION_NAME,
                        "inputs": {"data": {"kind": "archive", "id": "input-data"}},
                        "parameters": {},
                        "outputs": {
                            "metadata": {"kind": "archive", "id": "metadata-output"}
                        },
                    }
                ],
            }
        )

        task = pipeline.graph[0]
        self.assertIsInstance(task, ConvertToMetadataTask)
        scope = {"input-data": "/tmp/table.qza"}
        task.exec(ctx=None, params={}, scope=scope)
        self.assertEqual(scope["metadata-output"], "/tmp/table.qza")


if __name__ == "__main__":
    unittest.main()
