import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from rich.console import Console

from adagio.cli import qapi as qapi_cli
from adagio.qapi.build import _iter_public_qiime_actions


class QapiBuildTests(unittest.TestCase):
    def test_iter_public_qiime_actions_skips_private_action_names(self) -> None:
        public_action = SimpleNamespace(id="public_action")
        skipped_actions: list[str] = []

        actions = {
            "public_action": public_action,
            "-private_by_key": SimpleNamespace(id="private_by_key"),
            "private_by_id": SimpleNamespace(id="-private_by_id"),
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
            ["example.-private_by_key", "example.-private_by_id"],
        )

    def test_build_qapi_submits_payload_after_private_actions_are_skipped(self) -> None:
        output = io.StringIO()
        original_console = qapi_cli.console
        qapi_cli.console = Console(file=output, force_terminal=False, color_system=None)

        def fake_generate_qapi_payload(*, on_skipped_private_action, **kwargs):
            on_skipped_private_action("example.-private_action")
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
        self.assertIn("example.-private_action", output.getvalue())


if __name__ == "__main__":
    unittest.main()
