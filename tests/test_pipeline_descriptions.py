import typing
import unittest

from adagio.app.parsers.pipeline import Input, Parameter, parse_inputs, parse_parameters
from adagio.cli.dynamic import (
    _compact_type_text,
    _display_type_label,
    _pipeline_type_label,
    _wrap_type_label,
    build_dynamic_run,
)
from adagio.model.pipeline import AdagioPipeline


class PipelineDescriptionTests(unittest.TestCase):
    def test_pipeline_model_accepts_signature_descriptions(self) -> None:
        ast = {
            "type": "expression",
            "builtin": True,
            "name": "Str",
            "predicate": None,
            "fields": [],
        }
        pipeline = AdagioPipeline.model_validate(
            {
                "type": "pipeline",
                "signature": {
                    "inputs": [
                        {
                            "id": "input-1",
                            "name": "table",
                            "type": "FeatureTable[Frequency]",
                            "ast": ast,
                            "required": True,
                            "description": "Input table.",
                        }
                    ],
                    "parameters": [
                        {
                            "id": "param-1",
                            "name": "trunc_len",
                            "type": "Int",
                            "ast": ast,
                            "required": False,
                            "default": 120,
                            "description": "Trim reads to this length.",
                        }
                    ],
                    "outputs": [
                        {
                            "id": "output-1",
                            "name": "table",
                            "type": "FeatureTable[Frequency]",
                            "ast": ast,
                            "description": "Denoised table.",
                        }
                    ],
                },
                "graph": [],
            }
        )

        self.assertEqual(pipeline.signature.inputs[0].description, "Input table.")
        self.assertEqual(
            pipeline.signature.parameters[0].description,
            "Trim reads to this length.",
        )
        self.assertEqual(pipeline.signature.outputs[0].description, "Denoised table.")

    def test_pipeline_parsers_preserve_descriptions(self) -> None:
        data = {
            "signature": {
                "inputs": [
                    {
                        "id": "00000000-0000-0000-0000-000000000001",
                        "name": "table",
                        "required": True,
                        "type": "FeatureTable[Frequency]",
                        "description": "Input table.",
                    }
                ],
                "parameters": [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "name": "trunc_len",
                        "required": False,
                        "default": 120,
                        "type": "Int",
                        "description": "Trim reads to this length.",
                    }
                ],
                "outputs": [],
            }
        }

        self.assertEqual(parse_inputs(data)[0].description, "Input table.")
        self.assertEqual(
            parse_parameters(data)[0].description, "Trim reads to this length."
        )

    def test_dynamic_run_help_includes_descriptions(self) -> None:
        dynamic_run = build_dynamic_run(
            input_specs=[
                Input(
                    id="00000000-0000-0000-0000-000000000001",
                    name="table",
                    required=True,
                    type="FeatureTable[Frequency]",
                    description="Input table.",
                )
            ],
            param_specs=[
                Parameter(
                    id="00000000-0000-0000-0000-000000000002",
                    name="trunc_len",
                    required=False,
                    default=120,
                    type="Int",
                    description="Trim reads to this length.",
                )
            ],
            output_specs=[],
            run_handler=lambda *args, **kwargs: None,
        )

        input_annotation = dynamic_run.__signature__.parameters["input_table"].annotation
        param_annotation = dynamic_run.__signature__.parameters["param_trunc_len"].annotation
        input_help = typing.get_args(input_annotation)[1].help
        param_help = typing.get_args(param_annotation)[1].help

        self.assertIsInstance(input_help, str)
        self.assertIsInstance(param_help, str)
        self.assertIn("Input table.", input_help)
        self.assertIn("Trim reads to this length.", param_help)
        self.assertNotIn("Pipeline input:", input_help)
        self.assertNotIn("Pipeline parameter:", param_help)

    def test_choices_are_rendered_compactly(self) -> None:
        compact = _compact_type_text(
            "Str % Choices('ace', 'berger_parker_d', 'brillouin_d')"
        )
        self.assertEqual(compact, "[ace|berger_parker_d|brillouin_d]")

        compact_unquoted = _compact_type_text(
            "Str % Choices(ace, berger_parker_d, brillouin_d)"
        )
        self.assertEqual(compact_unquoted, "[ace|berger_parker_d|brillouin_d]")

    def test_long_choice_labels_wrap_on_pipes(self) -> None:
        wrapped = _wrap_type_label(
            "[ace|berger_parker_d|brillouin_d|chao1|dominance]", 22
        )
        self.assertIn("\n", wrapped)
        self.assertTrue(wrapped.startswith("["))
        self.assertTrue(wrapped.endswith("]"))
        self.assertIn("\n |", wrapped)

    def test_pipeline_type_labels_use_general_cli_types(self) -> None:
        self.assertEqual(_pipeline_type_label(int), "INTEGER")
        self.assertEqual(_pipeline_type_label(float), "NUMBER")
        self.assertEqual(_pipeline_type_label(bool), "BOOLEAN")
        self.assertEqual(_pipeline_type_label(str | None), "TEXT")

    def test_display_type_label_prefers_choices_and_path(self) -> None:
        self.assertEqual(
            _display_type_label(
                spec_type="FeatureTable[Frequency]", type_hint=str, is_input=True
            ),
            "PATH",
        )
        self.assertEqual(
            _display_type_label(
                spec_type="Str % Choices(ace, berger_parker_d, brillouin_d)",
                type_hint=str,
                is_input=False,
            ),
            "[ace|berger_parker_d|brillouin_d]",
        )
        self.assertEqual(
            _display_type_label(spec_type="Int", type_hint=int, is_input=False),
            "INTEGER",
        )
