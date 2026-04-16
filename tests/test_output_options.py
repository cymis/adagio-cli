import typing
import unittest

from adagio.app.parsers.pipeline import Input, Output, Parameter, parse_outputs
from adagio.cli.args import ShowParamsMode
from adagio.cli.dynamic import build_dynamic_run
from adagio.cli.main import _filter_visible_specs
from adagio.cli.runner import _apply_output_overrides


class OutputOptionTests(unittest.TestCase):
    def test_parse_outputs_preserves_descriptions(self) -> None:
        data = {
            "signature": {
                "inputs": [],
                "parameters": [],
                "outputs": [
                    {
                        "id": "00000000-0000-0000-0000-000000000001",
                        "name": "table",
                        "type": "FeatureTable[Frequency]",
                        "description": "Denoised feature table.",
                    }
                ],
            }
        }

        outputs = parse_outputs(data)

        self.assertEqual(outputs[0].name, "table")
        self.assertEqual(outputs[0].description, "Denoised feature table.")

    def test_dynamic_run_adds_output_dir_and_per_output_options(self) -> None:
        dynamic_run = build_dynamic_run(
            input_specs=[],
            param_specs=[],
            output_specs=[
                Output(
                    id="00000000-0000-0000-0000-000000000001",
                    name="table",
                    type="FeatureTable[Frequency]",
                    description="Denoised feature table.",
                )
            ],
            run_handler=lambda *args, **kwargs: None,
        )

        self.assertIn("output_dir", dynamic_run.__signature__.parameters)
        self.assertIn("output_table", dynamic_run.__signature__.parameters)

        output_dir_annotation = dynamic_run.__signature__.parameters["output_dir"].annotation
        output_annotation = dynamic_run.__signature__.parameters["output_table"].annotation
        output_dir_help = typing.get_args(output_dir_annotation)[1].help
        output_help = typing.get_args(output_annotation)[1].help

        self.assertEqual(output_dir_help, "Directory for all pipeline outputs.")
        self.assertIn("Denoised feature table.", output_help)
        self.assertIn("Overrides --output-dir", output_help)

    def test_output_dir_is_a_command_option_and_required_pipeline_options_are_first(
        self,
    ) -> None:
        dynamic_run = build_dynamic_run(
            input_specs=[
                Input(
                    id="00000000-0000-0000-0000-000000000001",
                    name="tree",
                    required=False,
                    type="Phylogeny[Rooted]",
                    description="Optional tree.",
                ),
                Input(
                    id="00000000-0000-0000-0000-000000000002",
                    name="seqs",
                    required=True,
                    type="SampleData[Sequences]",
                    description="Required sequences.",
                ),
            ],
            param_specs=[
                Parameter(
                    id="00000000-0000-0000-0000-000000000003",
                    name="threads",
                    required=False,
                    default=1,
                    type="Int",
                    description="Optional thread count.",
                ),
                Parameter(
                    id="00000000-0000-0000-0000-000000000004",
                    name="metric",
                    required=True,
                    default=None,
                    type="Str",
                    description="Required metric.",
                ),
            ],
            output_specs=[
                Output(
                    id="00000000-0000-0000-0000-000000000005",
                    name="table",
                    type="FeatureTable[Frequency]",
                    description="Output table.",
                )
            ],
            run_handler=lambda *args, **kwargs: None,
        )

        output_dir_annotation = dynamic_run.__signature__.parameters["output_dir"].annotation
        output_dir_group = typing.get_args(output_dir_annotation)[1].group

        self.assertEqual(output_dir_group[0]._name, "Command Options")
        self.assertEqual(
            list(dynamic_run.__signature__.parameters)[:7],
            [
                "pipeline",
                "cache_dir",
                "arguments_file",
                "show_params",
                "config_file",
                "reuse",
                "output_dir",
            ],
        )
        self.assertEqual(
            list(dynamic_run.__signature__.parameters)[7:],
            [
                "input_seqs",
                "param_metric",
                "input_tree",
                "param_threads",
                "output_table",
            ],
        )

    def test_outputs_are_only_visible_in_all_mode(self) -> None:
        output_specs = [
            Output(
                id="00000000-0000-0000-0000-000000000005",
                name="table",
                type="FeatureTable[Frequency]",
                description="Output table.",
            )
        ]

        _, _, required_outputs = _filter_visible_specs(
            input_specs=[],
            param_specs=[],
            output_specs=output_specs,
            show_mode=ShowParamsMode.REQUIRED,
            arguments_data=None,
        )
        _, _, missing_outputs = _filter_visible_specs(
            input_specs=[],
            param_specs=[],
            output_specs=output_specs,
            show_mode=ShowParamsMode.MISSING,
            arguments_data=None,
        )
        _, _, all_outputs = _filter_visible_specs(
            input_specs=[],
            param_specs=[],
            output_specs=output_specs,
            show_mode=ShowParamsMode.ALL,
            arguments_data=None,
        )

        self.assertEqual(required_outputs, [])
        self.assertEqual(missing_outputs, [])
        self.assertEqual(all_outputs, output_specs)

    def test_output_dir_override_applies_to_all_outputs(self) -> None:
        resolved = _apply_output_overrides(
            outputs={"table": "/tmp/from-file/table.qza", "stats": "/tmp/from-file/stats.qza"},
            output_names=["table", "stats"],
            output_dir="/tmp/all-outputs",
            output_overrides={"stats": "/tmp/custom/stats.qza"},
        )

        self.assertEqual(
            resolved,
            {
                "table": "/tmp/all-outputs/table",
                "stats": "/tmp/custom/stats.qza",
            },
        )

    def test_per_output_override_merges_with_shared_directory_outputs(self) -> None:
        resolved = _apply_output_overrides(
            outputs="/tmp/from-arguments-dir",
            output_names=["table", "stats"],
            output_dir=None,
            output_overrides={"stats": "/tmp/custom/stats.qza"},
        )

        self.assertEqual(
            resolved,
            {
                "table": "/tmp/from-arguments-dir/table",
                "stats": "/tmp/custom/stats.qza",
            },
        )
