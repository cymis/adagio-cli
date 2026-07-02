import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from rich.console import Console

from adagio.describe import render_pipeline_text
from adagio.model.pipeline import AdagioPipeline


AST = {
    "type": "expression",
    "builtin": True,
    "name": "Str",
    "predicate": None,
    "fields": [],
}


def _sample_pipeline_dict() -> dict:
    return {
        "type": "pipeline",
        "signature": {
            "inputs": [
                {
                    "id": "input-seqs",
                    "name": "seqs",
                    "type": "SampleData[SequencesWithQuality]",
                    "ast": AST,
                    "required": True,
                    "description": "Demultiplexed sequence data.",
                },
                {
                    "id": "input-barcodes",
                    "name": "barcodes",
                    "type": "MetadataColumn[Categorical]",
                    "ast": AST,
                    "required": True,
                    "description": "Barcode metadata column.",
                },
            ],
            "parameters": [
                {
                    "id": "param-barcodes",
                    "name": "barcodes",
                    "type": "MetadataColumn[Categorical]",
                    "ast": AST,
                    "required": True,
                    "description": "Column used to find barcode values.",
                },
                {
                    "id": "param-trim-left",
                    "name": "trim_left",
                    "type": "Int",
                    "ast": AST,
                    "required": True,
                    "description": "Trim this many bases from the start of each read.",
                },
            ],
            "outputs": [
                {
                    "id": "output-table",
                    "name": "table",
                    "type": "FeatureTable[Frequency]",
                    "ast": AST,
                    "description": "Denoised feature table.",
                },
                {
                    "id": "output-demux",
                    "name": "per_sample_sequences",
                    "type": "SampleData[SequencesWithQuality]",
                    "ast": AST,
                    "description": "Per-sample demultiplexed sequences.",
                }
            ],
        },
        "graph": [
            {
                "id": "task-dada2",
                "kind": "plugin-action",
                "plugin": "dada2",
                "action": "denoise_single",
                "inputs": {
                    "demultiplexed_seqs": {"kind": "archive", "id": "output-demux"}
                },
                "parameters": {
                    "trim_left": {"kind": "promoted", "id": "param-trim-left"}
                },
                "outputs": {
                    "table": {"kind": "archive", "id": "output-table"}
                },
            },
            {
                "id": "task-demux",
                "kind": "plugin-action",
                "plugin": "demux",
                "action": "emp_single",
                "inputs": {
                    "seqs": {"kind": "archive", "id": "input-seqs"},
                    "barcodes": {"kind": "metadata", "id": "input-barcodes"},
                },
                "parameters": {
                    "barcodes": {
                        "kind": "metadata",
                        "column": {"kind": "promoted", "id": "param-barcodes"},
                    }
                },
                "outputs": {
                    "per_sample_sequences": {
                        "kind": "archive",
                        "id": "output-demux",
                    }
                },
            },
        ],
    }


def _collection_pipeline_dict() -> dict:
    return {
        "type": "pipeline",
        "signature": {
            "inputs": [
                {
                    "id": "input-table-a",
                    "name": "table_a",
                    "type": "FeatureTable[Frequency]",
                    "ast": AST,
                    "required": True,
                    "description": "First table.",
                },
                {
                    "id": "input-table-b",
                    "name": "table_b",
                    "type": "FeatureTable[Frequency]",
                    "ast": AST,
                    "required": True,
                    "description": "Second table.",
                },
            ],
            "parameters": [],
            "outputs": [],
        },
        "graph": [
            {
                "id": "task-merge",
                "kind": "plugin-action",
                "plugin": "feature_table",
                "action": "merge",
                "inputs": {
                    "tables": {
                        "kind": "archive-collection",
                        "style": "list",
                        "items": [
                            {"key": "0", "id": "input-table-a"},
                            {"key": "1", "id": "input-table-b"},
                        ],
                    }
                },
                "parameters": {},
                "outputs": {},
            }
        ],
    }


def _user_description_pipeline_dict() -> dict:
    return {
        "type": "pipeline",
        "signature": {
            "inputs": [
                {
                    "id": "input-seqs",
                    "name": "seqs",
                    "type": "SampleData[SequencesWithQuality]",
                    "ast": AST,
                    "required": True,
                    "description": "Demultiplexed sequence data.",
                    "user_description": "Raw reads from sequencing run 3",
                },
            ],
            "parameters": [],
            "outputs": [
                {
                    "id": "output-table",
                    "name": "table",
                    "type": "FeatureTable[Frequency]",
                    "ast": AST,
                    "description": "Denoised feature table.",
                    "user_description": "Denoised table we publish",
                },
            ],
        },
        "graph": [
            {
                "id": "task-dada2",
                "kind": "plugin-action",
                "plugin": "dada2",
                "action": "denoise_single",
                "user_description": "Denoise tuned for our amplicon",
                "inputs": {
                    "demultiplexed_seqs": {"kind": "archive", "id": "input-seqs"}
                },
                "parameters": {},
                "outputs": {"table": {"kind": "archive", "id": "output-table"}},
            },
        ],
    }


def _render_plain(renderable: object) -> str:
    console = Console(record=True, width=160, file=io.StringIO())
    console.print(renderable, soft_wrap=True)
    return console.export_text()


class PipelineShowTests(unittest.TestCase):
    def test_render_pipeline_text_uses_dependency_order_and_resolves_bindings(
        self,
    ) -> None:
        pipeline = AdagioPipeline.model_validate(_sample_pipeline_dict())

        rendered = _render_plain(render_pipeline_text(pipeline))

        self.assertLess(rendered.index("demux.emp_single"), rendered.index("dada2.denoise_single"))
        self.assertNotIn('Plugin: demux', rendered)
        self.assertNotIn('Action: emp_single', rendered)
        self.assertIn("╭─ demux.emp_single ", rendered)
        self.assertIn('seqs: (SampleData[SequencesWithQuality]) pipeline input "seqs"', rendered)
        self.assertIn('Demultiplexed sequence data.', rendered)
        self.assertIn('barcodes: (MetadataColumn[Categorical]) pipeline input "barcodes"', rendered)
        self.assertIn('Barcode metadata column.', rendered)
        self.assertIn(
            'barcodes: (MetadataColumn[Categorical]) metadata column from pipeline input "barcodes" using pipeline parameter "barcodes"',
            rendered,
        )
        self.assertIn('Column used to find barcode values.', rendered)
        self.assertIn(
            'demultiplexed_seqs: (SampleData[SequencesWithQuality]) demux.emp_single.per_sample_sequences',
            rendered,
        )
        self.assertIn('Per-sample demultiplexed sequences.', rendered)
        self.assertIn('trim_left: (Int) pipeline parameter "trim_left"', rendered)
        self.assertIn('Trim this many bases from the start of each read.', rendered)
        self.assertIn('table (FeatureTable[Frequency])', rendered)
        self.assertIn('Denoised feature table.', rendered)

    def test_pipeline_show_cli_prints_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_path = Path(tmpdir) / "pipeline.json"
            payload = {"spec": _sample_pipeline_dict()}
            pipeline_path.write_text(json.dumps(payload), encoding="utf-8")

            result = subprocess.run(
                [sys.executable, "-m", "adagio.cli.main", "pipeline", "show", str(pipeline_path)],
                capture_output=True,
                check=False,
                text=True,
            )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("demux.emp_single", result.stdout)
        self.assertIn("dada2.denoise_single", result.stdout)
        self.assertIn("Inputs:", result.stdout)
        self.assertIn('barcodes: (MetadataColumn[Categorical]) pipeline input "barcodes"', result.stdout)
        self.assertIn('table (FeatureTable[Frequency])', result.stdout)

    def test_render_pipeline_text_displays_collection_inputs(self) -> None:
        pipeline = AdagioPipeline.model_validate(_collection_pipeline_dict())

        rendered = _render_plain(render_pipeline_text(pipeline))

        self.assertIn("feature_table.merge", rendered)
        self.assertIn(
            'tables: list [pipeline input "table_a", pipeline input "table_b"]',
            rendered,
        )

    def test_user_descriptions_hidden_by_default(self) -> None:
        pipeline = AdagioPipeline.model_validate(_user_description_pipeline_dict())

        rendered = _render_plain(render_pipeline_text(pipeline))

        # Plugin descriptions still render by default ...
        self.assertIn("Demultiplexed sequence data.", rendered)
        # ... but the user-authored notes do not.
        self.assertNotIn("note:", rendered)
        self.assertNotIn("Raw reads from sequencing run 3", rendered)
        self.assertNotIn("Denoised table we publish", rendered)
        self.assertNotIn("Denoise tuned for our amplicon", rendered)

    def test_user_descriptions_shown_when_verbose(self) -> None:
        pipeline = AdagioPipeline.model_validate(_user_description_pipeline_dict())

        rendered = _render_plain(
            render_pipeline_text(pipeline, show_user_descriptions=True)
        )

        # Input, output, and action notes are all rendered, prefixed with "note:".
        self.assertIn("note: Raw reads from sequencing run 3", rendered)
        self.assertIn("note: Denoised table we publish", rendered)
        self.assertIn("note: Denoise tuned for our amplicon", rendered)
        # The action note appears inside the action panel above its Inputs section.
        self.assertLess(
            rendered.index("note: Denoise tuned for our amplicon"),
            rendered.index("Inputs:"),
        )
        # Plugin descriptions remain alongside the user notes.
        self.assertIn("Demultiplexed sequence data.", rendered)

    def test_pipeline_show_cli_verbose_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_path = Path(tmpdir) / "pipeline.json"
            payload = {"spec": _user_description_pipeline_dict()}
            pipeline_path.write_text(json.dumps(payload), encoding="utf-8")

            base_cmd = [
                sys.executable,
                "-m",
                "adagio.cli.main",
                "pipeline",
                "show",
                str(pipeline_path),
            ]
            default = subprocess.run(
                base_cmd, capture_output=True, check=False, text=True
            )
            verbose = subprocess.run(
                [*base_cmd, "-v"], capture_output=True, check=False, text=True
            )

        self.assertEqual(default.returncode, 0, msg=default.stderr)
        self.assertEqual(verbose.returncode, 0, msg=verbose.stderr)
        self.assertNotIn("note:", default.stdout)
        self.assertIn("note: Raw reads from sequencing run 3", verbose.stdout)


if __name__ == "__main__":
    unittest.main()
