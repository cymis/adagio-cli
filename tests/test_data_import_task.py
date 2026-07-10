import unittest

from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import DataImportTask

AST = {
    "type": "expression",
    "builtin": True,
    "name": "Str",
    "predicate": None,
    "fields": [],
}


def _data_import_graph(**task_overrides):
    task = {
        "id": "import-1",
        "kind": "built-in",
        "name": "data-import",
        "user_description": "Raw reads dropped in by the lab",
        "inputs": {"source": {"kind": "archive", "id": "src-1"}},
        "parameters": {
            "semantic_type": {"kind": "literal", "value": "EMPSingleEndSequences"},
            "input_format": {"kind": "literal", "value": "EMPSingleEndDirFmt"},
            "validate_level": {"kind": "literal", "value": "max"},
        },
        # The editor emits type/ast/description alongside kind/id; the CLI model
        # ignores the extra keys, so this mirrors a real .adg output payload.
        "outputs": {
            "artifact": {
                "kind": "archive",
                "id": "out-1",
                "type": "EMPSingleEndSequences",
                "ast": AST,
                "description": "Imported QIIME artifact.",
            }
        },
    }
    task.update(task_overrides)
    return {
        "type": "pipeline",
        "signature": {"inputs": [], "parameters": [], "outputs": []},
        "graph": [task],
    }


class DataImportTaskTests(unittest.TestCase):
    def test_pipeline_model_accepts_data_import_task(self) -> None:
        pipeline = AdagioPipeline.model_validate(_data_import_graph())

        (task,) = pipeline.graph
        self.assertIsInstance(task, DataImportTask)
        self.assertEqual(task.name, "data-import")
        self.assertEqual(task.user_description, "Raw reads dropped in by the lab")
        self.assertEqual(task.inputs["source"].id, "src-1")
        self.assertEqual(task.outputs["artifact"].id, "out-1")
        self.assertEqual(task.parameters["semantic_type"].value, "EMPSingleEndSequences")
        self.assertEqual(task.parameters["input_format"].value, "EMPSingleEndDirFmt")
        self.assertEqual(task.parameters["validate_level"].value, "max")

    def test_user_description_is_optional(self) -> None:
        pipeline = AdagioPipeline.model_validate(_data_import_graph(user_description=None))
        (task,) = pipeline.graph
        self.assertIsNone(task.user_description)

    def test_param_value_resolves_literal_and_promoted(self) -> None:
        graph = _data_import_graph(
            parameters={
                "semantic_type": {"kind": "promoted", "id": "p-sem"},
                "input_format": {"kind": "literal", "value": "EMPSingleEndDirFmt"},
                "validate_level": {"kind": "literal", "value": "min"},
            }
        )
        (task,) = AdagioPipeline.model_validate(graph).graph

        params = {"p-sem": "EMPPairedEndSequences"}
        self.assertEqual(task._param_value("semantic_type", params), "EMPPairedEndSequences")
        self.assertEqual(task._param_value("input_format", params), "EMPSingleEndDirFmt")
        self.assertEqual(task._param_value("validate_level", params), "min")
        self.assertEqual(task._param_value("missing", params, "fallback"), "fallback")


if __name__ == "__main__":
    unittest.main()
