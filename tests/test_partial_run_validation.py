import unittest
from types import SimpleNamespace

from adagio.cli.runtime import _validate_required_arguments
from adagio.model.arguments import AdagioArguments
from adagio.model.task import PluginActionTask


def _task(task_id: str, input_name: str, input_id: str, output_id: str) -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": task_id,
            "kind": "plugin-action",
            "plugin": "p",
            "action": "a",
            "inputs": {input_name: {"kind": "archive", "id": input_id}},
            "parameters": {},
            "outputs": {"out": {"kind": "archive", "id": output_id}},
        }
    )


class _FakeSignature:
    def __init__(self, inputs, parameters) -> None:  # noqa: ANN001
        self.inputs = inputs
        self.parameters = parameters
        self.outputs: list[object] = []


class _FakePipeline:
    def __init__(self, tasks, inputs, parameters) -> None:  # noqa: ANN001
        self.signature = _FakeSignature(inputs, parameters)
        self._tasks = tasks

    def iter_tasks(self):
        return iter(self._tasks)


def _input(input_id: str, name: str, required: bool = True):
    return SimpleNamespace(id=input_id, name=name, required=required)


class PartialRunValidationTests(unittest.TestCase):
    def _pipeline(self) -> _FakePipeline:
        # Two independent branches: A consumes root input 'seqs', B consumes
        # 'barcodes'. Both inputs are pipeline-required.
        return _FakePipeline(
            tasks=[
                _task("A", "seqs", "seqs", "a-out"),
                _task("B", "barcodes", "barcodes", "b-out"),
            ],
            inputs=[_input("seqs", "seqs"), _input("barcodes", "barcodes")],
            parameters=[],
        )

    def test_partial_run_ignores_out_of_closure_inputs(self) -> None:
        pipeline = self._pipeline()
        arguments = AdagioArguments(
            inputs={"seqs": "seqs.qza"}, parameters={}, outputs={}
        )
        # Targeting A must not demand 'barcodes' (branch B is out of closure).
        _validate_required_arguments(pipeline, arguments, target_ids={"A"})

    def test_full_run_requires_every_input(self) -> None:
        pipeline = self._pipeline()
        arguments = AdagioArguments(
            inputs={"seqs": "seqs.qza"}, parameters={}, outputs={}
        )
        with self.assertRaises(SystemExit):
            _validate_required_arguments(pipeline, arguments)

    def test_partial_run_still_requires_its_own_closure_inputs(self) -> None:
        pipeline = self._pipeline()
        arguments = AdagioArguments(inputs={}, parameters={}, outputs={})
        # Targeting A with 'seqs' missing must still fail — it IS in A's closure.
        with self.assertRaises(SystemExit):
            _validate_required_arguments(pipeline, arguments, target_ids={"A"})


if __name__ == "__main__":
    unittest.main()
