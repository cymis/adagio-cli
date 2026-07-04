import unittest

from adagio.executors.common import plan_execution_order, prune_to_targets
from adagio.model.task import PluginActionTask


def _task(task_id: str, *, inputs: dict[str, str], output_id: str) -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": task_id,
            "kind": "plugin-action",
            "plugin": "p",
            "action": "a",
            "inputs": {
                name: {"kind": "archive", "id": src} for name, src in inputs.items()
            },
            "parameters": {},
            "outputs": {"out": {"kind": "archive", "id": output_id}},
        }
    )


class TargetsPruningTests(unittest.TestCase):
    def _plan(self):
        # root-input -> A -> B -> D ; also A -> C -> D. root scope: "root".
        task_a = _task("A", inputs={"x": "root"}, output_id="a-out")
        task_b = _task("B", inputs={"x": "a-out"}, output_id="b-out")
        task_c = _task("C", inputs={"x": "a-out"}, output_id="c-out")
        task_d = _task("D", inputs={"x": "b-out", "y": "c-out"}, output_id="d-out")
        plan = plan_execution_order(
            tasks=[task_a, task_b, task_c, task_d],
            scope={"root": "root.qza"},
        )
        return plan

    def test_no_targets_is_noop(self) -> None:
        plan = self._plan()
        pruned = prune_to_targets(execution_plan=plan, target_ids=set())
        self.assertEqual([t.id for t in pruned], [t.id for t in plan])

    def test_all_targets_keeps_everything(self) -> None:
        plan = self._plan()
        all_ids = {t.id for t in plan}
        pruned = prune_to_targets(execution_plan=plan, target_ids=all_ids)
        self.assertEqual({t.id for t in pruned}, all_ids)

    def test_target_by_task_id_prunes_to_upstream_closure(self) -> None:
        plan = self._plan()
        # Ask only for B: needs A (produces a-out) but not C or D.
        pruned = prune_to_targets(execution_plan=plan, target_ids={"B"})
        self.assertEqual({t.id for t in pruned}, {"A", "B"})
        # Order preserved (A before B).
        ids = [t.id for t in pruned]
        self.assertLess(ids.index("A"), ids.index("B"))

    def test_target_by_output_element_id(self) -> None:
        plan = self._plan()
        # c-out is produced by C, which needs A.
        pruned = prune_to_targets(execution_plan=plan, target_ids={"c-out"})
        self.assertEqual({t.id for t in pruned}, {"A", "C"})

    def test_terminal_target_pulls_all_upstream(self) -> None:
        plan = self._plan()
        pruned = prune_to_targets(execution_plan=plan, target_ids={"D"})
        self.assertEqual({t.id for t in pruned}, {"A", "B", "C", "D"})


if __name__ == "__main__":
    unittest.main()
