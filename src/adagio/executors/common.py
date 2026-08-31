import typing as t

from adagio.model.task import input_source_ids


def plan_execution_order(
    *,
    tasks: list[t.Any],
    scope: dict[str, t.Any],
    optional_missing_ids: set[str] | None = None,
) -> list[t.Any]:
    """Return a dependency-respecting serial execution plan."""
    available_ids = set(scope.keys())
    optional_missing_ids = optional_missing_ids or set()
    remaining = list(tasks)
    planned: list[t.Any] = []

    while remaining:
        progressed = False
        for task in list(remaining):
            missing = [
                source_id
                for src in task.inputs.values()
                for source_id in input_source_ids(src)
                if source_id not in available_ids and source_id not in optional_missing_ids
            ]
            if missing:
                continue

            planned.append(task)
            remaining.remove(task)
            progressed = True
            for output in task.outputs.values():
                available_ids.add(output.id)

        if not progressed:
            details = []
            for task in remaining:
                missing = ", ".join(
                    source_id
                    for src in task.inputs.values()
                    for source_id in input_source_ids(src)
                    if source_id not in available_ids
                    and source_id not in optional_missing_ids
                )
                details.append(f"{task.id}: missing [{missing}]")
            raise RuntimeError("Unable to resolve task dependencies. " + "; ".join(details))

    return planned


def prune_to_targets(
    *,
    execution_plan: list[t.Any],
    target_ids: set[str],
) -> list[t.Any]:
    """Prune tasks to the upstream closure needed for ``target_ids``.

    ``target_ids`` are node ids the caller wants produced (design §5.6). A task
    is retained when it (a) produces one of the target ids via an ``outputs``
    entry, (b) *is* one of the target ids, or (c) is required upstream to produce
    a retained task's inputs. Serial engine only; input order is preserved, so
    callers can calculate dependency order after pruning.

    v1 callers pass *all* node ids, so this is a no-op then; it must be correct
    when a strict subset is given.
    """
    if not target_ids:
        return execution_plan

    # Map each element id an output produces -> the task that produces it.
    producer_of: dict[str, t.Any] = {}
    for task in execution_plan:
        for output in task.outputs.values():
            producer_of[output.id] = task

    needed_task_ids: set[str] = set()
    frontier: list[str] = []

    def _need(task: t.Any) -> None:
        if task.id not in needed_task_ids:
            needed_task_ids.add(task.id)
            frontier.append(task.id)

    task_by_id = {task.id: task for task in execution_plan}

    # Seed from targets: a target may be a task id or an output element id.
    for target in target_ids:
        if target in task_by_id:
            _need(task_by_id[target])
        producer = producer_of.get(target)
        if producer is not None:
            _need(producer)

    # Walk upstream: pull in the producers of every retained task's inputs.
    while frontier:
        task = task_by_id[frontier.pop()]
        for src in task.inputs.values():
            for source_id in input_source_ids(src):
                producer = producer_of.get(source_id)
                if producer is not None:
                    _need(producer)

    return [task for task in execution_plan if task.id in needed_task_ids]


def task_label(task: t.Any) -> str:
    kind = getattr(task, "kind", "unknown")
    task_id = getattr(task, "id", "<unknown>")
    if kind == "plugin-action":
        plugin = getattr(task, "plugin", "<plugin>")
        action = getattr(task, "action", "<action>")
        return f"{task_id} ({plugin}.{action})"
    if kind == "built-in":
        name = getattr(task, "name", "built-in")
        return f"{task_id} ({name})"
    return task_id
