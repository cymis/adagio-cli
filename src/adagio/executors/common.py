import typing as t


def plan_execution_order(*, tasks: list[t.Any], scope: dict[str, t.Any]) -> list[t.Any]:
    """Return a dependency-respecting serial execution plan."""
    available_ids = set(scope.keys())
    remaining = list(tasks)
    planned: list[t.Any] = []

    while remaining:
        progressed = False
        for task in list(remaining):
            missing = [src.id for src in task.inputs.values() if src.id not in available_ids]
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
                missing = ", ".join(src.id for src in task.inputs.values() if src.id not in available_ids)
                details.append(f"{task.id}: missing [{missing}]")
            raise RuntimeError("Unable to resolve task dependencies. " + "; ".join(details))

    return planned


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
