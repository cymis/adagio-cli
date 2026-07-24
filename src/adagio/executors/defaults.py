from adagio.model.task import PluginActionTask

from .base import (
    TaskEnvironmentOverride,
    TaskEnvironmentResolver,
    TaskEnvironmentSpec,
)


class ConfigurableTaskEnvironmentResolver(TaskEnvironmentResolver):
    def __init__(
        self,
        *,
        base: TaskEnvironmentResolver | None = None,
        default_override: TaskEnvironmentOverride | None = None,
        plugin_overrides: dict[str, TaskEnvironmentOverride] | None = None,
        task_overrides: dict[str, TaskEnvironmentOverride] | None = None,
    ) -> None:
        self._base = base
        self._default_override = default_override
        self._plugin_overrides = plugin_overrides or {}
        self._task_overrides = task_overrides or {}

    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec:
        base_environment = (
            self._base.resolve(task=task) if self._base is not None else None
        )
        kind = base_environment.kind if base_environment is not None else ""
        reference = base_environment.reference if base_environment is not None else ""
        options = (
            dict(base_environment.options or {}) if base_environment is not None else {}
        )
        configured = False

        for override in (
            self._default_override,
            self._find_plugin_override(task=task),
            self._find_task_override(task=task),
        ):
            if override is None:
                continue
            if override.kind is not None:
                if override.kind != kind and override.reference is None:
                    reference = ""
                kind = override.kind
                configured = True
            if override.reference is not None:
                reference = override.reference
                configured = True
            if override.platform is not None:
                options["platform"] = override.platform
                configured = True
            if override.options is not None:
                options.update(dict(override.options))
                configured = True

        if not kind or not reference:
            raise ValueError(
                f'No execution environment is configured for plugin "{task.plugin}". '
                "Provide a plugin or task environment in the run configuration."
            )

        return TaskEnvironmentSpec(
            kind=kind,
            reference=reference,
            description=(
                f"configured environment for {task.name or task.id}"
                if configured
                else base_environment.description
                if base_environment is not None
                else None
            ),
            options=options or None,
        )

    def _find_task_override(
        self, *, task: PluginActionTask
    ) -> TaskEnvironmentOverride | None:
        candidates = [task.id]
        if task.name:
            candidates.insert(0, task.name)
        candidates.append(f"{task.plugin}.{task.action}")

        for candidate in candidates:
            override = self._task_overrides.get(candidate)
            if override:
                return override
        return None

    def _find_plugin_override(
        self, *, task: PluginActionTask
    ) -> TaskEnvironmentOverride | None:
        plugin_candidates = [task.plugin, task.plugin.lower()]
        for candidate in plugin_candidates:
            override = self._plugin_overrides.get(candidate)
            if override:
                return override

        return None
