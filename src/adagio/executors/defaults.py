from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from adagio.model.task import PluginActionTask

from .base import (
    TaskEnvironmentOverride,
    TaskEnvironmentResolver,
    TaskEnvironmentSpec,
)


@dataclass(frozen=True)
class EnvironmentLayer:
    """One source of environment overrides, such as a run-config file.

    Within a layer a task override beats a plugin override, which beats the
    defaults. Across layers the later one wins for every field it sets, so a
    ``--config`` file overrides the environments saved with the pipeline even
    where the pipeline set them per task.
    """

    source: str
    default_override: TaskEnvironmentOverride | None = None
    plugin_overrides: Mapping[str, TaskEnvironmentOverride] = field(
        default_factory=dict
    )
    task_overrides: Mapping[str, TaskEnvironmentOverride] = field(
        default_factory=dict
    )

    def overrides_for(
        self, task: PluginActionTask
    ) -> list[TaskEnvironmentOverride]:
        return [
            override
            for override in (
                self.default_override,
                _find_plugin_override(self.plugin_overrides, task=task),
                _find_task_override(self.task_overrides, task=task),
            )
            if override is not None
        ]


class ConfigurableTaskEnvironmentResolver(TaskEnvironmentResolver):
    def __init__(
        self,
        *,
        base: TaskEnvironmentResolver | None = None,
        default_override: TaskEnvironmentOverride | None = None,
        plugin_overrides: dict[str, TaskEnvironmentOverride] | None = None,
        task_overrides: dict[str, TaskEnvironmentOverride] | None = None,
        lower_layers: Sequence[EnvironmentLayer] = (),
    ) -> None:
        """``lower_layers`` apply first, lowest first; the direct overrides last."""
        self._base = base
        self._layers = [
            *lower_layers,
            EnvironmentLayer(
                source="--config",
                default_override=default_override,
                plugin_overrides=plugin_overrides or {},
                task_overrides=task_overrides or {},
            ),
        ]

    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec:
        environment, _ = self._resolve(task=task)
        return environment

    def source_of(self, *, task: PluginActionTask) -> str | None:
        """The layer that last configured ``task``'s environment, if any did."""
        _, source = self._resolve(task=task)
        return source

    def _resolve(
        self, *, task: PluginActionTask
    ) -> tuple[TaskEnvironmentSpec, str | None]:
        base_environment = (
            self._base.resolve(task=task) if self._base is not None else None
        )
        kind = base_environment.kind if base_environment is not None else ""
        reference = base_environment.reference if base_environment is not None else ""
        options = (
            dict(base_environment.options or {}) if base_environment is not None else {}
        )
        source: str | None = None

        for layer in self._layers:
            for override in layer.overrides_for(task):
                if override.kind is not None:
                    if override.kind != kind and override.reference is None:
                        reference = ""
                    kind = override.kind
                if override.reference is not None:
                    reference = override.reference
                if override.platform is not None:
                    options["platform"] = override.platform
                if override.options is not None:
                    options.update(dict(override.options))
                source = layer.source

        if not kind or not reference:
            raise ValueError(
                f'No execution environment is configured for plugin "{task.plugin}". '
                "Provide a plugin or task environment in the run configuration."
            )

        return (
            TaskEnvironmentSpec(
                kind=kind,
                reference=reference,
                description=(
                    f"configured environment for {task.name or task.id}"
                    if source is not None
                    else base_environment.description
                    if base_environment is not None
                    else None
                ),
                options=options or None,
            ),
            source,
        )


def _find_task_override(
    task_overrides: Mapping[str, TaskEnvironmentOverride], *, task: PluginActionTask
) -> TaskEnvironmentOverride | None:
    candidates = [task.id]
    if task.name:
        candidates.insert(0, task.name)
    candidates.append(f"{task.plugin}.{task.action}")

    for candidate in candidates:
        override = task_overrides.get(candidate)
        if override:
            return override
    return None


def _find_plugin_override(
    plugin_overrides: Mapping[str, TaskEnvironmentOverride], *, task: PluginActionTask
) -> TaskEnvironmentOverride | None:
    for candidate in (task.plugin, task.plugin.lower()):
        override = plugin_overrides.get(candidate)
        if override:
            return override
    return None
