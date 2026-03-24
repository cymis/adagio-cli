from adagio.model.task import PluginActionTask

from .base import (
    TaskEnvironmentOverride,
    TaskEnvironmentResolver,
    TaskEnvironmentSpec,
)

DEFAULT_REGISTRY = "ghcr.io/cymis"
DEFAULT_IMAGE_PREFIX = "qiime2-plugin-"
DEFAULT_TAG = "2026.1"


class DefaultTaskEnvironmentResolver(TaskEnvironmentResolver):
    """Resolve plugin actions to default task environments.

    The current default is a Docker image in GHCR derived from the plugin name.
    The interface is task-scoped so future config can override individual tasks
    with Docker, SIF/Apptainer, Conda, or cluster-specific environments.
    """

    def __init__(
        self,
        *,
        registry: str = DEFAULT_REGISTRY,
        image_prefix: str = DEFAULT_IMAGE_PREFIX,
        tag: str = DEFAULT_TAG,
    ) -> None:
        self._registry = registry.rstrip("/")
        self._image_prefix = image_prefix
        self._tag = tag

    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec:
        normalized = task.plugin.lower().replace("_", "-")
        reference = f"{self._registry}/{self._image_prefix}{normalized}:{self._tag}"
        return TaskEnvironmentSpec(
            kind="docker",
            reference=reference,
            description=f"default plugin image for {task.plugin}",
        )


class ConfigurableTaskEnvironmentResolver(TaskEnvironmentResolver):
    def __init__(
        self,
        *,
        base: TaskEnvironmentResolver,
        default_override: TaskEnvironmentOverride | None = None,
        plugin_overrides: dict[str, TaskEnvironmentOverride] | None = None,
        task_overrides: dict[str, TaskEnvironmentOverride] | None = None,
    ) -> None:
        self._base = base
        self._default_override = default_override
        self._plugin_overrides = plugin_overrides or {}
        self._task_overrides = task_overrides or {}

    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec:
        base_environment = self._base.resolve(task=task)
        reference = base_environment.reference
        options = dict(base_environment.options or {})
        configured = False

        for override in (
            self._default_override,
            self._find_plugin_override(task=task),
            self._find_task_override(task=task),
        ):
            if override is None:
                continue
            if override.reference is not None:
                reference = override.reference
                configured = True
            if override.platform is not None:
                options["platform"] = override.platform
                configured = True

        return TaskEnvironmentSpec(
            kind=base_environment.kind,
            reference=reference,
            description=(
                f"configured environment for {task.name or task.id}"
                if configured
                else base_environment.description
            ),
            options=options or None,
        )

    def _find_task_override(self, *, task: PluginActionTask) -> TaskEnvironmentOverride | None:
        candidates = [task.id]
        if task.name:
            candidates.insert(0, task.name)
        candidates.append(f"{task.plugin}.{task.action}")

        for candidate in candidates:
            override = self._task_overrides.get(candidate)
            if override:
                return override
        return None

    def _find_plugin_override(self, *, task: PluginActionTask) -> TaskEnvironmentOverride | None:
        plugin_candidates = [task.plugin, task.plugin.lower()]
        for candidate in plugin_candidates:
            override = self._plugin_overrides.get(candidate)
            if override:
                return override

        return None
