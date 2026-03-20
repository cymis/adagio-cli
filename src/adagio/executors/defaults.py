from adagio.model.task import PluginActionTask

from .base import TaskEnvironmentResolver, TaskEnvironmentSpec

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
        plugin_image_overrides: dict[str, str] | None = None,
        task_image_overrides: dict[str, str] | None = None,
    ) -> None:
        self._base = base
        self._plugin_image_overrides = plugin_image_overrides or {}
        self._task_image_overrides = task_image_overrides or {}

    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec:
        override = self._find_override(task=task)
        if override is None:
            return self._base.resolve(task=task)

        return TaskEnvironmentSpec(
            kind="docker",
            reference=override,
            description=f"configured image override for {task.name or task.id}",
        )

    def _find_override(self, *, task: PluginActionTask) -> str | None:
        candidates = [task.id]
        if task.name:
            candidates.insert(0, task.name)
        candidates.append(f"{task.plugin}.{task.action}")

        for candidate in candidates:
            override = self._task_image_overrides.get(candidate)
            if override:
                return override

        plugin_candidates = [task.plugin, task.plugin.lower()]
        for candidate in plugin_candidates:
            override = self._plugin_image_overrides.get(candidate)
            if override:
                return override

        return None
