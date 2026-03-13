from __future__ import annotations

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
