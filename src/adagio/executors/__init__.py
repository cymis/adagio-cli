__all__ = ["select_default_executor"]


def select_default_executor(
    *,
    plugin_image_overrides: dict[str, str] | None = None,
    task_image_overrides: dict[str, str] | None = None,
):
    from .defaults import (
        ConfigurableTaskEnvironmentResolver,
        DefaultTaskEnvironmentResolver,
    )
    from .docker import DockerTaskEnvironmentLauncher
    from .task_environments import TaskEnvironmentExecutor

    return TaskEnvironmentExecutor(
        environment_resolver=ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            plugin_image_overrides=plugin_image_overrides,
            task_image_overrides=task_image_overrides,
        ),
        launchers={
            "docker": DockerTaskEnvironmentLauncher(),
        },
    )
