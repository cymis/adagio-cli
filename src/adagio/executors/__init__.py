__all__ = ["select_default_executor"]


def select_default_executor(
    *,
    default_override=None,
    plugin_overrides=None,
    task_overrides=None,
):
    from .defaults import (
        ConfigurableTaskEnvironmentResolver,
        DefaultTaskEnvironmentResolver,
    )
    from .apptainer import ApptainerTaskEnvironmentLauncher
    from .docker import DockerTaskEnvironmentLauncher
    from .task_environments import TaskEnvironmentExecutor

    return TaskEnvironmentExecutor(
        environment_resolver=ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            default_override=default_override,
            plugin_overrides=plugin_overrides,
            task_overrides=task_overrides,
        ),
        launchers={
            "apptainer": ApptainerTaskEnvironmentLauncher(),
            "docker": DockerTaskEnvironmentLauncher(),
        },
    )
