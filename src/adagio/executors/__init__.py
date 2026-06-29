__all__ = ["select_default_executor"]


def builtin_task_environment_launchers():
    from .apptainer import ApptainerTaskEnvironmentLauncher
    from .conda import CondaTaskEnvironmentLauncher
    from .docker import DockerTaskEnvironmentLauncher

    launchers = [
        ApptainerTaskEnvironmentLauncher(),
        CondaTaskEnvironmentLauncher(),
        DockerTaskEnvironmentLauncher(),
    ]
    return {launcher.kind: launcher for launcher in launchers}


def select_default_executor(
    *,
    default_override=None,
    plugin_overrides=None,
    task_overrides=None,
    launchers=None,
):
    from .defaults import (
        ConfigurableTaskEnvironmentResolver,
        DefaultTaskEnvironmentResolver,
    )
    from .task_environments import TaskEnvironmentExecutor

    return TaskEnvironmentExecutor(
        environment_resolver=ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            default_override=default_override,
            plugin_overrides=plugin_overrides,
            task_overrides=task_overrides,
        ),
        launchers=launchers or builtin_task_environment_launchers(),
    )
