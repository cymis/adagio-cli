__all__ = ["select_default_executor"]


def select_default_executor():
    from .defaults import DefaultTaskEnvironmentResolver
    from .docker import DockerTaskEnvironmentLauncher
    from .task_environments import TaskEnvironmentExecutor

    return TaskEnvironmentExecutor(
        environment_resolver=DefaultTaskEnvironmentResolver(),
        launchers={
            "docker": DockerTaskEnvironmentLauncher(),
        },
    )
