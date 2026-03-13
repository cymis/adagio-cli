from .base import PipelineExecutor
from .defaults import DefaultTaskEnvironmentResolver
from .docker import DockerTaskEnvironmentLauncher
from .task_environments import TaskEnvironmentExecutor


def select_default_executor() -> PipelineExecutor:
    return TaskEnvironmentExecutor(
        environment_resolver=DefaultTaskEnvironmentResolver(),
        launchers={
            "docker": DockerTaskEnvironmentLauncher(),
        },
    )
