from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

from rich.console import Console

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.model.task import PluginActionTask
from adagio.monitor.api import Monitor

from .cache_support import ExecutionCacheConfig


class PipelineExecutor(Protocol):
    mode_label: str

    def execute(
        self,
        *,
        pipeline: AdagioPipeline,
        arguments: AdagioArguments,
        console: Console | None = None,
        monitor: Monitor | None = None,
        cache_config: ExecutionCacheConfig | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class TaskEnvironmentSpec:
    kind: str
    reference: str
    description: str | None = None
    options: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class TaskEnvironmentOverride:
    kind: str | None = None
    reference: str | None = None
    platform: str | None = None


@dataclass(frozen=True)
class TaskExecutionRequest:
    task: PluginActionTask
    cwd: Path
    work_path: Path
    archive_inputs: Mapping[str, str]
    archive_collection_inputs: Mapping[str, list[str]]
    metadata_inputs: Mapping[str, str]
    params: Mapping[str, Any]
    metadata_column_kwargs: Mapping[str, Mapping[str, str]]
    outputs: Mapping[str, str]
    cache_path: str | None = None
    recycle_pool: str | None = None


@dataclass(frozen=True)
class TaskExecutionResult:
    outputs: Mapping[str, str]
    reused: bool = False


class TaskEnvironmentResolver(Protocol):
    def resolve(self, *, task: PluginActionTask) -> TaskEnvironmentSpec: ...


class TaskEnvironmentLauncher(Protocol):
    kind: str

    def launch(
        self,
        *,
        environment: TaskEnvironmentSpec,
        request: TaskExecutionRequest,
        console: Console | None = None,
    ) -> TaskExecutionResult: ...
