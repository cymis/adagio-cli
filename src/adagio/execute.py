from typing import Any, Dict
from pathlib import Path
import json

PipelineSpec = Dict[str, Any]
Config = Dict[str, Any]

# Obviously this is all temporary


def parse_spec(input_file: Path) -> PipelineSpec:
    with open(input_file, "r") as f:
        return json.load(f)


def parse_config(input_file: Path) -> Config:
    with open(input_file, "r") as f:
        return json.load(f)


def process_job(spec: PipelineSpec, config: Config) -> None:
    pass
import typing as t

from pathlib import Path
from adagio.execution.context import AdagioContext
from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.api import Monitor
from adagio.monitor.log import LogMonitor




def execute_pipeline(pipeline: AdagioPipeline, arguments: AdagioArguments,
                     recycle=True, advanced=None):
    sig = pipeline.signature

    pipeline.validate_graph()
    sig.validate_arguments(arguments)

    ctx = _setup_context(advanced)
    scope: dict[str, t.Any] = {}

    sig.load_inputs(ctx, arguments, scope)
    params = sig.get_params(arguments)

    for task in pipeline.iter_tasks():
        task.exec(ctx, params, scope)

    sig.save_outputs(ctx, arguments, scope)


def _setup_context(advanced):
    from qiime2 import get_cache
    from qiime2.sdk import Context

    # TODO: actually configure a non-temp cache
    from qiime2.sdk.parallel_config import get_vendored_config

    from qiime2.sdk import PluginManager
    PluginManager()
    cache = get_cache()
    # TODO: implement a suitable parallel context
    with cache:
        ctx = AdagioContext()
        ctx.monitor = LogMonitor()

    return ctx
