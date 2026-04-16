from pathlib import Path
import json

import typing as t

from adagio.model.arguments import AdagioArguments
from adagio.model.pipeline import AdagioPipeline
from adagio.monitor.log import LogMonitor

PipelineSpec = t.Dict[str, t.Any]
Config = t.Dict[str, t.Any]

# Obviously this is all temporary


def parse_spec(input_file: Path) -> PipelineSpec:
    with open(input_file, "r") as f:
        return json.load(f)


def parse_config(input_file: Path) -> Config:
    with open(input_file, "r") as f:
        return json.load(f)


def process_job(spec: PipelineSpec, config: Config) -> None:
    pass


def execute_pipeline(
    pipeline: AdagioPipeline, arguments: AdagioArguments, recycle=True, advanced=None
):
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

    # TODO: actually configure a non-temp cache

    from adagio.execution.context import AdagioContext
    from qiime2.sdk import PluginManager

    PluginManager()
    cache = get_cache()
    # TODO: implement a suitable parallel context
    with cache:
        ctx = AdagioContext()
        ctx.monitor = LogMonitor()

    return ctx
