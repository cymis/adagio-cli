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
