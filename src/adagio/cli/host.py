"""Commands a supervising runtime uses to ask what this host runs, and to clean up."""

import argparse
import json
import sys
from pathlib import Path

from .config import RUN_CONFIG_VERSION


def run_capabilities(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="adagio capabilities",
        description=(
            "Print, as JSON, the run-configuration version this CLI reads and the "
            "executors this host can use."
        ),
    )
    parser.parse_args(argv)

    from ..execution.backends import executor_capabilities
    from ..executors import builtin_task_environment_launchers

    print(
        json.dumps(
            {
                "config_version": RUN_CONFIG_VERSION,
                "executors": executor_capabilities(builtin_task_environment_launchers()),
            }
        )
    )


def run_cleanup(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="adagio cleanup",
        description=(
            "Cancel scheduler jobs an interrupted run left behind, using the file "
            "it was given with --run-record. Prints a JSON report and exits 1 if "
            "anything could not be confirmed."
        ),
    )
    parser.add_argument("run_record", help="The run's --run-record file.")
    opts = parser.parse_args(argv)

    from ..execution.backends import clean_up_run

    try:
        errors = clean_up_run(Path(opts.run_record))
    except (OSError, ValueError, KeyError) as error:
        errors = [f"Cleanup failed: {error}"]
    print(json.dumps({"complete": not errors, "errors": errors}))
    if errors:
        sys.exit(1)
