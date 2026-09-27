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


#: ``adagio cleanup`` exit status while the run's own process is still alive
#: (EX_TEMPFAIL): nothing was touched, so try again later.
OWNER_ALIVE_EXIT = 75


def run_cleanup(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="adagio cleanup",
        description=(
            "Cancel scheduler jobs an interrupted run left behind, using the file "
            "it was given with --run-record. Prints a JSON report and exits 1 if "
            "anything could not be confirmed, or 75 without touching anything "
            "while the run's own process is still alive."
        ),
    )
    parser.add_argument("run_record", help="The run's --run-record file.")
    parser.add_argument(
        "--settle-unconfirmed",
        action="store_true",
        help=(
            "Treat submissions whose reply was lost as never having become jobs "
            "once no job with their name is in the queue. Use only after "
            "checking the scheduler yourself, when the cluster's credential "
            "lifetime is not configured."
        ),
    )
    opts = parser.parse_args(argv)

    from ..execution.backends import clean_up_run
    from ..execution.backends.run_record import RunOwned

    try:
        errors = clean_up_run(
            Path(opts.run_record), settle_unconfirmed=opts.settle_unconfirmed
        )
    except RunOwned as owned:
        report = {"complete": False, "owner_alive": True, "errors": [str(owned)]}
        print(json.dumps(report))
        sys.exit(OWNER_ALIVE_EXIT)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        errors = [f"Cleanup failed: {error}"]
    print(json.dumps({"complete": not errors, "errors": errors}))
    if errors:
        sys.exit(1)
