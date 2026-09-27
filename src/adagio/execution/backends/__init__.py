"""Where tasks run: one backend per ``[executor] kind``.

Supporting another batch scheduler takes one adapter module (see ``slurm``)
and one entry in each of ``ExecutorConfig`` and ``SCHEDULERS`` below. The
coordinator, the batch machinery and the cleanup contract are shared.
"""

import subprocess
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Annotated, Any, Union

from pydantic import Field

from .base import Backend
from .batch import (
    SHARED_ENVIRONMENTS,
    BatchBackend,
    cancel_outstanding,
    make_command_runner,
    missing_commands,
)
from .local import LocalBackend, LocalExecutorConfig
from .run_record import owning_run, read_run_record, remove_run_record
from .slurm import SlurmExecutorConfig, SlurmScheduler
from .submissions import SubmissionRegistry

ExecutorConfig = Annotated[
    Union[LocalExecutorConfig, SlurmExecutorConfig],
    Field(discriminator="kind"),
]

SCHEDULERS = {SlurmScheduler.kind: SlurmScheduler}


def create_backend(
    config: LocalExecutorConfig | SlurmExecutorConfig,
    *,
    run_record: Path | None = None,
) -> Backend:
    if isinstance(config, LocalExecutorConfig):
        return LocalBackend(config)
    return BatchBackend(
        config=config,
        scheduler=SCHEDULERS[config.kind].for_executor(config),
        run_record=run_record,
    )


def executor_capabilities(local_environments: Iterable[str]) -> dict[str, Any]:
    """Which executors this host can use, and the task environments each runs."""
    capabilities: dict[str, Any] = {
        LocalBackend.kind: {
            "available": True,
            "task_environments": sorted(local_environments),
        }
    }
    for kind, scheduler in SCHEDULERS.items():
        missing = missing_commands(scheduler)
        entry: dict[str, Any] = {
            "available": not missing,
            "task_environments": sorted(SHARED_ENVIRONMENTS),
        }
        if missing:
            entry["reason"] = f"{', '.join(missing)} not found on PATH"
        capabilities[kind] = entry
    return capabilities


def clean_up_run(
    record_path: Path,
    *,
    settle_unconfirmed: bool = False,
    command_runner: Callable[..., subprocess.CompletedProcess] | None = None,
) -> list[str]:
    """Cancel whatever an interrupted run left with its scheduler.

    Returns what could not be confirmed; the record is removed once nothing
    is left. Raises ``RunOwned`` while the run's own process is still alive:
    only it may act on the run then. ``settle_unconfirmed`` settles
    submissions whose reply was lost and that no job has appeared for, once
    someone has checked the scheduler.
    """
    if not record_path.exists():
        return []
    with owning_run(record_path):
        record = read_run_record(record_path)
        if record is None:
            return []
        if not record.registry.exists():
            return [
                f"The run's registry {record.registry} is missing, so its jobs "
                "cannot be looked up. If its storage is not mounted, mount it and "
                "try again; if it was deleted, make sure none of the run's "
                f"adagio-… jobs remain, then delete {record_path}."
            ]
        scheduler = SCHEDULERS[record.executor]()
        registry = SubmissionRegistry.load(record.registry)
        errors = cancel_outstanding(
            registry,
            scheduler,
            make_command_runner(scheduler, runner=command_runner),
            record=record_path,
            settle_unconfirmed=settle_unconfirmed,
        )
        if not errors:
            remove_run_record(record_path)
    return errors
