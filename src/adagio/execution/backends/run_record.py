"""The file that lets a supervisor clean up after a run it had to kill.

``adagio runtime --run-record PATH`` names a file this CLI writes while a run
owns work outside its own process tree, such as scheduler jobs. If the CLI
dies before cancelling that work, the supervisor runs ``adagio cleanup PATH``.
The contents are private to the CLI: supervisors only pass the path around.

Exactly one process owns a run at a time: the CLI running it, or a cleanup
after the CLI has died. Ownership is a lock on ``PATH.lock``, which the kernel
releases however its holder dies, so a lock nobody holds proves the owner is
gone. The record must therefore live on local disk, and cleanup must run on
the same host as the run: other hosts may not see the lock.
"""

from __future__ import annotations

import json
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from adagio.executors.task_contract import read_json_file, write_json_file

RUN_RECORD_VERSION = 1


class RunOwned(RuntimeError):
    """A live process owns the run, so nothing else may act on it."""


@dataclass(frozen=True)
class RunRecord:
    executor: str
    registry: Path


def write_run_record(path: Path, *, executor: str, registry: Path) -> None:
    write_json_file(
        path,
        {"version": RUN_RECORD_VERSION, "executor": executor, "registry": str(registry)},
    )


def read_run_record(path: Path) -> RunRecord | None:
    """Return the record at ``path``, or None when the run left nothing behind."""
    if not path.exists():
        return None
    data = read_json_file(path)
    registry = Path(str(data.get("registry", "")))
    if (
        data.get("version") != RUN_RECORD_VERSION
        or not isinstance(data.get("executor"), str)
        or not registry.is_absolute()
    ):
        raise ValueError(f"Unrecognized run record: {path}.")
    return RunRecord(executor=data["executor"], registry=registry)


@contextmanager
def owning_run(path: Path) -> Iterator[None]:
    """Own the run behind the record ``path`` until the block exits.

    Raises ``RunOwned`` when a live process already owns it. Whoever removes
    the record also removes the lock, while still holding it.
    """
    lock = path.with_name(path.name + ".lock")
    descriptor = _acquire(lock)
    try:
        os.ftruncate(descriptor, 0)
        os.write(
            descriptor,
            json.dumps({"host": socket.gethostname(), "pid": os.getpid()}).encode(),
        )
        yield
    finally:
        if not path.exists():
            lock.unlink(missing_ok=True)
        os.close(descriptor)


def _acquire(lock: Path) -> int:
    import fcntl

    while True:
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            owner = lock.read_text(errors="replace").strip() or "unknown"
            os.close(descriptor)
            raise RunOwned(f"A running process owns this run ({owner}).") from None
        except OSError as error:
            os.close(descriptor)
            raise RuntimeError(
                f"Cannot lock {lock}: {error}. The run record must be on a local "
                "filesystem that supports file locks."
            ) from error
        # A previous owner may have removed the lock file between our open and
        # our lock; only a lock on the file still at that path counts.
        try:
            current = os.path.samestat(os.fstat(descriptor), os.stat(lock))
        except FileNotFoundError:
            current = False
        if not current:
            os.close(descriptor)
            continue
        if not _excludes_others(lock):
            os.close(descriptor)
            raise RuntimeError(
                f"{lock.parent} does not enforce file locks. The run record must "
                "be on a local filesystem that does."
            )
        return descriptor


def _excludes_others(lock: Path) -> bool:
    """Whether the lock just taken keeps a second holder out.

    Some filesystems, such as network mounts and container bind mounts, accept
    a lock without enforcing it, which would let two owners act at once.
    """
    import fcntl

    probe = os.open(lock, os.O_RDWR)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    finally:
        os.close(probe)
    return False
