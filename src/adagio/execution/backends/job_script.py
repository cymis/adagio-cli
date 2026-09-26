"""The shell script a batch scheduler runs for one prepared task invocation.

It is scheduler-neutral: it checks that every path the task needs is visible on
the compute host, then replaces itself with the task command under a clean
environment. Hosted control-plane credentials in the submitting process's
environment are never exported to compute jobs.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import TYPE_CHECKING

from adagio.executors.container_support import (
    STAGED_CONTAINER_PYTHON_ROOT,
    is_uri,
    manifest_referenced_host_paths,
)

if TYPE_CHECKING:
    from adagio.executors.prepared import PreparedInvocation

#: Exit status when a shared path is missing on the compute host.
MISSING_SHARED_PATH_EXIT = 72


def shared_paths(prepared: PreparedInvocation) -> list[Path]:
    """Every path a task reads or writes, which must be shared with compute hosts."""
    request = prepared.request
    paths = [request.cwd, request.work_path, prepared.spec_path]
    for value in [
        *request.archive_inputs.values(),
        *request.metadata_inputs.values(),
        *(v for values in request.archive_collection_inputs.values() for v in values),
    ]:
        if is_uri(value):
            raise ValueError(
                "Batch inputs must be shared filesystem paths; remote URLs are unsupported."
            )
        paths.append(Path(value))
    paths.extend(
        manifest_referenced_host_paths(
            archive_inputs=request.archive_inputs,
            materializations=request.archive_input_materializations,
        )
    )
    if request.cache_path:
        cache = Path(request.cache_path)
        paths.append(cache if cache.exists() else cache.parent)
    paths.append(Path(prepared.image_ref))
    paths.append(
        request.work_path
        / STAGED_CONTAINER_PYTHON_ROOT
        / "adagio"
        / "cli"
        / "task_exec.py"
    )
    return paths


def check_shared_paths(prepared: PreparedInvocation) -> None:
    """Fail before submission when a needed path is missing on this host."""
    for path in shared_paths(prepared):
        if not path.is_absolute() or not path.exists():
            raise ValueError(
                f"Shared path is missing on the submit host: {path}. "
                "Compute-host visibility is checked inside the job."
            )


def render_job_script(prepared: PreparedInvocation) -> str:
    assignments = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONNOUSERSITE": "1",
    }
    for key in ("PYTHONPATH", "PYTHONWARNINGS"):
        if prepared.env and key in prepared.env:
            assignments[key] = prepared.env[key]
    lines = ["#!/bin/sh", "set -eu", "umask 077"]
    for path in shared_paths(prepared):
        quoted = shlex.quote(str(path))
        message = shlex.quote(f"Shared path is unavailable on compute host: {path}")
        lines.append(
            f"test -r {quoted} || "
            f"{{ echo {message} >&2; exit {MISSING_SHARED_PATH_EXIT}; }}"
        )
    lines.append(
        "exec env -i "
        + " ".join(shlex.quote(f"{k}={v}") for k, v in assignments.items())
        + " "
        + shlex.join(prepared.command)
    )
    return "\n".join(lines) + "\n"
