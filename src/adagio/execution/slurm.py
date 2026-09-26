"""Slurm placement for opaque prepared invocations, never local fallback."""

import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

from adagio.executors.container_support import is_uri, manifest_referenced_host_paths
from adagio.executors.task_contract import read_json_file, write_json_file

ACTIVE = {
    "PENDING",
    "RUNNING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
    "RESIZING",
    "REQUEUED",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "SIGNALING",
    "STAGE_OUT",
}
SUCCESS = "COMPLETED"
TERMINAL = {
    SUCCESS,
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
}


def memory_megabytes(value: str) -> int:
    match = re.fullmatch(r"([\d.]+)\s*([KMGTPE]?)(i?)B", value, re.IGNORECASE)
    if match is None:
        raise ValueError(f"Invalid memory quantity: {value!r}.")
    amount, prefix, binary = match.groups()
    exponent = " KMGTPE".index(prefix.upper()) if prefix else 0
    size = Decimal(amount) * (1024 if binary else 1000) ** exponent
    return max(1, int((size / (1024**2)).to_integral_value(rounding=ROUND_CEILING)))


@dataclass
class SlurmHandle:
    job_id: str
    cluster: str | None
    prepared: object
    state: str = "PENDING"
    exit_code: str | None = None
    unknown_since: float | None = None


class SlurmBackend:
    def __init__(
        self,
        *,
        config,
        run_dir: Path,
        command_runner=subprocess.run,
        clock=time.monotonic,
        accounting_timeout=120,
        command_timeout=10,
    ):
        self.config = config
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.registry_path = run_dir / "submissions.json"
        self.registry = {"version": 1, "run_dir": str(run_dir), "attempts": {}}
        self.command_runner = command_runner
        self.clock = clock
        self.accounting_timeout = accounting_timeout
        self.command_timeout = command_timeout
        self.handles = []
        self.stopped = False
        self._save()

    def _save(self):
        write_json_file(self.registry_path, self.registry)

    def _command(self, argv):
        # SBATCH_* env vars override defaults. Do not inherit scheduler controls
        # or hosted credentials into submissions. Compute jobs use export=NONE.
        env = {k: v for k, v in os.environ.items() if not k.startswith("SBATCH_")}
        result = self.command_runner(
            argv,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.command_timeout,
            env=env,
        )
        if result.returncode:
            raise RuntimeError(
                f"Slurm command {argv[0]} failed ({result.returncode}): {result.stderr.strip()}"
            )
        return result.stdout.strip()

    def submit(self, prepared, resources):
        if self.stopped:
            raise RuntimeError("Slurm submissions have stopped for this run.")
        _validate_shared_inputs(prepared)
        spec = read_json_file(prepared.spec_path)
        spec["attempt_id"] = prepared.attempt_id
        write_json_file(prepared.spec_path, spec)
        script = prepared.request.work_path / "submit.sh"
        script.write_text(_batch_script(prepared), encoding="utf-8")
        options = self.config.slurm
        job_name = "adagio-" + prepared.attempt_id
        argv = [
            "sbatch",
            "--parsable",
            "--nodes=1",
            "--ntasks=1",
            "--no-requeue",
            "--export=NONE",
            f"--job-name={job_name}",
            f"--chdir={prepared.cwd}",
            f"--output={prepared.log_path}",
            f"--error={prepared.log_path}",
            f"--cpus-per-task={resources.cpus}",
        ]
        if resources.memory:
            argv.append(f"--mem={memory_megabytes(resources.memory)}M")
        for field, option in [
            ("partition", "partition"),
            ("account", "account"),
            ("time_limit", "time"),
            ("qos", "qos"),
        ]:
            value = getattr(options, field)
            if value is not None:
                argv.append(f"--{option}={value}")
        argv.extend(options.extra_args)
        argv.append(str(script))
        entry = {
            "node_id": prepared.node_id or prepared.request.task.id,
            "binding": prepared.binding,
            "job_name": job_name,
            "state": "submitting",
            "argv": argv,
            "script": str(script),
            "log_path": str(prepared.log_path),
            "job_id": None,
            "cluster": None,
        }
        self.registry["attempts"][prepared.attempt_id] = entry
        self._save()  # intent before RPC: ambiguous submission must never be retried
        try:
            response = self._command(argv)
            match = re.fullmatch(r"([0-9]+)(?:;([A-Za-z0-9_.-]+))?", response)
            if not match:
                raise RuntimeError(f"Invalid sbatch --parsable response: {response!r}.")
        except BaseException as error:
            entry.update(state="submission-uncertain", error=str(error))
            self.stopped = True
            self._save()
            raise RuntimeError(
                f"Slurm submission could not be confirmed for {job_name}. It was not retried. Reconcile by job name; registry: {self.registry_path}. {error}"
            ) from error
        job_id, cluster = match.groups()
        handle = SlurmHandle(job_id, cluster, prepared)
        self.handles.append(handle)
        entry.update(state="PENDING", job_id=job_id, cluster=cluster)
        self._save()
        return handle

    def poll(self, handles):
        # Batch by cluster; allocation rows only (never .batch/.extern steps).
        now = self.clock()
        for cluster in {h.cluster for h in handles}:
            group = [h for h in handles if h.cluster == cluster]
            ids = ",".join(h.job_id for h in group)
            cluster_args = [f"--clusters={cluster}"] if cluster else []
            try:
                queue = self._command(
                    [
                        "squeue",
                        "--noheader",
                        f"--jobs={ids}",
                        "--format=%i|%T",
                        *cluster_args,
                    ]
                )
                active = {}
                for line in queue.splitlines():
                    job, _, state = line.strip().partition("|")
                    if job in {h.job_id for h in group}:
                        active[job] = state
                absent = [
                    h
                    for h in group
                    if h.job_id not in active or active[h.job_id] not in ACTIVE
                ]
                accounting = {}
                if absent:
                    rows = self._command(
                        [
                            "sacct",
                            "--noheader",
                            "--parsable2",
                            "--allocations",
                            f"--jobs={','.join(h.job_id for h in absent)}",
                            "--format=JobIDRaw,State,ExitCode",
                            *cluster_args,
                        ]
                    )
                    for line in rows.splitlines():
                        parts = line.strip().split("|")
                        if (
                            len(parts) >= 3
                            and parts[1].strip()
                            and parts[0] in {h.job_id for h in absent}
                        ):
                            accounting[parts[0]] = (
                                parts[1].split()[0].rstrip("+"),
                                parts[2],
                            )
                for handle in group:
                    state, code = accounting.get(
                        handle.job_id, (active.get(handle.job_id), None)
                    )
                    if state in ACTIVE or (
                        state in TERMINAL
                        and code is not None
                        and re.fullmatch(r"\d+:\d+", code)
                    ):
                        handle.state, handle.exit_code = state, code
                        handle.unknown_since = None
                        self.registry["attempts"][handle.prepared.attempt_id].update(
                            state=state, exit_code=code
                        )
                    else:
                        self._unknown(
                            handle,
                            now,
                            "queue/accounting has no confirmed state and exit code",
                        )
            except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                for handle in group:
                    self._unknown(handle, now, str(error))
        self._save()
        return {h.job_id: h.state for h in handles}

    def _unknown(self, handle, now, reason):
        if handle.unknown_since is None:
            handle.unknown_since = now
        if now - handle.unknown_since >= self.accounting_timeout:
            raise RuntimeError(
                f"Slurm job {handle.job_id} state is unknown: {reason}. Disappearance is not success. Registry: {self.registry_path}."
            )

    def collect(self, handle):
        if handle.state != SUCCESS or handle.exit_code != "0:0":
            raise RuntimeError(
                f"Slurm job {handle.job_id} ended {handle.state} (exit {handle.exit_code}). Logs: {handle.prepared.log_path}"
            )
        return handle.prepared.collect()

    def cancel(self, handles):
        self.stopped = True
        errors = []
        for cluster in {h.cluster for h in handles}:
            group = [
                h for h in handles if h.cluster == cluster and h.state not in TERMINAL
            ]
            if not group:
                continue
            try:
                self._command(
                    [
                        "scancel",
                        *([f"--clusters={cluster}"] if cluster else []),
                        *[h.job_id for h in group],
                    ]
                )
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        # Bounded confirmation; registry retains unconfirmed identities for runtime fallback.
        deadline = self.clock() + 10
        remaining = [h for h in handles if h.state not in TERMINAL]
        while remaining and self.clock() < deadline:
            try:
                self.poll(remaining)
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
                break
            remaining = [h for h in remaining if h.state not in TERMINAL]
            if remaining:
                time.sleep(0.2)
        if remaining:
            errors.append(
                "Cancellation not confirmed for Slurm jobs: "
                + ", ".join(h.job_id for h in remaining)
            )
        uncertain = [
            a["job_name"]
            for a in self.registry["attempts"].values()
            if a["state"] == "submission-uncertain"
        ]
        if uncertain:
            errors.append(
                "Submission requires reconciliation by exact job name: "
                + ", ".join(uncertain)
            )
        self._save()
        return errors


def _shared_paths(prepared):
    request = prepared.request
    paths = [request.cwd, request.work_path, prepared.spec_path]
    for value in [
        *request.archive_inputs.values(),
        *request.metadata_inputs.values(),
        *(v for values in request.archive_collection_inputs.values() for v in values),
    ]:
        if is_uri(value):
            raise ValueError(
                "Slurm inputs must be shared filesystem paths; remote URLs are unsupported."
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
        / ".adagio-container-python"
        / "adagio"
        / "cli"
        / "task_exec.py"
    )
    return paths


def _validate_shared_inputs(prepared):
    for path in _shared_paths(prepared):
        if not path.is_absolute() or not path.exists():
            raise ValueError(
                f"Slurm shared path is missing on the submit host: {path}. Compute-node visibility is checked inside the job."
            )


def _batch_script(prepared):
    # Export only explicit worker environment, never hosted control-plane credentials.
    assignments = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONNOUSERSITE": "1",
    }
    for key in ("PYTHONPATH", "PYTHONWARNINGS"):
        if prepared.env and key in prepared.env:
            assignments[key] = prepared.env[key]
    lines = ["#!/bin/sh", "set -eu", "umask 077"]
    for path in _shared_paths(prepared):
        quoted = shlex.quote(str(path))
        message = shlex.quote(
            f"Slurm shared path is unavailable on compute host: {path}"
        )
        lines.append(f"test -r {quoted} || {{ echo {message} >&2; exit 72; }}")
    lines.append(
        "exec env -i "
        + " ".join(shlex.quote(f"{k}={v}") for k, v in assignments.items())
        + " "
        + shlex.join(prepared.command)
    )
    return "\n".join(lines) + "\n"
