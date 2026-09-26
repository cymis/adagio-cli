"""A task invocation prepared to run elsewhere, and the checks on its result."""

import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .base import TaskExecutionRequest, TaskExecutionResult
from .container_support import host_path_from_container
from .task_contract import parse_result_manifest, read_json_file


@dataclass
class PreparedInvocation:
    """Everything needed to run one task without its launcher: a command and paths.

    ``prepare(..., shared=True)`` builds one for execution on another host; the
    result comes back only through the manifest, so ``collect`` trusts nothing
    it cannot check.
    """

    command: list[str]
    env: dict[str, str] | None
    cwd: Path
    spec_path: Path
    manifest_path: Path
    log_path: Path
    request: TaskExecutionRequest
    image_ref: str
    containerized: bool = False
    attempt_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def collect(self) -> TaskExecutionResult:
        if not self.manifest_path.is_file():
            raise RuntimeError(
                f"Task {self.request.task.id!r} completed but did not write an output manifest. Logs: {self.log_path}"
            )
        try:
            payload = read_json_file(self.manifest_path)
            if not isinstance(payload, dict) or not isinstance(
                payload.get("outputs"), dict
            ):
                raise TypeError("outputs must be an object")
            if (
                not isinstance(payload.get("metadata_outputs", {}), dict)
                or type(payload.get("reused", False)) is not bool
            ):
                raise ValueError("invalid metadata_outputs or reused field")
        except (ValueError, TypeError) as error:
            raise RuntimeError(
                f"Task {self.request.task.id!r} returned a malformed result manifest: {error}. Logs: {self.log_path}"
            ) from error
        if payload.get("attempt_id") != self.attempt_id:
            raise RuntimeError(
                f"Task {self.request.task.id!r} returned a stale or incomplete result manifest."
            )
        outputs, metadata, reused = parse_result_manifest(payload)
        resolved = []
        for actual, expected in [
            (outputs, self.request.outputs),
            (metadata, self.request.metadata_outputs or {}),
        ]:
            if set(actual) != set(expected):
                raise RuntimeError(
                    f"Task {self.request.task.id!r} returned unexpected output names: {sorted(actual)}; expected {sorted(expected)}."
                )
            paths = {}
            for name, value in actual.items():
                if not isinstance(value, str):
                    raise RuntimeError(
                        f"Task {self.request.task.id!r} returned an invalid path for {name!r}."
                    )
                path = (
                    host_path_from_container(value)
                    if self.containerized
                    else Path(value)
                )
                if not path.is_absolute() or not path.exists():
                    raise RuntimeError(
                        f"Task {self.request.task.id!r} output {name!r} is missing: {path}."
                    )
                if not path.resolve().is_relative_to(self.request.work_path.resolve()):
                    raise RuntimeError(
                        f"Task {self.request.task.id!r} output {name!r} is outside its attempt directory."
                    )
                paths[name] = str(path)
            resolved.append(paths)
        return TaskExecutionResult(
            outputs=resolved[0],
            metadata_outputs=resolved[1],
            reused=reused,
            command=self.command,
            exit_code=0,
            image_ref=self.image_ref,
            log_path=str(self.log_path),
        )
