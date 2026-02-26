from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import subprocess
from typing import Literal, Sequence

StepStatus = Literal["ok", "changed", "skipped", "failed"]


@dataclass(slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass(slots=True)
class InstallStep:
    name: str
    status: StepStatus
    detail: str
    command: str | None = None


@dataclass(slots=True)
class InstallReport:
    platform: str
    image: str
    runtime: str | None = None
    config_path: Path | None = None
    steps: list[InstallStep] = field(default_factory=list)

    def add(self, name: str, status: StepStatus, detail: str, command: str | None = None):
        self.steps.append(
            InstallStep(name=name, status=status, detail=detail, command=command)
        )

    @property
    def ok(self) -> bool:
        return all(step.status != "failed" for step in self.steps)


def run_command(args: Sequence[str]) -> CommandResult:
    proc = subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(
        returncode=proc.returncode,
        stdout=proc.stdout.strip(),
        stderr=proc.stderr.strip(),
    )
