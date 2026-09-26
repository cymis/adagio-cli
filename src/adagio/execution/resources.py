"""What each task asks for, and the policy that decides it.

A request describes one task execution. Backends that place work (batch
schedulers) translate it into scheduler options; the local backend runs one
task at a time and does not enforce it.
"""

import re
from decimal import ROUND_CEILING, Decimal
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

_MEMORY_PATTERN = re.compile(r"^(?P<amount>\d+(?:\.\d+)?|\.\d+)\s*(?P<unit>[A-Za-z]+)$")
_MEMORY_UNITS = {
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "pb": 10**15,
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
    "pib": 2**50,
}


def memory_bytes(value: str) -> int:
    """Parse a unit-bearing memory quantity such as ``"8 GiB"`` into bytes."""
    match = _MEMORY_PATTERN.fullmatch(value.strip())
    unit = _MEMORY_UNITS.get(match.group("unit").lower()) if match else None
    if match is None or unit is None:
        raise ValueError(
            'Memory must be a positive, unit-bearing quantity such as "8 GiB".'
        )
    amount = Decimal(match.group("amount")) * unit
    if amount <= 0:
        raise ValueError("Memory must be greater than zero.")
    return int(amount.to_integral_value(rounding=ROUND_CEILING))


class TaskResourceRequirements(BaseModel):
    """Requested shape for one task execution; unset fields defer to defaults."""

    model_config = ConfigDict(extra="forbid")

    cpus: int | None = Field(default=None, ge=1, strict=True)
    memory: str | None = None

    @field_validator("memory")
    @classmethod
    def _validate_memory(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        memory_bytes(normalized)
        return normalized


class ResourceRequirementsConfig(BaseModel):
    """The ``[resources]`` table: run-wide defaults and per-task overrides."""

    model_config = ConfigDict(extra="forbid")

    defaults: TaskResourceRequirements = Field(default_factory=TaskResourceRequirements)
    tasks: dict[str, TaskResourceRequirements] = Field(default_factory=dict)


class ResourcePolicy(Protocol):
    """Decide what one task requests.

    Configuration is the only source today. A policy that sizes work from
    action hints or input sizes implements this same method.
    """

    def for_task(self, task: Any) -> TaskResourceRequirements: ...


class ConfiguredResourcePolicy:
    """Take each field from the task's override, then the defaults; one CPU at least."""

    def __init__(self, config: ResourceRequirementsConfig | None = None) -> None:
        self._config = config or ResourceRequirementsConfig()

    def for_task(self, task: Any) -> TaskResourceRequirements:
        override = self._config.tasks.get(task.id, TaskResourceRequirements())
        defaults = self._config.defaults
        return TaskResourceRequirements(
            cpus=override.cpus or defaults.cpus or 1,
            memory=override.memory or defaults.memory,
        )
