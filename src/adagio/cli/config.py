import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..executors.base import TaskEnvironmentOverride

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


class EnvironmentOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str | None = None
    image: str | None = None
    prefix: str | None = None
    platform: str | None = None
    conda_executable: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_reference_fields(self) -> "EnvironmentOverride":
        configured = [
            name
            for name, value in (
                ("image", self.image),
                ("prefix", self.prefix),
            )
            if value is not None
        ]
        if len(configured) > 1:
            names = ", ".join(configured)
            raise ValueError(
                f"Only one environment reference field may be set: {names}"
            )
        # Stripped BEFORE the absoluteness check and stored stripped, matching
        # the UI's trimmed validation and the backend validator - otherwise
        # " /opt/env" is rejected after the UI called it valid, and
        # "/opt/env " names a different directory with a trailing space.
        # Absoluteness is checked before ``.resolve()`` so a relative spelling
        # fails loudly instead of silently binding to the CLI's working
        # directory. ``~`` is fine - expanduser() yields an absolute path.
        if self.prefix is not None:
            self.prefix = self.prefix.strip()
            if not Path(self.prefix).expanduser().is_absolute():
                raise ValueError(
                    f'Conda prefix must be an absolute path; got "{self.prefix}".'
                )
        return self

    def to_task_environment_override(self) -> TaskEnvironmentOverride | None:
        options = dict(self.options)
        reference = self.image
        if self.prefix is not None:
            reference = str(Path(self.prefix).expanduser().resolve())
        if self.conda_executable is not None:
            options["conda_executable"] = self.conda_executable

        if (
            self.kind is None
            and reference is None
            and self.platform is None
            and not options
        ):
            return None

        return TaskEnvironmentOverride(
            kind=self.kind,
            reference=reference,
            platform=self.platform,
            options=options or None,
        )


_MEMORY_REQUEST_PATTERN = re.compile(
    r"^(?P<amount>(?:\d+(?:\.\d+)?|\.\d+))\s*"
    r"(?:B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)$",
    re.IGNORECASE,
)


class TaskResourceRequirements(BaseModel):
    """Requested shape for one task execution.

    These values are parsed and retained for forward compatibility. The serial
    executor intentionally does not apply them yet.
    """

    model_config = ConfigDict(extra="forbid")

    cpus: int | None = Field(default=None, ge=1, strict=True)
    memory: str | None = None

    @field_validator("memory")
    @classmethod
    def _validate_memory(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        match = _MEMORY_REQUEST_PATTERN.fullmatch(normalized)
        if match is None:
            raise ValueError(
                'Memory must be a positive, unit-bearing quantity such as "8 GiB".'
            )
        try:
            amount = Decimal(match.group("amount"))
        except InvalidOperation as err:  # pragma: no cover - guarded by regex
            raise ValueError("Memory amount is invalid.") from err
        if amount <= 0:
            raise ValueError("Memory must be greater than zero.")
        return normalized


class ResourceRequirementsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    defaults: TaskResourceRequirements = Field(default_factory=TaskResourceRequirements)
    tasks: dict[str, TaskResourceRequirements] = Field(default_factory=dict)

    def for_task(self, task_id: str) -> TaskResourceRequirements:
        override = self.tasks.get(task_id, TaskResourceRequirements())
        return TaskResourceRequirements(
            cpus=override.cpus or self.defaults.cpus or 1,
            memory=override.memory or self.defaults.memory,
        )


# Exact long options only. An allowlist also blocks Slurm's abbreviated options,
# short aliases, replacement commands and options added by future Slurm releases.
SLURM_EXTRA_OPTIONS = frozenset(
    {"constraint", "reservation", "licenses", "prefer", "nice"}
)


class SlurmConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partition: str | None = None
    account: str | None = None
    time_limit: str | None = None
    qos: str | None = None
    extra_args: list[str] = Field(default_factory=list)

    @field_validator("partition", "account", "qos")
    @classmethod
    def clean_name(cls, value):
        if value is None:
            return None
        if not re.fullmatch(r"[A-Za-z0-9_.@,+-]+", value):
            raise ValueError(
                "Slurm names must contain only letters, digits, _, ., @, comma, + or -."
            )
        return value

    @field_validator("time_limit")
    @classmethod
    def valid_time(cls, value):
        if value is not None and not re.fullmatch(
            r"(?:[0-9]+-)?[0-9]+(?::[0-5][0-9]){0,2}", value
        ):
            raise ValueError("Time limit must use minutes, HH:MM:SS or D-HH:MM:SS.")
        if value is not None and not any(c in "123456789" for c in value):
            raise ValueError("Time limit must be greater than zero.")
        return value

    @field_validator("extra_args")
    @classmethod
    def safe_arguments(cls, values):
        seen = set()
        for value in values:
            option, separator, argument = value.partition("=")
            name = option.removeprefix("--")
            if (
                not option.startswith("--")
                or name not in SLURM_EXTRA_OPTIONS
                or not separator
                or not argument
                or any(c in value for c in "\n\r\0")
            ):
                raise ValueError(
                    "Additional Slurm options must use --option=value; allowed options: "
                    + ", ".join(sorted(SLURM_EXTRA_OPTIONS))
                    + "."
                )
            if name in seen:
                raise ValueError(f"Duplicate additional Slurm option: {option}.")
            seen.add(name)
        return values


class ExecutorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["serial", "slurm"] = "serial"
    work_dir: str | None = None
    max_in_flight: int = Field(default=8, ge=1, strict=True)
    slurm: SlurmConfig = Field(default_factory=SlurmConfig)

    @model_validator(mode="after")
    def shared_work_directory(self):
        if self.kind == "slurm" and (
            not self.work_dir or not self.work_dir.startswith("/")
        ):
            raise ValueError(
                "Slurm requires an absolute shared work directory, visible at the same path on submit and compute hosts."
            )
        if self.work_dir and any(c in self.work_dir for c in "\n\r\0"):
            raise ValueError(
                "Shared work directory must not contain control characters."
            )
        return self


class AdagioRunConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    profile: dict[str, str] | None = None
    defaults: EnvironmentOverride = Field(default_factory=EnvironmentOverride)
    plugins: dict[str, EnvironmentOverride] = Field(default_factory=dict)
    tasks: dict[str, EnvironmentOverride] = Field(default_factory=dict)
    resources: ResourceRequirementsConfig = Field(
        default_factory=ResourceRequirementsConfig
    )

    @field_validator("version", mode="before")
    @classmethod
    def supported_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("Unsupported run configuration version; expected 1.")
        return value


def load_run_config(path: Path | None) -> AdagioRunConfig | None:
    if path is None:
        return None

    data = _parse_config_text(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Invalid config file: expected a TOML table or JSON object.")

    return AdagioRunConfig.model_validate(data)


def _parse_config_text(text: str) -> Any:
    """Parse a runtime config as TOML *or* JSON (auto-detect).

    The runtime launch contract lets an adapter hand the CLI a valid
    ``AdagioRunConfig`` serialized as either TOML or JSON (design §5.7). A JSON
    document (leading ``{``/``[``) parses cleanly as JSON but not as TOML, so we
    sniff the first non-whitespace character and prefer JSON there; otherwise we
    parse TOML. Existing TOML behavior is unchanged — a TOML config never starts
    with ``{``/``[`` at document scope (``[table]`` headers do, so we fall back
    to TOML on JSON-parse failure to stay safe).
    """
    stripped = text.lstrip()
    if stripped[:1] in ("{", "["):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # A TOML file legitimately begins with an ``[table]`` header; fall
            # through to the TOML parser rather than failing outright.
            pass
    return tomllib.loads(text)


def default_environment_override(
    run_config: AdagioRunConfig | None,
) -> TaskEnvironmentOverride | None:
    if run_config is None:
        return None
    return run_config.defaults.to_task_environment_override()


def named_environment_overrides(
    raw_overrides: dict[str, EnvironmentOverride],
) -> dict[str, TaskEnvironmentOverride] | None:
    resolved = {
        name: override
        for name, raw_override in raw_overrides.items()
        if (override := raw_override.to_task_environment_override()) is not None
    }
    return resolved or None
