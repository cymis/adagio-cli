from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

from ..executors.base import TaskEnvironmentOverride

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


class EnvironmentOverride(BaseModel):
    kind: str | None = None
    image: str | None = None
    reference: str | None = None
    environment: str | None = None
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
                ("reference", self.reference),
                ("environment", self.environment),
                ("prefix", self.prefix),
            )
            if value is not None
        ]
        if len(configured) > 1:
            names = ", ".join(configured)
            raise ValueError(f"Only one environment reference field may be set: {names}")
        return self

    def to_task_environment_override(self) -> TaskEnvironmentOverride | None:
        options = dict(self.options)
        reference = self.reference if self.reference is not None else self.image
        if self.environment is not None:
            reference = self.environment
            options["conda_reference_type"] = "environment"
        if self.prefix is not None:
            reference = self.prefix
            options["conda_reference_type"] = "prefix"
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


class AdagioRunConfig(BaseModel):
    version: int = 1
    defaults: EnvironmentOverride = Field(default_factory=EnvironmentOverride)
    plugins: dict[str, EnvironmentOverride] = Field(default_factory=dict)
    tasks: dict[str, EnvironmentOverride] = Field(default_factory=dict)


def load_run_config(path: Path | None) -> AdagioRunConfig | None:
    if path is None:
        return None

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Invalid config file: expected a TOML table.")

    return AdagioRunConfig.model_validate(data)


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
