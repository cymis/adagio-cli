import json
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
    prefix: str | None = None
    platform: str | None = None
    conda_executable: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_environment_key(cls, data: Any) -> Any:
        # The name-based ``environment`` field is gone. Without this check
        # pydantic's default extra-ignore would silently drop a legacy env
        # selection and fall back to the default environment.
        if isinstance(data, dict) and "environment" in data:
            raise ValueError(
                "Conda environments are referenced by absolute path now. "
                'Replace environment = "<name>" with prefix = "/path/to/env" '
                "(conda env list shows each environment's path)."
            )
        return data

    @model_validator(mode="after")
    def _validate_reference_fields(self) -> "EnvironmentOverride":
        configured = [
            name
            for name, value in (
                ("image", self.image),
                ("reference", self.reference),
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


class AdagioRunConfig(BaseModel):
    version: int = 1
    defaults: EnvironmentOverride = Field(default_factory=EnvironmentOverride)
    plugins: dict[str, EnvironmentOverride] = Field(default_factory=dict)
    tasks: dict[str, EnvironmentOverride] = Field(default_factory=dict)


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
