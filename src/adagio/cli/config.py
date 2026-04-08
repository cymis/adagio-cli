from pathlib import Path

from pydantic import BaseModel, Field

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


class ImageOverride(BaseModel):
    image: str | None = None
    platform: str | None = None


class DefaultOverride(BaseModel):
    image: str | None = None
    platform: str | None = None


class AdagioRunConfig(BaseModel):
    version: int = 1
    defaults: DefaultOverride = Field(default_factory=DefaultOverride)
    plugins: dict[str, ImageOverride] = Field(default_factory=dict)
    tasks: dict[str, ImageOverride] = Field(default_factory=dict)


def load_run_config(path: Path | None) -> AdagioRunConfig | None:
    if path is None:
        return None

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Invalid config file: expected a TOML table.")

    return AdagioRunConfig.model_validate(data)
