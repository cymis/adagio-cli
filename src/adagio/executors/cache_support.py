from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CACHE_DIRNAME = ".adagio/cache"
DEFAULT_RECYCLE_POOL = "adagio-recycle"

CACHE_DIR_ENV_VAR = "ADAGIO_CACHE_DIR"
RECYCLE_POOL_ENV_VAR = "ADAGIO_RECYCLE_POOL"
NO_RECYCLE_ENV_VAR = "ADAGIO_NO_RECYCLE"

CACHE_DIR_HELP = (
    "Path to the shared QIIME cache Adagio should use for reusable task results. "
    "Defaults to /storage/adagio-cache when /storage exists, otherwise ./.adagio/cache."
)
RECYCLE_POOL_HELP = (
    "Named recycle pool used for task result reuse. Defaults to a persistent "
    f"pool named {DEFAULT_RECYCLE_POOL!r}."
)
NO_RECYCLE_HELP = (
    "Disable reuse of cached task results for this run while still using the selected cache."
)


@dataclass(frozen=True)
class ExecutionCacheConfig:
    cache_dir: Path
    recycle_pool: str | None = None


def validate_cache_settings(*, recycle_pool: str | None, no_recycle: bool) -> None:
    if recycle_pool is not None and no_recycle:
        raise SystemExit(
            "Cannot set --recycle-pool and --no-recycle at the same time."
        )


def resolve_cache_config(
    *,
    cwd: Path,
    cache_dir: str | Path | None,
    recycle_pool: str | None,
    no_recycle: bool,
) -> ExecutionCacheConfig:
    env_cache_dir = os.getenv(CACHE_DIR_ENV_VAR) if cache_dir is None else None
    env_recycle_pool = (
        os.getenv(RECYCLE_POOL_ENV_VAR) if recycle_pool is None else None
    )
    env_no_recycle = _is_truthy(os.getenv(NO_RECYCLE_ENV_VAR))

    resolved_cache_dir = _resolve_cache_dir(
        cwd=cwd,
        raw_value=cache_dir if cache_dir is not None else env_cache_dir,
    )
    resolved_no_recycle = no_recycle or env_no_recycle
    resolved_recycle_pool = (
        None
        if resolved_no_recycle
        else (recycle_pool or env_recycle_pool or DEFAULT_RECYCLE_POOL)
    )

    return ExecutionCacheConfig(
        cache_dir=resolved_cache_dir,
        recycle_pool=resolved_recycle_pool,
    )


def mount_path_for_cache(cache_dir: Path) -> Path:
    return cache_dir if cache_dir.exists() else cache_dir.parent


def describe_cache_config(config: ExecutionCacheConfig) -> str:
    if config.recycle_pool is None:
        return f"{config.cache_dir} (recycle disabled)"
    return f"{config.cache_dir} (pool: {config.recycle_pool})"


def _resolve_cache_dir(*, cwd: Path, raw_value: str | Path | None) -> Path:
    candidate = _default_cache_dir(cwd=cwd) if raw_value is None else Path(raw_value)
    candidate = candidate.expanduser()
    if not candidate.is_absolute():
        candidate = (cwd / candidate).resolve()
    else:
        candidate = candidate.resolve()

    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate


def _default_cache_dir(*, cwd: Path) -> Path:
    storage_root = Path("/storage")
    if storage_root.exists():
        return (storage_root / "adagio-cache").resolve()
    return (cwd / DEFAULT_CACHE_DIRNAME).resolve()


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}
