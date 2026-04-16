from dataclasses import dataclass
from pathlib import Path

DEFAULT_RECYCLE_POOL = "adagio-recycle"

CACHE_DIR_HELP = "Path to the shared QIIME cache directory. Required."
REUSE_HELP = (
    "Reuse matching prior task results from the selected cache. Enabled by default."
)


@dataclass(frozen=True)
class ExecutionCacheConfig:
    cache_dir: Path
    recycle_pool: str | None = None


def resolve_cache_config(
    *,
    cwd: Path,
    cache_dir: str | Path | None,
    reuse: bool,
) -> ExecutionCacheConfig:
    resolved_cache_dir = resolve_cache_dir_path(cwd=cwd, raw_value=cache_dir)
    resolved_cache_dir.parent.mkdir(parents=True, exist_ok=True)
    resolved_recycle_pool = DEFAULT_RECYCLE_POOL if reuse else None

    return ExecutionCacheConfig(
        cache_dir=resolved_cache_dir,
        recycle_pool=resolved_recycle_pool,
    )


def mount_path_for_cache(cache_dir: Path) -> Path:
    return cache_dir if cache_dir.exists() else cache_dir.parent


def describe_cache_config(config: ExecutionCacheConfig) -> str:
    if config.recycle_pool is None:
        return f"{config.cache_dir} (reuse disabled)"
    return f"{config.cache_dir} (reuse enabled)"


def resolve_cache_dir_path(*, cwd: Path, raw_value: str | Path | None) -> Path:
    if raw_value is None:
        raise SystemExit("Missing required --cache-dir.")

    candidate = Path(raw_value)
    candidate = candidate.expanduser()
    if not candidate.is_absolute():
        candidate = (cwd / candidate).resolve()
    else:
        candidate = candidate.resolve()

    return candidate
