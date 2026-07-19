from dataclasses import dataclass
from pathlib import Path

DEFAULT_RECYCLE_POOL = "adagio-recycle"

CACHE_DIR_HELP = "Path to the shared QIIME cache directory. Required."
REUSE_HELP = (
    "Reuse matching prior task results from the selected cache. Enabled by default."
)
RECYCLE_POOL_HELP = (
    "Name of the cache recycle pool to reuse task results from. Defaults to "
    f"{DEFAULT_RECYCLE_POOL!r}. Lineage-scoped pools (e.g. 'pipeline:<id>' or "
    "'job:<id>') let separate runs share or isolate their caches."
)


@dataclass(frozen=True)
class ExecutionCacheConfig:
    cache_dir: Path
    recycle_pool: str | None = None
    no_reuse_nodes: frozenset[str] = frozenset()

    def recycle_pool_for(self, node_id: str) -> str | None:
        """Return the ordinary pool unless this node must execute fresh."""
        if node_id in self.no_reuse_nodes:
            return None
        return self.recycle_pool


def resolve_cache_config(
    *,
    cwd: Path,
    cache_dir: str | Path | None,
    reuse: bool,
    recycle_pool: str | None = None,
    no_reuse_nodes: set[str] | frozenset[str] | None = None,
) -> ExecutionCacheConfig:
    """Resolve the cache directory and the recycle pool for a run.

    ``recycle_pool`` overrides :data:`DEFAULT_RECYCLE_POOL` when supplied (design
    §1.6: lineage-scoped pools such as ``pipeline:<id>`` / ``job:<id>``). When
    ``reuse`` is disabled the pool is always ``None`` regardless of the override.
    Default behavior (override absent) is unchanged.
    """
    resolved_cache_dir = resolve_cache_dir_path(cwd=cwd, raw_value=cache_dir)
    resolved_cache_dir.parent.mkdir(parents=True, exist_ok=True)
    if not reuse:
        resolved_recycle_pool = None
    elif recycle_pool:
        resolved_recycle_pool = recycle_pool
    else:
        resolved_recycle_pool = DEFAULT_RECYCLE_POOL

    return ExecutionCacheConfig(
        cache_dir=resolved_cache_dir,
        recycle_pool=resolved_recycle_pool,
        no_reuse_nodes=frozenset(no_reuse_nodes or ()),
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
