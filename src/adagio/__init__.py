from importlib.metadata import PackageNotFoundError, version


def _resolve_version() -> str:
    for dist_name in ("adagio-cli", "adagio"):
        try:
            return version(dist_name)
        except PackageNotFoundError:
            continue
    return "0.0.0"


__version__ = _resolve_version()

__all__ = ["__version__"]
