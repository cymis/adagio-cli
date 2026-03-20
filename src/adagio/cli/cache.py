import re
import shutil
from pathlib import Path
from typing import Annotated

from cyclopts import App, Group, Parameter
from rich.console import Console

from ..executors.cache_support import CACHE_DIR_HELP, resolve_cache_dir_path

QIIME_CACHE_CONTENTS = {"VERSION", "data", "keys", "pools", "processes"}
QIIME_CACHE_LINE_RE = re.compile(r"cache: v?\d+\Z")


def run_cache(argv: list[str], *, console: Console) -> None:
    app = App(
        name="adagio cache",
        help="Manage Adagio's shared QIIME cache directory.",
    )
    command_group = Group("Command Options", sort_key=0)

    @app.command
    def clear(
        *,
        cache_dir: Annotated[
            Path,
            Parameter(
                name=("--cache-dir",),
                group=command_group,
                help=CACHE_DIR_HELP,
            ),
        ],
    ) -> None:
        """Delete an existing QIIME cache directory."""
        resolved_cache_dir = resolve_cache_dir_path(
            cwd=Path.cwd().resolve(),
            raw_value=str(cache_dir),
        )
        _clear_cache(cache_dir=resolved_cache_dir, console=console)

    app(argv)


def _clear_cache(*, cache_dir: Path, console: Console) -> None:
    _require_qiime_cache(cache_dir)
    shutil.rmtree(cache_dir)
    console.print(f"Cleared cache directory: {cache_dir}")


def _require_qiime_cache(cache_dir: Path) -> None:
    if not cache_dir.exists():
        raise SystemExit(f"Cache directory does not exist: {cache_dir}")
    if not cache_dir.is_dir():
        raise SystemExit(f"Cache path is not a directory: {cache_dir}")

    contents = set(item.name for item in cache_dir.iterdir())
    if not contents.issuperset(QIIME_CACHE_CONTENTS):
        raise SystemExit(f"Path is not a QIIME cache: {cache_dir}")

    version_file = cache_dir / "VERSION"
    try:
        version_text = version_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(f"Could not read cache version file: {version_file}") from exc

    if not _looks_like_qiime_cache_version(version_text):
        raise SystemExit(f"Path is not a QIIME cache: {cache_dir}")


def _looks_like_qiime_cache_version(version_text: str) -> bool:
    lines = version_text.splitlines()
    if len(lines) != 3:
        return False

    if lines[0] != "QIIME 2":
        return False

    if not QIIME_CACHE_LINE_RE.fullmatch(lines[1]):
        return False

    framework_prefix = "framework: "
    return lines[2].startswith(framework_prefix) and bool(lines[2][len(framework_prefix) :].strip())
