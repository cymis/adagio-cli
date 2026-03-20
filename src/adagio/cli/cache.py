import argparse
import re
import shutil
from pathlib import Path

from rich.console import Console

from ..executors.cache_support import CACHE_DIR_HELP, resolve_cache_dir_path

QIIME_CACHE_CONTENTS = {"VERSION", "data", "keys", "pools", "processes"}
QIIME_CACHE_VERSION_RE = re.compile(r"^QIIME 2\ncache: v?\d+\nframework: 20\d\d\.\d+\Z")


def run_cache(argv: list[str], *, console: Console) -> None:
    parser = argparse.ArgumentParser(
        prog="adagio cache",
        description="Manage Adagio's shared QIIME cache directory.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    clear_parser = subparsers.add_parser(
        "clear",
        help="Delete an existing cache directory.",
        description=(
            "Delete an existing QIIME cache directory. "
            "Only run this when no jobs are actively using the cache."
        ),
    )
    clear_parser.add_argument(
        "--cache-dir",
        required=True,
        help=CACHE_DIR_HELP,
    )

    opts = parser.parse_args(argv)

    if opts.command == "clear":
        cache_dir = resolve_cache_dir_path(
            cwd=Path.cwd().resolve(),
            raw_value=opts.cache_dir,
        )
        _clear_cache(cache_dir=cache_dir, console=console)
        return

    raise SystemExit(f"Unknown cache command: {opts.command}")


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

    if not QIIME_CACHE_VERSION_RE.fullmatch(version_text):
        raise SystemExit(f"Path is not a QIIME cache: {cache_dir}")
