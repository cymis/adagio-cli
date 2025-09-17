"""Hello module."""

from pathlib import Path


def hello(input_file: Path) -> None:
    """Hello."""
    print(input_file)
