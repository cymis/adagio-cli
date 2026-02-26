from __future__ import annotations

import importlib.abc
import importlib.resources
from pathlib import Path
import tempfile
import zipapp


def _copy_tree_to_root(
    source: importlib.abc.Traversable,
    target_dir: Path,
    *,
    skip_main_module: bool = False,
):
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in source.iterdir():
        name = entry.name
        if name == "__pycache__":
            continue
        if entry.is_dir():
            _copy_tree_to_root(entry, target_dir / name)
            continue
        if name.endswith(".pyc"):
            continue
        if name.startswith("test_") and name.endswith(".py"):
            continue
        if skip_main_module and name == "__main__.py":
            continue
        (target_dir / name).write_bytes(entry.read_bytes())


def build_zipapp_from_subpackage(
    target: Path,
    subpackage: str,
    *,
    interpreter: str = "/usr/bin/env python3",
):
    parts = subpackage.split(".")
    if not parts:
        raise ValueError("subpackage must not be empty")

    source_tree = importlib.resources.files(subpackage)
    source_main = source_tree.joinpath("__main__.py")
    if not source_main.is_file():
        raise ValueError(f"subpackage {subpackage} must define __main__.py")

    with tempfile.TemporaryDirectory(prefix="adagio-zipapp-build-") as build_tmp:
        build_root = Path(build_tmp)

        package_root = build_root.joinpath(*parts)
        _copy_tree_to_root(source_tree, package_root, skip_main_module=True)

        # Parent packages are generated as stubs so absolute imports resolve
        # without pulling unrelated code into the embedded artifact.
        for i in range(1, len(parts)):
            parent = build_root.joinpath(*parts[:i])
            parent.mkdir(parents=True, exist_ok=True)
            init_file = parent / "__init__.py"
            if not init_file.exists():
                init_file.write_text("", encoding="utf-8")
        package_init = package_root / "__init__.py"
        if not package_init.exists():
            package_init.write_text("", encoding="utf-8")

        (build_root / "__main__.py").write_bytes(source_main.read_bytes())

        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        zipapp.create_archive(
            source=build_root,
            target=target,
            interpreter=interpreter,
        )
