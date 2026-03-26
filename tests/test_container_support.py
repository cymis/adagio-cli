import tempfile
import unittest
from pathlib import Path

from adagio.executors.container_support import (
    STAGED_CONTAINER_PYTHON_ROOT,
    container_python_root,
)


class ContainerPythonRootTests(unittest.TestCase):
    def test_prefers_repo_src_tree_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            src_root = root / "src"
            package_dir = src_root / "adagio"
            module_file = package_dir / "executors" / "container_support.py"
            work_path = root / "work"

            (package_dir / "executors").mkdir(parents=True)
            work_path.mkdir()
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            module_file.write_text("", encoding="utf-8")

            result = container_python_root(work_path=work_path, module_file=module_file)

            self.assertEqual(result, src_root)
            self.assertFalse((work_path / STAGED_CONTAINER_PYTHON_ROOT).exists())

    def test_stages_only_adagio_package_from_site_packages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            site_packages = root / "site-packages"
            package_dir = site_packages / "adagio"
            module_file = package_dir / "executors" / "container_support.py"
            work_path = root / "work"

            (package_dir / "executors").mkdir(parents=True)
            (package_dir / "cli").mkdir()
            (site_packages / "psutil").mkdir(parents=True)
            work_path.mkdir()

            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            (package_dir / "cli" / "task_exec.py").write_text(
                "VALUE = 1\n", encoding="utf-8"
            )
            module_file.write_text("", encoding="utf-8")
            (site_packages / "psutil" / "__init__.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            )

            result = container_python_root(work_path=work_path, module_file=module_file)

            staged_root = work_path / STAGED_CONTAINER_PYTHON_ROOT
            self.assertEqual(result, staged_root)
            self.assertTrue((staged_root / "adagio" / "__init__.py").exists())
            self.assertTrue((staged_root / "adagio" / "cli" / "task_exec.py").exists())
            self.assertFalse((staged_root / "psutil").exists())
