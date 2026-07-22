import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.conda import (
    CondaTaskEnvironmentLauncher,
    _resolve_conda_executable,
)
from adagio.executors.container_support import container_python_root
from adagio.executors.task_contract import (
    build_result_manifest,
    read_json_file,
    result_manifest_path,
    task_spec_path,
    write_json_file,
)
from adagio.model.task import PluginActionTask


def _task() -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": "task-1",
            "kind": "plugin-action",
            "plugin": "demux",
            "action": "summarize",
            "inputs": {},
            "parameters": {},
            "outputs": {"visualization": {"kind": "archive", "id": "out-1"}},
        }
    )


class CondaLauncherTests(unittest.TestCase):
    def test_launch_builds_conda_run_command(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            conda_executable = root / "bin" / "conda"
            prefix = root / "envs" / "qiime2-2026.1"
            cwd.mkdir()
            work_path.mkdir()
            conda_executable.parent.mkdir()
            conda_executable.write_text("stub", encoding="utf-8")
            output_path = work_path / "summary.qzv"
            input_path = cwd / "input.qza"
            collection_input_path = cwd / "collection-input.qza"
            input_path.write_text("input", encoding="utf-8")
            collection_input_path.write_text("collection", encoding="utf-8")

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"data": str(input_path)},
                archive_collection_inputs={"tables": [str(collection_input_path)]},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = str(task_spec_path(task_id=task.id, work_path=work_path))

            def fake_run(cmd, check, cwd, env, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": str(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch(
                "adagio.executors.conda.subprocess.run",
                side_effect=fake_run,
            ) as run_mock:
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="conda",
                        reference=str(prefix),
                        options={"conda_executable": str(conda_executable)},
                    ),
                    request=request,
                )

            task_spec = read_json_file(
                task_spec_path(task_id=task.id, work_path=work_path)
            )
            command = run_mock.call_args.args[0]
            kwargs = run_mock.call_args.kwargs
            python_root = container_python_root(work_path=work_path)

        self.assertEqual(
            command,
            [
                str(conda_executable),
                "run",
                "--no-capture-output",
                "-p",
                str(prefix),
                str(prefix / "bin" / "python"),
                "-m",
                "adagio.cli.task_exec",
                "--task",
                expected_spec,
            ],
        )
        self.assertEqual(kwargs["cwd"], cwd)
        self.assertIn(str(python_root), kwargs["env"]["PYTHONPATH"])
        self.assertEqual(kwargs["env"]["PYTHONNOUSERSITE"], "1")
        self.assertEqual(task_spec["archive_inputs"], {"data": str(input_path)})
        self.assertEqual(
            task_spec["archive_collection_inputs"],
            {"tables": [str(collection_input_path)]},
        )
        self.assertEqual(result.outputs, {"visualization": str(output_path)})
        self.assertFalse(result.reused)

    def test_launch_uses_conda_prefix(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            conda_executable = root / "bin" / "conda"
            prefix = root / "envs" / "qiime2"
            cwd.mkdir()
            work_path.mkdir()
            conda_executable.parent.mkdir()
            conda_executable.write_text("stub", encoding="utf-8")
            output_path = work_path / "summary.qzv"
            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            def fake_run(cmd, check, cwd, env, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": str(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.conda.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
                patch.dict(
                    os.environ,
                    {
                        "PYTHONHOME": "/bundled/python",
                        "PYTHONPATH": "/bundled/site-packages",
                        "VIRTUAL_ENV": "/bundled/venv",
                    },
                ),
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="conda",
                        reference=str(prefix),
                        options={
                            # A stale option from an old adapter is ignored.
                            "conda_reference_type": "prefix",
                            "conda_executable": str(conda_executable),
                        },
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        child_env = run_mock.call_args.kwargs["env"]
        python_root = container_python_root(work_path=work_path)
        self.assertEqual(
            command[:5],
            [str(conda_executable), "run", "--no-capture-output", "-p", str(prefix)],
        )
        self.assertEqual(command[5], str(prefix / "bin" / "python"))
        self.assertIn("adagio.cli.task_exec", command)
        self.assertEqual(child_env["PYTHONPATH"], str(python_root))
        self.assertNotIn("PYTHONHOME", child_env)
        self.assertNotIn("VIRTUAL_ENV", child_env)

    def test_launch_requires_environment_reference(self) -> None:
        launcher = CondaTaskEnvironmentLauncher()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            request = TaskExecutionRequest(
                task=_task(),
                cwd=cwd,
                work_path=work_path,
                archive_inputs={},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(work_path / "summary.qzv")},
            )

            with self.assertRaisesRegex(
                RuntimeError,
                r"Conda task environments require an environment path\. "
                r'Set prefix = "/path/to/env"\.',
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(kind="conda", reference=""),
                    request=request,
                )


class CondaExecutableResolutionTests(unittest.TestCase):
    """Resolution order: option, ADAGIO_CONDA_EXE, CONDA_EXE, prefix-derived, PATH."""

    def _make_executable(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\n", encoding="utf-8")
        path.chmod(0o755)
        return path

    def test_resolution_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            configured = self._make_executable(root / "configured" / "conda")
            adagio_exe = self._make_executable(root / "adagio" / "conda")
            conda_exe = self._make_executable(root / "shell" / "conda")
            install_root = root / "miniforge3"
            derived = self._make_executable(install_root / "condabin" / "conda")
            path_dir = root / "on-path"
            which_hit = self._make_executable(path_dir / "conda")
            prefix = install_root / "envs" / "qiime2"
            prefix.mkdir(parents=True)

            env = {
                "ADAGIO_CONDA_EXE": str(adagio_exe),
                "CONDA_EXE": str(conda_exe),
                "PATH": str(path_dir),
            }
            with patch.dict(os.environ, env, clear=True):
                self.assertEqual(
                    _resolve_conda_executable(
                        options={"conda_executable": str(configured)},
                        prefix=str(prefix),
                    ),
                    str(configured),
                )
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(prefix)),
                    str(adagio_exe),
                )

                del os.environ["ADAGIO_CONDA_EXE"]
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(prefix)),
                    str(conda_exe),
                )

                del os.environ["CONDA_EXE"]
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(prefix)),
                    str(derived),
                )

                derived.unlink()
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(prefix)),
                    str(which_hit),
                )

    def test_prefix_derived_probes_root_bin_and_base_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            env = {"PATH": str(root / "empty")}

            # Nested env: <root>/envs/<name> resolves via the owning install.
            install_root = root / "miniforge3"
            nested_prefix = install_root / "envs" / "qiime2"
            nested_prefix.mkdir(parents=True)
            bin_conda = self._make_executable(install_root / "bin" / "conda")
            with patch.dict(os.environ, env, clear=True):
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(nested_prefix)),
                    str(bin_conda),
                )

            # Base-style prefix: the prefix itself is the install root.
            base_prefix = root / "mambaforge"
            base_conda = self._make_executable(base_prefix / "condabin" / "conda")
            with patch.dict(os.environ, env, clear=True):
                self.assertEqual(
                    _resolve_conda_executable(options={}, prefix=str(base_prefix)),
                    str(base_conda),
                )

    def test_all_sources_missing_is_an_explicit_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            prefix = root / "envs" / "qiime2"
            prefix.mkdir(parents=True)
            with patch.dict(os.environ, {"PATH": str(root / "empty")}, clear=True):
                with self.assertRaisesRegex(SystemExit, "ADAGIO_CONDA_EXE"):
                    _resolve_conda_executable(options={}, prefix=str(prefix))
