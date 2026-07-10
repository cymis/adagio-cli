import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.apptainer import ApptainerTaskEnvironmentLauncher
from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.container_support import (
    container_python_root,
    containerize_path,
    mount_roots,
)
from adagio.executors.task_contract import (
    build_result_manifest,
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
            "plugin": "dada2",
            "action": "denoise_single",
            "inputs": {},
            "parameters": {},
            "outputs": {"table": {"kind": "archive", "id": "out-1"}},
        }
    )


def _foreign_root(path: Path) -> Path:
    """A first-level filesystem root that exists and differs from ``path``'s."""
    excluded = path.parts[1] if len(path.parts) > 1 else None
    for candidate in ("usr", "bin", "etc", "opt", "var", "lib"):
        if candidate == excluded:
            continue
        root = Path("/", candidate)
        if root.exists():
            return root
    raise unittest.SkipTest("No foreign top-level root available on this host.")


class ApptainerLauncherTests(unittest.TestCase):
    def test_launch_builds_apptainer_exec_command(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            image_path = root / "q2-dada2.sif"
            image_path.write_text("stub", encoding="utf-8")
            output_path = work_path / "table.qza"
            input_path = cwd / "input.qza"
            input_path.write_text("input", encoding="utf-8")

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"seqs": str(input_path)},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"table": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = containerize_path(
                task_spec_path(task_id=task.id, work_path=work_path)
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"table": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.apptainer.shutil.which",
                    side_effect=["/usr/bin/apptainer", None],
                ),
                patch(
                    "adagio.executors.apptainer.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
            ):
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference=str(image_path),
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        python_root = container_python_root(work_path=work_path)
        bind_targets = {
            f"{root_path}:{containerize_path(root_path)}:rw"
            for root_path in mount_roots(
                [cwd, work_path, input_path, python_root]
            )
        }

        self.assertEqual(command[0], "/usr/bin/apptainer")
        self.assertEqual(command[1], "exec")
        self.assertIn("--cleanenv", command)
        self.assertIn("--no-home", command)
        self.assertIn("--pwd", command)
        self.assertIn(containerize_path(cwd), command)
        self.assertIn(str(image_path), command)
        self.assertIn("env", command)
        self.assertIn(f"PYTHONPATH={containerize_path(python_root)}", command)
        self.assertIn("PYTHONNOUSERSITE=1", command)
        self.assertIn("python", command)
        self.assertIn("-m", command)
        self.assertIn("adagio.cli.task_exec", command)
        self.assertIn("--task", command)
        self.assertIn(expected_spec, command)
        self.assertTrue(bind_targets.issubset(set(command)))
        self.assertEqual(result.outputs, {"table": str(output_path)})
        self.assertFalse(result.reused)

    def test_launch_falls_back_to_singularity(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            image_path = root / "q2-dada2.sif"
            image_path.write_text("stub", encoding="utf-8")
            output_path = work_path / "table.qza"
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
                outputs={"table": str(output_path)},
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"table": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.apptainer.shutil.which",
                    side_effect=[None, "/usr/bin/singularity"],
                ),
                patch(
                    "adagio.executors.apptainer.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference=str(image_path),
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        self.assertEqual(command[0], "/usr/bin/singularity")

    def test_launch_mounts_manifest_referenced_fastq_roots(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            image_path = root / "q2-dada2.sif"
            image_path.write_text("stub", encoding="utf-8")
            output_path = work_path / "table.qza"

            # The manifest lives under the cwd, but the fastqs it points at live
            # under a *different* top-level root that nothing else binds.
            fastq_root = _foreign_root(cwd)
            manifest_path = cwd / "manifest.tsv"
            manifest_path.write_text(
                "sample-id\tforward-absolute-filepath\treverse-absolute-filepath\n"
                f"sample1\t{fastq_root}/adagio-reads/s1_R1.fastq.gz"
                f"\t{fastq_root}/adagio-reads/s1_R2.fastq.gz\n",
                encoding="utf-8",
            )

            request = TaskExecutionRequest(
                task=task,
                cwd=cwd,
                work_path=work_path,
                archive_inputs={"seqs": str(manifest_path)},
                archive_collection_inputs={},
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"table": str(output_path)},
                archive_input_materializations={
                    "seqs": {
                        "mode": "raw",
                        "semantic_type": (
                            "SampleData[PairedEndSequencesWithQuality]"
                        ),
                        "input_format": "PairedEndFastqManifestPhred33V2",
                        "validate_level": "max",
                    }
                },
            )

            result_path = result_manifest_path(task_id=task.id, work_path=work_path)

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    result_path,
                    build_result_manifest(
                        outputs={"table": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                patch(
                    "adagio.executors.apptainer.shutil.which",
                    side_effect=["/usr/bin/apptainer", None],
                ),
                patch(
                    "adagio.executors.apptainer.subprocess.run",
                    side_effect=fake_run,
                ) as run_mock,
            ):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference=str(image_path),
                    ),
                    request=request,
                )

        command = run_mock.call_args.args[0]
        fastq_bind = f"{fastq_root}:{containerize_path(fastq_root)}:rw"
        self.assertIn(fastq_bind, command)
        # The fastq root genuinely differs from the cwd's top-level root.
        self.assertNotEqual(fastq_root.parts[1], cwd.parts[1])

    def test_launch_rejects_non_local_image_reference(self) -> None:
        launcher = ApptainerTaskEnvironmentLauncher()

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
                outputs={"table": str(work_path / "table.qza")},
            )

            with self.assertRaisesRegex(RuntimeError, "local \\.sif image paths"):
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="apptainer",
                        reference="docker://ghcr.io/cymis/qiime2-plugin-dada2:2026.1",
                    ),
                    request=request,
                )
