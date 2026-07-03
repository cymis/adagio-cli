import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adagio.executors.base import TaskEnvironmentSpec, TaskExecutionRequest
from adagio.executors.container_support import (
    container_python_root,
    containerize_path,
    mount_roots,
)
from adagio.executors.docker import DockerTaskEnvironmentLauncher
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


class DockerLauncherTests(unittest.TestCase):
    def test_launch_builds_docker_run_command(self) -> None:
        launcher = DockerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
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
                archive_collection_inputs={
                    "tables": [str(collection_input_path)]
                },
                metadata_inputs={},
                params={},
                metadata_column_kwargs={},
                outputs={"visualization": str(output_path)},
            )

            manifest_path = result_manifest_path(task_id=task.id, work_path=work_path)
            expected_spec = containerize_path(
                task_spec_path(task_id=task.id, work_path=work_path)
            )

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    manifest_path,
                    build_result_manifest(
                        outputs={"visualization": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch(
                "adagio.executors.docker.subprocess.run",
                side_effect=fake_run,
            ) as run_mock:
                result = launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="docker",
                        reference="ghcr.io/cymis/qiime2-plugin-demux:2026.1",
                    ),
                    request=request,
                )

            task_spec = read_json_file(task_spec_path(task_id=task.id, work_path=work_path))

            command = run_mock.call_args.args[0]
            python_root = container_python_root(work_path=work_path)
            bind_targets = {
                f"{root_path}:{containerize_path(root_path)}:rw"
                for root_path in mount_roots(
                    [cwd, work_path, input_path, collection_input_path, python_root]
                )
            }

            self.assertEqual(command[0], "docker")
            self.assertEqual(command[1], "run")
            self.assertEqual(command[2], "--rm")
            self.assertIn("-w", command)
            self.assertIn(containerize_path(cwd), command)
            self.assertIn(
                f"PYTHONPATH={containerize_path(python_root)}",
                command,
            )
            self.assertIn("PYTHONNOUSERSITE=1", command)
            self.assertIn("python", command)
            self.assertIn("-m", command)
            self.assertIn("adagio.cli.task_exec", command)
            self.assertIn("--task", command)
            self.assertIn(expected_spec, command)
            self.assertIn("ghcr.io/cymis/qiime2-plugin-demux:2026.1", command)
            self.assertTrue(bind_targets.issubset(set(command)))
            self.assertEqual(
                task_spec["archive_collection_inputs"],
                {"tables": [containerize_path(collection_input_path)]},
            )
            self.assertEqual(result.outputs, {"visualization": str(output_path)})
            self.assertFalse(result.reused)

    def test_launch_mounts_manifest_referenced_fastq_roots(self) -> None:
        launcher = DockerTaskEnvironmentLauncher()
        task = _task()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            cwd = root / "cwd"
            work_path = root / "work"
            cwd.mkdir()
            work_path.mkdir()
            output_path = work_path / "summary.qzv"

            # The manifest lives under the cwd, but the fastqs it points at live
            # under a *different* top-level root that nothing else mounts.
            fastq_root = _foreign_root(cwd)
            manifest_path = cwd / "manifest.tsv"
            manifest_path.write_text(
                "sample-id\tabsolute-filepath\n"
                f"sample1\t{fastq_root}/adagio-reads/s1.fastq.gz\n"
                f"sample2\t{fastq_root}/adagio-reads/s2.fastq.gz\n",
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
                outputs={"visualization": str(output_path)},
                archive_input_materializations={
                    "seqs": {
                        "mode": "raw",
                        "semantic_type": "SampleData[SequencesWithQuality]",
                        "input_format": "SingleEndFastqManifestPhred33V2",
                        "validate_level": "max",
                    }
                },
            )

            result_path = result_manifest_path(task_id=task.id, work_path=work_path)

            def fake_run(cmd, check, stdout, stderr, text):  # noqa: ANN001
                write_json_file(
                    result_path,
                    build_result_manifest(
                        outputs={"visualization": containerize_path(output_path)},
                        reused=False,
                    ),
                )
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with patch(
                "adagio.executors.docker.subprocess.run",
                side_effect=fake_run,
            ) as run_mock:
                launcher.launch(
                    environment=TaskEnvironmentSpec(
                        kind="docker", reference="img:test"
                    ),
                    request=request,
                )

            command = run_mock.call_args.args[0]
            fastq_bind = f"{fastq_root}:{containerize_path(fastq_root)}:rw"
            self.assertIn(fastq_bind, command)
            # The fastq root genuinely differs from the cwd's top-level root.
            self.assertNotEqual(fastq_root.parts[1], cwd.parts[1])


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
