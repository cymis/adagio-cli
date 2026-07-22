import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from adagio.cli.config import EnvironmentOverride, load_run_config
from adagio.executors.base import TaskEnvironmentOverride
from adagio.executors.defaults import (
    ConfigurableTaskEnvironmentResolver,
    DefaultTaskEnvironmentResolver,
)
from adagio.model.task import PluginActionTask


def _task(*, name: str | None = None) -> PluginActionTask:
    return PluginActionTask.model_validate(
        {
            "id": "task-1",
            "kind": "plugin-action",
            "name": name,
            "plugin": "dada2",
            "action": "denoise_single",
            "inputs": {},
            "parameters": {},
            "outputs": {},
        }
    )


class RunConfigTests(unittest.TestCase):
    def test_load_run_config_accepts_apptainer_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "runtime.toml"
            config_path.write_text(
                "\n".join(
                    [
                        "version = 1",
                        "",
                        "[defaults]",
                        'kind = "apptainer"',
                        'image = "/images/default.sif"',
                        "",
                        "[plugins]",
                        'dada2 = { kind = "apptainer", image = "/images/dada2.sif" }',
                        "",
                        "[tasks]",
                        '"dada2.denoise_single" = { image = "/images/task.sif" }',
                    ]
                ),
                encoding="utf-8",
            )

            config = load_run_config(config_path)

        assert config is not None
        self.assertEqual(config.defaults.kind, "apptainer")
        self.assertEqual(config.defaults.image, "/images/default.sif")
        self.assertEqual(config.plugins["dada2"].kind, "apptainer")
        self.assertEqual(config.tasks["dada2.denoise_single"].image, "/images/task.sif")

    def test_load_run_config_accepts_conda_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            default_prefix = root / "envs" / "qiime2-2026.1"
            task_prefix = root / "envs" / "q2-dada2"
            default_prefix.mkdir(parents=True)
            task_prefix.mkdir(parents=True)
            config_path = Path(tmpdir) / "runtime.toml"
            config_path.write_text(
                "\n".join(
                    [
                        "version = 1",
                        "",
                        "[defaults]",
                        'kind = "conda"',
                        f'prefix = "{default_prefix}"',
                        'conda_executable = "/opt/conda/bin/conda"',
                        "",
                        "[tasks]",
                        f'"dada2.denoise_single" = {{ prefix = "{task_prefix}" }}',
                    ]
                ),
                encoding="utf-8",
            )

            config = load_run_config(config_path)

            assert config is not None
            default_override = config.defaults.to_task_environment_override()
            task_override = config.tasks[
                "dada2.denoise_single"
            ].to_task_environment_override()

            assert default_override is not None
            assert task_override is not None
            self.assertEqual(default_override.kind, "conda")
            self.assertEqual(default_override.reference, str(default_prefix))
            self.assertEqual(
                default_override.options,
                {"conda_executable": "/opt/conda/bin/conda"},
            )
            self.assertEqual(task_override.reference, str(task_prefix))
            self.assertIsNone(task_override.options)

    def test_legacy_environment_key_is_a_hard_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "runtime.toml"
            config_path.write_text(
                "\n".join(
                    [
                        "version = 1",
                        "",
                        "[defaults]",
                        'kind = "conda"',
                        'environment = "qiime2-2026.1"',
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                Exception,
                r"Conda environments are referenced by absolute path now\. "
                r'Replace environment = "<name>" with prefix = "/path/to/env" '
                r"\(conda env list shows each environment's path\)\.",
            ):
                load_run_config(config_path)

    def test_conda_prefix_spellings_normalize_identically(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            home = Path(tmpdir).resolve()
            real_prefix = home / "envs" / "qiime2"
            real_prefix.mkdir(parents=True)
            link = home / "env-link"
            link.symlink_to(real_prefix)

            spellings = ["~/envs/qiime2", str(real_prefix), str(link)]
            with unittest.mock.patch.dict(
                os.environ, {"HOME": str(home), "USERPROFILE": str(home)}
            ):
                references = [
                    EnvironmentOverride(
                        kind="conda", prefix=spelling
                    ).to_task_environment_override()
                    for spelling in spellings
                ]

            assert all(override is not None for override in references)
            resolved = {override.reference for override in references if override}
            self.assertEqual(resolved, {str(real_prefix.resolve())})


class ConfigurableResolverTests(unittest.TestCase):
    def test_plugin_override_inherits_default_apptainer_kind(self) -> None:
        resolver = ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            default_override=TaskEnvironmentOverride(
                kind="apptainer",
                reference="/images/default.sif",
            ),
            plugin_overrides={
                "dada2": TaskEnvironmentOverride(reference="/images/dada2.sif"),
            },
        )

        environment = resolver.resolve(task=_task())

        self.assertEqual(environment.kind, "apptainer")
        self.assertEqual(environment.reference, "/images/dada2.sif")

    def test_task_override_can_switch_back_to_docker(self) -> None:
        resolver = ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            default_override=TaskEnvironmentOverride(
                kind="apptainer",
                reference="/images/default.sif",
            ),
            task_overrides={
                "named-step": TaskEnvironmentOverride(
                    kind="docker",
                    reference="registry.internal/dada2:1.0",
                    platform="linux/amd64",
                )
            },
        )

        environment = resolver.resolve(task=_task(name="named-step"))

        self.assertEqual(environment.kind, "docker")
        self.assertEqual(environment.reference, "registry.internal/dada2:1.0")
        self.assertEqual(environment.options, {"platform": "linux/amd64"})

    def test_kind_override_without_reference_clears_inherited_reference(self) -> None:
        resolver = ConfigurableTaskEnvironmentResolver(
            base=DefaultTaskEnvironmentResolver(),
            default_override=TaskEnvironmentOverride(kind="conda"),
        )

        environment = resolver.resolve(task=_task())

        self.assertEqual(environment.kind, "conda")
        self.assertEqual(environment.reference, "")
