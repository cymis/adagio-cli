import tempfile
import unittest
from pathlib import Path

from adagio.cli.config import load_run_config
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
