import json
import tempfile
import unittest
from pathlib import Path

from adagio.cli.config import load_run_config


def _write(root: Path, name: str, text: str) -> Path:
    path = root / name
    path.write_text(text, encoding="utf-8")
    return path


class RunConfigAutodetectTests(unittest.TestCase):
    def test_toml_config_still_parses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(
                Path(tmp),
                "config.toml",
                'version = 1\n[defaults]\nkind = "conda"\nenvironment = "q2-2026"\n'
                '[plugins]\ndada2 = { kind = "conda", prefix = "/opt/envs/dada2" }\n',
            )
            config = load_run_config(path)
        assert config is not None
        self.assertEqual(config.version, 1)
        self.assertEqual(config.defaults.kind, "conda")
        self.assertEqual(config.defaults.environment, "q2-2026")
        self.assertIn("dada2", config.plugins)
        self.assertEqual(config.plugins["dada2"].prefix, "/opt/envs/dada2")

    def test_json_config_is_autodetected(self) -> None:
        payload = {
            "version": 1,
            "defaults": {"kind": "docker", "image": "ghcr.io/x/y:1"},
            "plugins": {
                "dada2": {"kind": "conda", "prefix": "/opt/envs/dada2"},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(Path(tmp), "config.json", json.dumps(payload))
            config = load_run_config(path)
        assert config is not None
        self.assertEqual(config.defaults.kind, "docker")
        self.assertEqual(config.defaults.image, "ghcr.io/x/y:1")
        self.assertEqual(config.plugins["dada2"].prefix, "/opt/envs/dada2")

    def test_json_with_leading_whitespace_is_autodetected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(
                Path(tmp),
                "config.json",
                '\n   {"version": 1, "defaults": {"kind": "docker"}}\n',
            )
            config = load_run_config(path)
        assert config is not None
        self.assertEqual(config.defaults.kind, "docker")

    def test_none_path_returns_none(self) -> None:
        self.assertIsNone(load_run_config(None))


if __name__ == "__main__":
    unittest.main()
