import tempfile
import unittest
from pathlib import Path

from adagio.executors.cache_support import DEFAULT_RECYCLE_POOL, resolve_cache_config


class RecyclePoolThreadingTests(unittest.TestCase):
    def test_default_pool_when_no_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_cache_config(
                cwd=Path(tmp), cache_dir="cache", reuse=True
            )
        self.assertEqual(config.recycle_pool, DEFAULT_RECYCLE_POOL)

    def test_override_replaces_default_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_cache_config(
                cwd=Path(tmp),
                cache_dir="cache",
                reuse=True,
                recycle_pool="pipeline:abc-123",
            )
        self.assertEqual(config.recycle_pool, "pipeline:abc-123")

    def test_no_reuse_forces_none_even_with_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_cache_config(
                cwd=Path(tmp),
                cache_dir="cache",
                reuse=False,
                recycle_pool="job:xyz",
            )
        self.assertIsNone(config.recycle_pool)

    def test_empty_override_falls_back_to_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_cache_config(
                cwd=Path(tmp), cache_dir="cache", reuse=True, recycle_pool=""
            )
        self.assertEqual(config.recycle_pool, DEFAULT_RECYCLE_POOL)

    def test_selective_no_reuse_only_removes_matching_node_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = resolve_cache_config(
                cwd=Path(tmp),
                cache_dir="cache",
                reuse=True,
                recycle_pool="pipeline:abc",
                no_reuse_nodes={"local-1", "local-2"},
            )
        self.assertEqual(config.recycle_pool_for("official-upstream"), "pipeline:abc")
        self.assertIsNone(config.recycle_pool_for("local-1"))
        self.assertEqual(config.recycle_pool_for("official-downstream"), "pipeline:abc")


if __name__ == "__main__":
    unittest.main()
