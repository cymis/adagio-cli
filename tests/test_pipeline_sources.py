import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from rich.console import Console

from adagio.cli.pipeline import show_pipeline
from adagio.cli.pipeline_sources import (
    DEFAULT_PIPELINE_SOURCE,
    GitHubCatalogLocation,
    LocalCatalogLocation,
    PipelineResolutionError,
    PipelineSource,
    discover_workspace_catalog_roots,
    parse_pipeline_source_reference,
    resolve_pipeline_reference,
    resolve_pipeline_reference_details,
)


def _sample_pipeline_payload() -> dict:
    return {
        "spec": {
            "type": "pipeline",
            "signature": {
                "inputs": [],
                "parameters": [],
                "outputs": [],
            },
            "graph": [
                {
                    "id": "task-dada2",
                    "kind": "plugin-action",
                    "plugin": "dada2",
                    "action": "denoise_single",
                    "inputs": {},
                    "parameters": {},
                    "outputs": {},
                }
            ],
        }
    }


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._payload


class PipelineSourceTests(unittest.TestCase):
    def test_parse_pipeline_source_reference_recognizes_source_slug_syntax(
        self,
    ) -> None:
        self.assertEqual(
            parse_pipeline_source_reference("@adagio/microbial-diversity"),
            ("adagio", "microbial-diversity"),
        )
        self.assertIsNone(
            parse_pipeline_source_reference("@my-personal-channel/denoise")
        )
        self.assertIsNone(parse_pipeline_source_reference("adagio/denoise"))
        self.assertIsNone(parse_pipeline_source_reference("./pipeline.adg"))
        self.assertIsNone(parse_pipeline_source_reference("pipeline.adg"))
        self.assertIsNone(parse_pipeline_source_reference("@adagio/../denoise"))
        self.assertIsNone(parse_pipeline_source_reference("@adagio/denoise/extra"))
        self.assertIsNone(parse_pipeline_source_reference("@adagio/Denoise"))

    def test_discover_workspace_catalog_roots_finds_sibling_repo_from_worktree(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            catalog_root = workspace / "adagio-pipelines"
            (catalog_root / "pipelines" / "community").mkdir(parents=True)
            worktree_root = workspace / ".worktrees" / "adagio-cli-community-pipelines"
            worktree_root.mkdir(parents=True)

            discovered = discover_workspace_catalog_roots(search_roots=(worktree_root,))

        self.assertIn(catalog_root.resolve(), discovered)

    def test_existing_local_path_takes_precedence_over_source_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_path = Path(tmpdir) / DEFAULT_PIPELINE_SOURCE / "denoise"
            pipeline_path.parent.mkdir(parents=True)
            pipeline_path.write_text("{}", encoding="utf-8")

            with ExitStack() as exit_stack:
                resolved = resolve_pipeline_reference(
                    pipeline_path, exit_stack=exit_stack
                )

        self.assertEqual(resolved, pipeline_path.resolve())

    def test_source_reference_resolves_from_local_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_root = Path(tmpdir) / "adagio-pipelines"
            pipeline_path = (
                catalog_root / "pipelines" / "community" / "denoise" / "pipeline.adg"
            )
            pipeline_path.parent.mkdir(parents=True)
            pipeline_path.write_text("{}", encoding="utf-8")
            source = PipelineSource(
                name=DEFAULT_PIPELINE_SOURCE,
                locations=(LocalCatalogLocation(root=catalog_root),),
            )

            with ExitStack() as exit_stack:
                resolved = resolve_pipeline_reference(
                    f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                    exit_stack=exit_stack,
                    sources=(source,),
                )

        self.assertEqual(resolved, pipeline_path.resolve())

    def test_local_catalog_prefers_official_over_community(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_root = Path(tmpdir) / "adagio-pipelines"
            official_path = (
                catalog_root / "pipelines" / "official" / "denoise" / "pipeline.adg"
            )
            community_path = (
                catalog_root / "pipelines" / "community" / "denoise" / "pipeline.adg"
            )
            official_path.parent.mkdir(parents=True)
            community_path.parent.mkdir(parents=True)
            official_path.write_text('{"source": "official"}', encoding="utf-8")
            community_path.write_text('{"source": "community"}', encoding="utf-8")
            source = PipelineSource(
                name=DEFAULT_PIPELINE_SOURCE,
                locations=(LocalCatalogLocation(root=catalog_root),),
            )

            with ExitStack() as exit_stack:
                resolved = resolve_pipeline_reference(
                    f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                    exit_stack=exit_stack,
                    sources=(source,),
                )

        self.assertEqual(resolved, official_path.resolve())

    def test_local_catalog_hit_skips_network_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_root = Path(tmpdir) / "adagio-pipelines"
            pipeline_path = (
                catalog_root / "pipelines" / "official" / "denoise" / "pipeline.adg"
            )
            pipeline_path.parent.mkdir(parents=True)
            pipeline_path.write_text("{}", encoding="utf-8")
            source = PipelineSource(
                name=DEFAULT_PIPELINE_SOURCE,
                locations=(
                    LocalCatalogLocation(root=catalog_root),
                    GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),
                ),
            )

            with patch("adagio.cli.pipeline_sources.urlopen") as mock_urlopen:
                with ExitStack() as exit_stack:
                    resolved = resolve_pipeline_reference(
                        f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                        exit_stack=exit_stack,
                        sources=(source,),
                    )

        self.assertEqual(resolved, pipeline_path.resolve())
        mock_urlopen.assert_not_called()

    def test_source_reference_falls_back_to_github_when_needed(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )

        with patch(
            "adagio.cli.pipeline_sources.urlopen",
            return_value=_FakeResponse(
                b'{"spec": {"type": "pipeline", "signature": {"inputs": [], "parameters": [], "outputs": []}, "graph": []}}'
            ),
        ) as mock_urlopen:
            with ExitStack() as exit_stack:
                resolved = resolve_pipeline_reference(
                    f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                    exit_stack=exit_stack,
                    sources=(source,),
                )
                payload = json.loads(resolved.read_text(encoding="utf-8"))

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/pipelines/official/denoise/pipeline.adg", request.full_url)
        self.assertEqual(payload["spec"]["type"], "pipeline")

    def test_source_reference_reports_remote_origin(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )

        with patch(
            "adagio.cli.pipeline_sources.urlopen",
            return_value=_FakeResponse(b"{}"),
        ):
            with ExitStack() as exit_stack:
                resolved = resolve_pipeline_reference_details(
                    f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                    exit_stack=exit_stack,
                    sources=(source,),
                )

        self.assertTrue(resolved.is_remote)
        self.assertIn("/pipelines/official/denoise/pipeline.adg", resolved.origin)

    def test_remote_http_error_is_reported(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )

        with patch(
            "adagio.cli.pipeline_sources.urlopen",
            side_effect=HTTPError(
                url="https://example.invalid/pipeline.adg",
                code=500,
                msg="server error",
                hdrs=None,
                fp=None,
            ),
        ):
            with ExitStack() as exit_stack:
                with self.assertRaises(PipelineResolutionError) as error:
                    resolve_pipeline_reference(
                        f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                        exit_stack=exit_stack,
                        sources=(source,),
                    )

        self.assertIn("HTTP 500", str(error.exception))

    def test_remote_url_error_is_reported(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )

        with patch(
            "adagio.cli.pipeline_sources.urlopen",
            side_effect=URLError("network unavailable"),
        ):
            with ExitStack() as exit_stack:
                with self.assertRaises(PipelineResolutionError) as error:
                    resolve_pipeline_reference(
                        f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                        exit_stack=exit_stack,
                        sources=(source,),
                    )

        self.assertIn("network unavailable", str(error.exception))

    def test_remote_pipeline_downloads_to_cache_dir_when_provided(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            expected_path = (
                cache_dir
                / "adagio-pipelines"
                / DEFAULT_PIPELINE_SOURCE
                / "denoise"
                / "pipeline.adg"
            )

            with patch(
                "adagio.cli.pipeline_sources.urlopen",
                return_value=_FakeResponse(
                    b'{"spec": {"type": "pipeline", "signature": {"inputs": [], "parameters": [], "outputs": []}, "graph": []}}'
                ),
            ) as mock_urlopen:
                with ExitStack() as exit_stack:
                    resolved = resolve_pipeline_reference_details(
                        f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                        exit_stack=exit_stack,
                        sources=(source,),
                        download_cache_dir=cache_dir,
                    )
                    payload = json.loads(resolved.path.read_text(encoding="utf-8"))

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/pipelines/official/denoise/pipeline.adg", request.full_url)
        self.assertEqual(resolved.path, expected_path)
        self.assertEqual(payload["spec"]["type"], "pipeline")
        self.assertTrue(resolved.is_remote)

    def test_cached_remote_pipeline_short_circuits_network(self) -> None:
        source = PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines"),),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cached_path = (
                cache_dir
                / "adagio-pipelines"
                / DEFAULT_PIPELINE_SOURCE
                / "denoise"
                / "pipeline.adg"
            )
            cached_path.parent.mkdir(parents=True)
            cached_path.write_text('{"cached": true}', encoding="utf-8")

            with patch("adagio.cli.pipeline_sources.urlopen") as mock_urlopen:
                with ExitStack() as exit_stack:
                    resolved = resolve_pipeline_reference_details(
                        f"@{DEFAULT_PIPELINE_SOURCE}/denoise",
                        exit_stack=exit_stack,
                        sources=(source,),
                        download_cache_dir=cache_dir,
                    )

        self.assertEqual(resolved.path, cached_path)
        self.assertEqual(resolved.origin, str(cached_path))
        self.assertFalse(resolved.is_remote)
        mock_urlopen.assert_not_called()

    def test_non_adagio_source_is_rejected(self) -> None:
        with ExitStack() as exit_stack:
            with self.assertRaises(PipelineResolutionError) as error:
                resolve_pipeline_reference(
                    "@my-personal-channel/denoise",
                    exit_stack=exit_stack,
                    sources=(),
                )

        self.assertIn("Expected @adagio/slug", str(error.exception))

    def test_invalid_at_reference_reports_reference_shape(self) -> None:
        with ExitStack() as exit_stack:
            with self.assertRaises(PipelineResolutionError) as error:
                resolve_pipeline_reference(
                    "@adagio/../secret",
                    exit_stack=exit_stack,
                    sources=(),
                )

        self.assertIn("Expected @adagio/slug", str(error.exception))

    def test_missing_source_reference_reports_attempted_locations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = PipelineSource(
                name=DEFAULT_PIPELINE_SOURCE,
                locations=(
                    LocalCatalogLocation(root=Path(tmpdir) / "adagio-pipelines"),
                ),
            )

            with ExitStack() as exit_stack:
                with self.assertRaises(PipelineResolutionError) as error:
                    resolve_pipeline_reference(
                        f"@{DEFAULT_PIPELINE_SOURCE}/missing",
                        exit_stack=exit_stack,
                        sources=(source,),
                    )

        message = str(error.exception)
        self.assertIn("Pipeline reference '@adagio/missing' was not found.", message)
        self.assertIn("pipelines/community/missing/pipeline.adg", message)


class PipelineSourceIntegrationTests(unittest.TestCase):
    def test_pipeline_show_accepts_source_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_root = Path(tmpdir) / "adagio-pipelines"
            pipeline_path = (
                catalog_root / "pipelines" / "community" / "denoise" / "pipeline.adg"
            )
            pipeline_path.parent.mkdir(parents=True)
            pipeline_path.write_text(
                json.dumps(_sample_pipeline_payload()),
                encoding="utf-8",
            )
            source = PipelineSource(
                name=DEFAULT_PIPELINE_SOURCE,
                locations=(LocalCatalogLocation(root=catalog_root),),
            )
            output = io.StringIO()
            console = Console(file=output, width=120, record=True)

            with patch(
                "adagio.cli.pipeline_sources.default_pipeline_sources",
                return_value=(source,),
            ):
                with patch("adagio.cli.pipeline.console", console):
                    show_pipeline(Path(f"@{DEFAULT_PIPELINE_SOURCE}/denoise"))

        self.assertIn("dada2.denoise_single", output.getvalue())


if __name__ == "__main__":
    unittest.main()
