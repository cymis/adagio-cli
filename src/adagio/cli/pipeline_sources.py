from __future__ import annotations

import re
from contextlib import ExitStack
from pathlib import Path
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


CATALOG_TIERS = ("official", "community")
DEFAULT_PIPELINE_SOURCE = "adagio"
SOURCE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]*")


class PipelineResolutionError(RuntimeError):
    """Raised when a pipeline reference cannot be resolved."""


@dataclass(frozen=True)
class LocalCatalogLocation:
    root: Path

    def candidate_paths(self, slug: str) -> tuple[Path, ...]:
        return tuple(
            (self.root / "pipelines" / tier / slug / "pipeline.adg").resolve()
            for tier in CATALOG_TIERS
        )


@dataclass(frozen=True)
class GitHubCatalogLocation:
    owner: str
    repo: str
    ref: str = "main"

    def candidate_urls(self, slug: str) -> tuple[str, ...]:
        quoted_slug = _quote_slug(slug)
        return tuple(
            f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/{self.ref}/"
            f"pipelines/{tier}/{quoted_slug}/pipeline.adg"
            for tier in CATALOG_TIERS
        )


@dataclass(frozen=True)
class PipelineSource:
    name: str
    locations: tuple[LocalCatalogLocation | GitHubCatalogLocation, ...]


@dataclass(frozen=True)
class PipelineResolution:
    path: Path
    origin: str
    is_remote: bool = False


def parse_pipeline_source_reference(reference: str) -> tuple[str, str] | None:
    raw = reference.strip()
    if not raw:
        return None
    if raw.startswith(("/", "./", "../", "~")):
        return None
    if "://" in raw:
        return None
    if Path(raw).suffix in {".adg", ".json"}:
        return None
    if not raw.startswith("@"):
        return None

    source_name, separator, slug = raw[1:].partition("/")
    if not separator or not source_name or not slug:
        return None
    if not SOURCE_NAME_RE.fullmatch(source_name):
        return None
    if source_name != DEFAULT_PIPELINE_SOURCE:
        return None
    if not SLUG_RE.fullmatch(slug):
        return None
    return source_name, slug


def discover_workspace_catalog_roots(
    *, search_roots: tuple[Path, ...] | None = None
) -> tuple[Path, ...]:
    seen: set[Path] = set()
    discovered: list[Path] = []
    anchors = list(search_roots or ())
    anchors.extend([Path.cwd(), Path(__file__).resolve()])

    for anchor in anchors:
        current = anchor.resolve()
        if current.is_file():
            current = current.parent

        for parent in (current, *current.parents):
            for candidate in _catalog_candidates(parent):
                resolved = candidate.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                discovered.append(resolved)

    return tuple(discovered)


def default_pipeline_sources(
    *, search_roots: tuple[Path, ...] | None = None
) -> tuple[PipelineSource, ...]:
    local_locations = tuple(
        LocalCatalogLocation(root=root)
        for root in discover_workspace_catalog_roots(search_roots=search_roots)
    )
    github_fallback = GitHubCatalogLocation(owner="cymis", repo="adagio-pipelines")
    built_in_locations = (*local_locations, github_fallback)
    return (
        PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=built_in_locations,
        ),
    )


def resolve_pipeline_reference(
    reference: str | Path,
    *,
    exit_stack: ExitStack,
    sources: tuple[PipelineSource, ...] | None = None,
    download_cache_dir: Path | None = None,
) -> Path:
    return resolve_pipeline_reference_details(
        reference,
        exit_stack=exit_stack,
        sources=sources,
        download_cache_dir=download_cache_dir,
    ).path


def resolve_pipeline_reference_details(
    reference: str | Path,
    *,
    exit_stack: ExitStack,
    sources: tuple[PipelineSource, ...] | None = None,
    download_cache_dir: Path | None = None,
) -> PipelineResolution:
    raw = str(reference).strip()
    if not raw:
        raise PipelineResolutionError("Pipeline reference is empty.")

    candidate_path = Path(raw).expanduser()
    if candidate_path.exists():
        resolved_path = candidate_path.resolve()
        return PipelineResolution(path=resolved_path, origin=str(resolved_path))

    parsed_reference = parse_pipeline_source_reference(raw)
    if parsed_reference is None:
        if raw.startswith("@"):
            raise PipelineResolutionError(
                f"Invalid pipeline reference '{raw}'. Expected @adagio/slug, "
                "where slug uses lowercase letters, digits, and hyphens."
            )
        raise PipelineResolutionError(f"Pipeline file does not exist: {raw}")

    source_name, slug = parsed_reference
    registered_sources = default_pipeline_sources() if sources is None else sources
    source_registry = {source.name: source for source in registered_sources}
    source = source_registry.get(source_name)
    if source is None:
        available = ", ".join(sorted(source_registry)) or "none"
        raise PipelineResolutionError(
            f"Unknown pipeline source '{source_name}'. Available sources: {available}."
        )

    attempted_candidates: list[str] = []
    access_errors: list[str] = []

    for location in source.locations:
        if isinstance(location, LocalCatalogLocation):
            for path in location.candidate_paths(slug):
                attempted_candidates.append(str(path))
                if path.exists():
                    return PipelineResolution(path=path, origin=str(path))
            continue

        cached_path = _cached_remote_pipeline_path(
            cache_dir=download_cache_dir,
            source_name=source_name,
            slug=slug,
        )
        if cached_path is not None:
            attempted_candidates.append(str(cached_path))
            if cached_path.exists():
                return PipelineResolution(path=cached_path, origin=str(cached_path))

        for url in location.candidate_urls(slug):
            attempted_candidates.append(url)
            try:
                return PipelineResolution(
                    path=_download_remote_pipeline(
                        url=url,
                        exit_stack=exit_stack,
                        cache_path=cached_path,
                    ),
                    origin=url,
                    is_remote=True,
                )
            except FileNotFoundError:
                continue
            except PipelineResolutionError as error:
                access_errors.append(str(error))
                break

    message = [f"Pipeline reference '{raw}' was not found."]
    if attempted_candidates:
        message.append("Looked in:")
        message.extend(f"  - {candidate}" for candidate in attempted_candidates)
    if access_errors:
        message.append("Errors:")
        message.extend(f"  - {error}" for error in access_errors)
    raise PipelineResolutionError("\n".join(message))


def _catalog_candidates(parent: Path) -> tuple[Path, ...]:
    candidates: list[Path] = []
    if parent.name == "adagio-pipelines" and (parent / "pipelines").is_dir():
        candidates.append(parent)

    sibling = parent / "adagio-pipelines"
    if sibling.is_dir() and (sibling / "pipelines").is_dir():
        candidates.append(sibling)

    return tuple(candidates)


def _download_remote_pipeline(
    *,
    url: str,
    exit_stack: ExitStack,
    cache_path: Path | None = None,
) -> Path:
    request = Request(url, headers={"User-Agent": "adagio-cli"})
    try:
        with urlopen(request, timeout=10) as response:
            payload = response.read()
    except HTTPError as error:
        if error.code == 404:
            raise FileNotFoundError(url) from error
        raise PipelineResolutionError(
            f"Failed to fetch pipeline from {url}: HTTP {error.code}"
        ) from error
    except URLError as error:
        raise PipelineResolutionError(
            f"Failed to fetch pipeline from {url}: {error.reason}"
        ) from error

    if cache_path is None:
        tempdir = Path(
            exit_stack.enter_context(TemporaryDirectory(prefix="adagio-pipeline-"))
        )
        pipeline_path = tempdir / "pipeline.adg"
    else:
        pipeline_path = cache_path
        pipeline_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline_path.write_bytes(payload)
    return pipeline_path


def _quote_slug(slug: str) -> str:
    return "/".join(quote(part) for part in Path(slug).parts)


def _cached_remote_pipeline_path(
    *,
    source_name: str,
    slug: str,
    cache_dir: Path | None,
) -> Path | None:
    if cache_dir is None:
        return None
    return cache_dir / "adagio-pipelines" / source_name / slug / "pipeline.adg"
