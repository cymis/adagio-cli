from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


CATALOG_TIERS = ("community", "official")
DEFAULT_PIPELINE_SOURCE = "adagio-playbook"


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

    source_name, separator, slug = raw.partition("/")
    if not separator or not source_name or not slug:
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
    return (
        PipelineSource(
            name=DEFAULT_PIPELINE_SOURCE,
            locations=(*local_locations, github_fallback),
        ),
    )


def resolve_pipeline_reference(
    reference: str | Path,
    *,
    exit_stack: ExitStack,
    sources: tuple[PipelineSource, ...] | None = None,
) -> Path:
    raw = str(reference).strip()
    if not raw:
        raise PipelineResolutionError("Pipeline reference is empty.")

    candidate_path = Path(raw).expanduser()
    if candidate_path.exists():
        return candidate_path.resolve()

    parsed_reference = parse_pipeline_source_reference(raw)
    if parsed_reference is None:
        raise PipelineResolutionError(f"Pipeline file does not exist: {raw}")

    source_name, slug = parsed_reference
    source_registry = {
        source.name: source for source in (sources or default_pipeline_sources())
    }
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
                    return path
            continue

        for url in location.candidate_urls(slug):
            attempted_candidates.append(url)
            try:
                return _download_remote_pipeline(url=url, exit_stack=exit_stack)
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


def _download_remote_pipeline(*, url: str, exit_stack: ExitStack) -> Path:
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

    tempdir = Path(
        exit_stack.enter_context(TemporaryDirectory(prefix="adagio-pipeline-"))
    )
    pipeline_path = tempdir / "pipeline.adg"
    pipeline_path.write_bytes(payload)
    return pipeline_path


def _quote_slug(slug: str) -> str:
    return "/".join(quote(part) for part in Path(slug).parts)
