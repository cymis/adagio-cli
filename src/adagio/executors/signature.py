"""Per-node input signature (design §1.5).

A node's input signature is the ``sha256`` of a canonical JSON document::

    {
      "params": <sorted resolved parameter values>,
      "inputs": <sorted input content digests>,
      "env":    <image_ref@digest | conda ref>
    }

File inputs are content-addressed (``sha256`` of the file bytes) so the
signature is stable across re-imports of an identical file. Upstream artifacts
carry identity (their artifact id / resolved path is used verbatim). The result
enables future dirty/up-to-date + selective re-run; it is **unused for gating in
v1** and computed purely as telemetry.

This module is a pure function surface (no I/O beyond hashing files that are
handed to it) so it can be unit-tested without Docker/conda/QIIME.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .container_support import is_uri

# Read files in bounded chunks so a large fastq/manifest never has to be held in
# memory in full just to fingerprint it.
_FILE_CHUNK_BYTES = 1024 * 1024


def _canonical(value: Any) -> Any:
    """Recursively canonicalize a value for stable JSON serialization.

    Dict keys are sorted; lists preserve order (order is meaningful for
    collections); everything else is passed through. ``json.dumps`` with
    ``sort_keys=True`` handles nested dicts, but we normalize here so the shape
    is explicit and testable.
    """
    if isinstance(value, Mapping):
        return {str(k): _canonical(value[k]) for k in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def hash_file_bytes(path: str | Path) -> str:
    """Return ``sha256:<hex>`` of a file's bytes, or ``missing`` if unreadable.

    Best-effort: an unreadable/absent path yields a stable ``missing:<path>``
    marker rather than raising, so signature computation never crashes a run.
    """
    file_path = Path(path)
    digest = hashlib.sha256()
    try:
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_FILE_CHUNK_BYTES), b""):
                digest.update(chunk)
    except OSError:
        return f"missing:{file_path}"
    return f"sha256:{digest.hexdigest()}"


def _digest_input_value(value: Any) -> Any:
    """Content-digest a single resolved input value.

    * ``str`` that is a URI  -> the URI verbatim (identity; not fetched).
    * ``str`` local path     -> ``sha256`` of the file bytes (content address).
    * ``list``               -> element-wise (order preserved for collections).
    * ``dict``               -> key-sorted, values digested (named collection).
    * anything else          -> passed through canonicalized.
    """
    if isinstance(value, str):
        if is_uri(value):
            return value
        return hash_file_bytes(value)
    if isinstance(value, (list, tuple)):
        return [_digest_input_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(k): _digest_input_value(value[k]) for k in sorted(value, key=str)}
    return value


def compute_input_signature(
    *,
    params: Mapping[str, Any],
    inputs: Mapping[str, Any],
    env: str | None,
    upstream_ids: Iterable[str] | None = None,
) -> str:
    """Compute the ``sha256`` per-node input signature.

    Args:
        params: Resolved parameter values (name -> value).
        inputs: Resolved *file* inputs (name -> path | list | dict). Each string
            path is replaced by the ``sha256`` of its bytes; URIs are kept as
            identity. Upstream-artifact inputs should be passed via
            ``upstream_ids`` instead (identity, not content).
        env: The resolved environment reference, e.g. ``image_ref@digest`` for a
            docker task or the conda env name/prefix. ``None`` is allowed.
        upstream_ids: Identity references for inputs produced by upstream tasks
            (artifact ids / element ids). Merged into the ``inputs`` block under
            their names, sorted, without content hashing.

    Returns:
        ``sha256:<hex>`` string.
    """
    digested_inputs: dict[str, Any] = {
        str(name): _digest_input_value(value) for name, value in inputs.items()
    }
    for identity in upstream_ids or ():
        # Identity-addressed upstream artifacts: store the id verbatim, keyed by
        # itself so ordering is stable and it never collides with a file input.
        digested_inputs.setdefault(f"@{identity}", identity)

    document = {
        "params": _canonical(dict(params)),
        "inputs": _canonical(digested_inputs),
        "env": env,
    }
    canonical_json = json.dumps(
        document, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    return "sha256:" + hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def environment_reference(
    *, kind: str, reference: str, digest: str | None = None
) -> str:
    """Render the ``env`` component of the signature.

    Docker: ``docker:<ref>@<digest>`` when a digest is known, else ``docker:<ref>``.
    Other kinds (conda/apptainer): ``<kind>:<reference>``.
    """
    if kind == "docker" and digest:
        return f"docker:{reference}@{digest}"
    return f"{kind}:{reference}"
