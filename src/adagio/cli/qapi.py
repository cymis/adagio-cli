import json
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console

from ..qapi import DEFAULT_SCHEMA_VERSION, generate_qapi_payload, submit_qapi_payload

console = Console()


def build_qapi(
    *,
    action_url: Annotated[
        str | None,
        Parameter(
            name=("--action-url",),
            help=(
                "Action Potential API base URL (e.g. http://localhost:81/api/v1). "
                "Defaults to ACTION_URL env var."
            ),
        ),
    ] = None,
    schema_version: Annotated[
        str,
        Parameter(
            name=("--schema-version",),
            help="Schema version string stored alongside generated plugin data.",
        ),
    ] = DEFAULT_SCHEMA_VERSION,
    output: Annotated[
        Path | None,
        Parameter(
            name=("--output",),
            help="Optional path to write the generated request JSON.",
        ),
    ] = None,
    timeout: Annotated[
        int,
        Parameter(
            name=("--timeout",),
            help="HTTP timeout (seconds) for submitting to Action Potential.",
        ),
    ] = 60,
    dry_run: Annotated[
        bool,
        Parameter(
            name=("--dry-run",),
            help="Generate the payload but do not submit it to Action Potential.",
        ),
    ] = False,
) -> None:
    """Generate QAPI from the active QIIME environment and submit it to Action Potential."""
    request_body = generate_qapi_payload(schema_version=schema_version)

    if output is not None:
        output.write_text(json.dumps(request_body, indent=2), encoding="utf-8")
        console.print(f"[green]Wrote QAPI payload:[/green] {output}")

    if dry_run:
        console.print("[yellow]Dry run enabled; skipping submit.[/yellow]")
        return

    url, status, response_body = submit_qapi_payload(
        request_body, action_url=action_url, timeout=timeout
    )

    console.print(f"[green]Submitted QAPI to[/green] {url} [green](HTTP {status})[/green]")
    if response_body.strip():
        console.print(response_body)
