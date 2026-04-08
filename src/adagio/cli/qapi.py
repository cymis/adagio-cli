import json
import os
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from ..qapi import DEFAULT_SCHEMA_VERSION, generate_qapi_payload, submit_qapi_payload

console = Console()


def run_qapi(argv: list[str]) -> None:
    app = App(
        name="adagio qapi",
        help="Generate and submit QAPI payloads from the active QIIME environment.",
    )
    app.command(build_qapi, name="build")
    app(argv)


def _print_submission_summary(response_body: object) -> None:
    if isinstance(response_body, dict):
        message = response_body.get("message")
        if isinstance(message, str) and message.strip():
            console.print(message)

        operations = response_body.get("operations")
        if isinstance(operations, list):
            created = [
                operation["plugin_name"]
                for operation in operations
                if isinstance(operation, dict) and operation.get("action") == "create"
            ]
            overwritten = [
                operation["plugin_name"]
                for operation in operations
                if isinstance(operation, dict) and operation.get("action") == "overwrite"
            ]
            if created:
                console.print(f"[green]Create:[/green] {', '.join(created)}")
            if overwritten:
                console.print(f"[yellow]Overwrite:[/yellow] {', '.join(overwritten)}")
        return

    if isinstance(response_body, str):
        if response_body.strip():
            console.print(response_body)
        return

    if response_body is not None:
        console.print(json.dumps(response_body, indent=2))


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
    plugin: Annotated[
        tuple[str, ...],
        Parameter(
            name=("--plugin",),
            help=(
                "Plugin name to include. Repeat the option for multiple plugins. "
                "Comma-separated values are also accepted."
            ),
        ),
    ] = (),
    all_plugins: Annotated[
        bool,
        Parameter(
            name=("--all",),
            help=(
                "Submit all installed plugins. This is also the default when "
                "no --plugin values are provided."
            ),
        ),
    ] = False,
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
            help=(
                "Preview the backend changes without writing them. If no Action URL is "
                "configured, this falls back to generating the payload locally only."
            ),
        ),
    ] = False,
    force_overwrite: Annotated[
        bool,
        Parameter(
            name=("--force-overwrite",),
            help="Overwrite existing plugins for the same QIIME version.",
        ),
    ] = False,
) -> None:
    """Generate QAPI from the active QIIME environment and submit it to Action Potential."""
    if all_plugins and plugin:
        raise SystemExit("Use either --all or --plugin, not both.")

    requested_plugins = None if all_plugins or not plugin else plugin
    try:
        request_body = generate_qapi_payload(
            schema_version=schema_version,
            plugins=requested_plugins,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if output is not None:
        output.write_text(json.dumps(request_body, indent=2), encoding="utf-8")
        console.print(f"[green]Wrote QAPI payload:[/green] {output}")

    resolved_action_url = action_url or os.getenv("ACTION_URL")
    if dry_run and not resolved_action_url:
        console.print(
            "[yellow]Dry run enabled without an Action URL; generated the payload locally only.[/yellow]"
        )
        return

    url, status, response_body = submit_qapi_payload(
        request_body,
        action_url=action_url,
        timeout=timeout,
        dry_run=dry_run,
        force_overwrite=force_overwrite,
    )

    verb = "Previewed QAPI submit against" if dry_run else "Submitted QAPI to"
    console.print(f"[green]{verb}[/green] {url} [green](HTTP {status})[/green]")
    _print_submission_summary(response_body)
