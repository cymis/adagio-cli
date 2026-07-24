import json
import os
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ..qapi import (
    DEFAULT_SCHEMA_VERSION,
    generate_qapi_payload,
    generate_qapi_plugin_index,
    submit_qapi_payload,
)

console = Console()


def run_qapi(argv: list[str]) -> None:
    app = App(
        name="adagio qapi",
        help="Generate and submit QAPI payloads from the active QIIME environment.",
    )
    app.command(build_qapi, name="build")
    app.command(list_qapi_plugins, name="list")
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
                if isinstance(operation, dict)
                and operation.get("action") == "overwrite"
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


def _print_skipped_private_actions(skipped_actions: list[str]) -> None:
    if not skipped_actions:
        return

    sorted_actions = sorted(skipped_actions)
    display_limit = 20
    displayed_actions = ", ".join(
        escape(action_name) for action_name in sorted_actions[:display_limit]
    )
    remaining_count = len(sorted_actions) - display_limit
    if remaining_count > 0:
        displayed_actions += f", and {remaining_count} more"

    noun = "action" if len(sorted_actions) == 1 else "actions"
    console.print(
        f"[yellow]Skipped {len(sorted_actions)} private QIIME {noun}:[/yellow] "
        f"{displayed_actions}"
    )


def _write_json_output(payload: object, output: Path | None) -> None:
    if output is None:
        console.print_json(json.dumps(payload))
        return

    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    console.print(f"[green]Wrote JSON:[/green] {output}")


def _add_default_environment(
    request_body: dict[str, object],
    *,
    default_docker_image: str | None,
    default_conda_prefix: Path | None,
) -> None:
    """Attach one explicit execution default to every submitted plugin."""
    if default_docker_image is not None and default_conda_prefix is not None:
        raise SystemExit(
            "Use either --default-docker-image or --default-conda-prefix, not both."
        )

    default_environment: dict[str, str] | None = None
    if default_docker_image is not None:
        image = default_docker_image.strip()
        if not image:
            raise SystemExit("--default-docker-image cannot be blank.")
        default_environment = {"kind": "docker", "image": image}
    elif default_conda_prefix is not None:
        if not default_conda_prefix.is_absolute():
            raise SystemExit("--default-conda-prefix must be an absolute path.")
        default_environment = {
            "kind": "conda",
            "prefix": str(default_conda_prefix),
        }

    if default_environment is None:
        return

    plugin_data = request_body.get("data")
    if not isinstance(plugin_data, dict):
        raise SystemExit("Generated QAPI payload does not contain plugin data.")
    for plugin in plugin_data.values():
        if isinstance(plugin, dict):
            plugin["default_environment"] = dict(default_environment)


def list_qapi_plugins(
    *,
    output: Annotated[
        Path | None,
        Parameter(
            name=("--output",),
            help="Optional path to write the registered plugin listing as JSON.",
        ),
    ] = None,
) -> None:
    """List plugins registered in the active QIIME environment."""
    _write_json_output(generate_qapi_plugin_index(), output)


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
    no_submit: Annotated[
        bool,
        Parameter(
            name=("--no-submit",),
            negative=(),
            help="Generate QAPI locally without contacting Action Potential.",
        ),
    ] = False,
    submission_token: Annotated[
        str | None,
        Parameter(
            name=("--submission-token",),
            help=(
                "Bearer token for protected QAPI submission routes. Defaults to "
                "QAPI_SUBMISSION_TOKEN env var; prefer the env var to avoid shell history leaks."
            ),
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
    default_docker_image: Annotated[
        str | None,
        Parameter(
            name=("--default-docker-image",),
            help=(
                "Persist this Docker image as the default execution environment "
                "for every submitted plugin."
            ),
        ),
    ] = None,
    default_conda_prefix: Annotated[
        Path | None,
        Parameter(
            name=("--default-conda-prefix",),
            help=(
                "Persist this absolute Conda environment path as the default "
                "execution environment for every submitted plugin."
            ),
        ),
    ] = None,
) -> None:
    """Generate QAPI from the active QIIME environment and submit it to Action Potential."""
    if all_plugins and plugin:
        raise SystemExit("Use either --all or --plugin, not both.")

    requested_plugins = None if all_plugins or not plugin else plugin
    skipped_private_actions: list[str] = []
    try:
        request_body = generate_qapi_payload(
            schema_version=schema_version,
            plugins=requested_plugins,
            on_skipped_private_action=skipped_private_actions.append,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    _add_default_environment(
        request_body,
        default_docker_image=default_docker_image,
        default_conda_prefix=default_conda_prefix,
    )
    _print_skipped_private_actions(skipped_private_actions)

    if output is not None:
        _write_json_output(request_body, output)

    if no_submit:
        if output is None:
            _write_json_output(request_body, None)
        return

    resolved_action_url = action_url or os.getenv("ACTION_URL")
    if dry_run and not resolved_action_url:
        console.print(
            "[yellow]Dry run enabled without an Action URL; generated the payload locally only.[/yellow]"
        )
        return

    url, status, response_body = submit_qapi_payload(
        request_body,
        action_url=action_url,
        submission_token=submission_token,
        timeout=timeout,
        dry_run=dry_run,
        force_overwrite=force_overwrite,
    )

    verb = "Previewed QAPI submit against" if dry_run else "Submitted QAPI to"
    console.print(f"[green]{verb}[/green] {url} [green](HTTP {status})[/green]")
    _print_submission_summary(response_body)
