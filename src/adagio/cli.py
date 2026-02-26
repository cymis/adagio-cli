from typing import Annotated
import typer
from pathlib import Path

from adagio.backend import (
    DEFAULT_FLUX_IMAGE,
    DispatchRequest,
    InstallRequest,
    dispatch_to_flux,
    enqueue_bridge_task,
    install_compute_environment,
)
from rich.console import Console
from rich.panel import Panel

import time
import itertools
from rich.live import Live

app = typer.Typer(
    help="Adagio command line tool for processing pipelines created with the Adagio GUI."
)
console = Console()


@app.command("hello")
def hello_cmd(
    input_file: Annotated[
        Path,
        typer.Option(
            "--input",
            "-i",
            help="Help text",
            exists=False,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    name: Annotated[
        str, typer.Option("--name", "-n", help="Say hello to someone else")
    ],
):
    """Say hello."""
    stick_figure = r"""
     O
    /|\
    / \
    """

    message = (
        f"[bold cyan]Hello {name}, {input_file} looks like a great file![/bold cyan]"
    )

    # Wrap the figure + message in a Rich panel for nicer output
    console.print(
        Panel.fit(
            f"{stick_figure}\n{message}",
            title="[yellow]Stick Figure[/yellow]",
            border_style="green",
        )
    )


@app.command("chicken")
def animate_big_chicken(
    laps: int = typer.Option(1, help="How many times to go left→right→left."),
    speed: float = typer.Option(0.08, help="Seconds between steps (lower = faster)."),
):
    """Animate a multi-line chicken walking across the screen."""

    # Two frames to fake wing flaps
    frames = [
        [
            "  __",
            " <(o )___",
            "   (  ._>",
            "    `---'",
        ],
        [
            "  __",
            " <( -)___",
            "   (o ._>",
            "    `---'",
        ],
    ]
    flap = itertools.cycle(frames)

    width = console.size.width
    rightmost = max(10, width - 12)

    def render(pos: int, art: list[str]) -> str:
        # Shift each line horizontally by pos
        shifted = [" " * pos + line for line in art]
        # Pad lines so Live keeps height/width stable
        padded = [line.ljust(width) for line in shifted]
        return "\n".join(padded)

    with Live(
        render(0, next(flap)), console=console, refresh_per_second=30, transient=True
    ) as live:
        for _ in range(laps):
            # Left → Right
            for x in range(0, rightmost):
                live.update(render(x, next(flap)))
                time.sleep(speed)
            # Right → Left
            for x in range(rightmost, 0, -1):
                live.update(render(x, next(flap)))
                time.sleep(speed)

    console.print("[bold yellow]🐔 Big chicken says cluck![/bold yellow]")


@app.command("install")
def install_cmd(
    apply: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Execute setup actions. Without this flag, run in dry-run mode.",
        ),
    ] = False,
    image: Annotated[
        str,
        typer.Option(
            "--image",
            help="Container image used as the baseline Flux execution environment.",
        ),
    ] = DEFAULT_FLUX_IMAGE,
    macos_profile: Annotated[
        str,
        typer.Option(
            "--macos-profile",
            help="Colima profile name used on macOS for Docker isolation.",
        ),
    ] = "adagio",
):
    """Install and standardize compute runtime resources for cross-platform job dispatch."""
    request = InstallRequest(apply=apply, image=image, macos_profile=macos_profile)
    report = install_compute_environment(request)

    mode = "apply" if apply else "dry-run"
    console.print(
        Panel.fit(
            f"Platform: [bold]{report.platform}[/bold]\n"
            f"Mode: [bold]{mode}[/bold]\n"
            f"Runtime: [bold]{report.runtime or 'unresolved'}[/bold]\n"
            f"Image: [bold]{report.image}[/bold]",
            title="[cyan]Adagio Install[/cyan]",
            border_style="cyan",
        )
    )

    styles = {
        "ok": ("✓", "green"),
        "changed": ("●", "cyan"),
        "skipped": ("○", "yellow"),
        "failed": ("✗", "red"),
    }
    for step in report.steps:
        icon, color = styles[step.status]
        console.print(f"[{color}]{icon}[/{color}] {step.name}: {step.detail}")
        if step.command:
            console.print(f"    [dim]{step.command}[/dim]")

    if report.config_path:
        console.print(f"Config path: [bold]{report.config_path}[/bold]")

    if not report.ok:
        raise typer.Exit(1)


@app.command("dispatch")
def dispatch_cmd(
    agent_cmd: Annotated[
        str,
        typer.Option(
            "--agent-cmd",
            help=(
                "Shell command executed inside the standardized Flux environment. "
                "It receives ADAGIO_BRIDGE_* env vars for host callbacks."
            ),
        ),
    ],
    task: Annotated[
        list[str] | None,
        typer.Option(
            "--task",
            help=(
                "Initial task for the agent bridge queue. "
                "Repeat this option to enqueue multiple tasks."
            ),
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to compute-environment.json. Defaults to the installer output location.",
        ),
    ] = None,
    workdir: Annotated[
        Path | None,
        typer.Option(
            "--workdir",
            help="Host path bind-mounted into the runtime for the agent command.",
        ),
    ] = None,
    container_workdir: Annotated[
        str,
        typer.Option(
            "--container-workdir",
            help="Target mount path inside the runtime for --workdir.",
        ),
    ] = "/workspace",
    bridge_host: Annotated[
        str | None,
        typer.Option(
            "--bridge-host",
            help="Hostname the in-runtime agent should use to reach the host bridge.",
        ),
    ] = None,
    bridge_bind: Annotated[
        str,
        typer.Option(
            "--bridge-bind",
            help="Host interface for binding the bridge server.",
        ),
    ] = "0.0.0.0",
    bridge_port: Annotated[
        int,
        typer.Option(
            "--bridge-port",
            min=0,
            max=65535,
            help="Bridge server port. Use 0 for an ephemeral port.",
        ),
    ] = 0,
    bridge_token: Annotated[
        str | None,
        typer.Option(
            "--bridge-token",
            help="Static auth token for bridge endpoints. Defaults to a generated token.",
        ),
    ] = None,
):
    """Dispatch an agent command in the compute environment with host callback bridge support."""
    def _print_output(stream_name: str, line: str):
        style = "cyan" if stream_name == "stdout" else "magenta"
        console.print(f"[{style}]{stream_name}>[/{style}] {line}")

    def _print_event(event):
        message = event.payload.get("message")
        if message:
            console.print(f"[green]event[{event.event_type}] {message}[/green]")
        else:
            console.print(f"[green]event[{event.event_type}] {event.payload}[/green]")

    def _print_start(session):
        console.print(
            Panel.fit(
                f"Host bridge URL: [bold]{session.host_bridge_url}[/bold]\n"
                f"Agent bridge URL: [bold]{session.agent_bridge_url}[/bold]\n"
                f"Bridge token: [bold]{session.token}[/bold]\n"
                f"Runtime command: [bold]{' '.join(session.command)}[/bold]",
                title="[cyan]Adagio Dispatch Started[/cyan]",
                border_style="cyan",
            )
        )
        console.print(
            "Queue tasks from another shell while dispatch is running:\n"
            f"  adagio dispatch-task --bridge-url {session.host_bridge_url} "
            f"--token {session.token} --task '<payload>'"
        )

    request = DispatchRequest(
        agent_command=agent_cmd,
        tasks=list(task or []),
        config_path=config,
        workdir=workdir,
        container_workdir=container_workdir,
        bridge_bind=bridge_bind,
        bridge_port=bridge_port,
        bridge_host=bridge_host,
        bridge_token=bridge_token,
    )

    try:
        report = dispatch_to_flux(
            request,
            on_start=_print_start,
            on_output=_print_output,
            on_event=_print_event,
        )
    except Exception as e:
        console.print(f"[red]Dispatch failed:[/red] {e}")
        raise typer.Exit(1)

    console.print(
        Panel.fit(
            f"Command exit code: [bold]{report.returncode}[/bold]\n"
            f"Events received: [bold]{len(report.events)}[/bold]",
            title="[cyan]Adagio Dispatch Complete[/cyan]",
            border_style="cyan",
        )
    )

    if not report.ok:
        raise typer.Exit(report.returncode)


@app.command("dispatch-task")
def dispatch_task_cmd(
    bridge_url: Annotated[
        str,
        typer.Option(
            "--bridge-url",
            help="Host bridge URL from `adagio dispatch` output.",
        ),
    ],
    token: Annotated[
        str,
        typer.Option(
            "--token",
            help="Bridge auth token from `adagio dispatch` output.",
        ),
    ],
    task: Annotated[
        str,
        typer.Option(
            "--task",
            help="Task payload added to the bridge queue for the in-runtime agent.",
        ),
    ],
):
    """Enqueue a task into a running dispatch bridge."""
    try:
        enqueue_bridge_task(bridge_url=bridge_url, token=token, task=task)
    except Exception as e:
        console.print(f"[red]Failed to enqueue task:[/red] {e}")
        raise typer.Exit(1)

    console.print("[green]Task queued.[/green]")


@app.callback(invoke_without_command=True)
def main_callback(
    version: Annotated[bool, typer.Option("--version", help="Show version")] = False,
):
    """Adagio command line tool version."""
    if version:
        try:
            from importlib.metadata import PackageNotFoundError
            from importlib.metadata import version as get_version
        except ImportError:
            from importlib_metadata import PackageNotFoundError  # type: ignore
            from importlib_metadata import version as get_version  # type: ignore
        try:
            package_version = get_version("adagio")
            console.print(f"Adagio {package_version}")
        except PackageNotFoundError:
            console.print("Adagio version unknown (not installed as a package)")

            raise typer.Exit()


if __name__ == "__main__":
    app()
