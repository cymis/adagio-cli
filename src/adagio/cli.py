from typing import Annotated
import typer
from pathlib import Path
from concurrent.futures import TimeoutError as FutureTimeoutError
import shlex
import json

from adagio.backend import (
    AgentLaunchRequest,
    DEFAULT_FLUX_IMAGE,
    FluxRPCSession,
    InstallRequest,
    install_compute_environment,
    run_agent_once,
)
from adagio.backend.dispatch import load_compute_environment
from rich.console import Console
from rich.panel import Panel

import time
import itertools
from rich.live import Live

app = typer.Typer(
    help="Adagio command line tool for processing pipelines created with the Adagio GUI."
)
debug_app = typer.Typer(help="Debug and diagnostics commands.", no_args_is_help=True)
app.add_typer(debug_app, name="debug")
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


@debug_app.command("dispatch")
def dispatch_cmd(
    command_parts: Annotated[
        list[str] | None,
        typer.Argument(
            help="Command and arguments. Use `--` before this to stop option parsing.",
        ),
    ] = None,
    command_opt: Annotated[
        str | None,
        typer.Option(
            "--command",
            help="One-off shell command executed inside the standardized compute environment.",
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
):
    """Run a one-off command in the configured compute environment.

    Use either:
    1. `adagio debug dispatch [opts] -- <cmd> [args...]`
    2. `adagio debug dispatch [opts] --command \"<cmd> [args...]\"`
    """

    command_from_args = shlex.join(command_parts) if command_parts else None

    if command_from_args and command_opt and command_from_args != command_opt:
        console.print("[red]Provide command either as an argument or with --command, not both.[/red]")
        raise typer.Exit(2)

    command = command_opt or command_from_args
    if not command:
        console.print("[red]Missing command. Provide it as an argument or with --command.[/red]")
        raise typer.Exit(2)

    def _print_output(stream_name: str, line: str):
        style = "cyan" if stream_name == "stdout" else "magenta"
        console.print(f"[{style}]{stream_name}>[/{style}] {line}")

    runtime_command: list[str] | None = None

    def _capture_start(binding):
        nonlocal runtime_command
        runtime_command = binding.command

    request = AgentLaunchRequest(
        agent_command=command,
        config_path=config,
        workdir=workdir,
        container_workdir=container_workdir,
    )

    try:
        report = run_agent_once(
            request,
            on_start=_capture_start,
            on_output=_print_output,
        )
    except Exception as e:
        console.print(f"[red]Dispatch failed:[/red] {e}")
        raise typer.Exit(1)

    rendered_cmd = " ".join(runtime_command or report.command)
    console.print(
        Panel.fit(
            f"Command exit code: [bold]{report.returncode}[/bold]\n"
            f"Runtime command: [bold]{rendered_cmd}[/bold]",
            title="[cyan]Adagio Dispatch Complete[/cyan]",
            border_style="cyan",
        )
    )

    if not report.ok:
        raise typer.Exit(report.returncode)


@debug_app.command("ping")
def ping_cmd(
    message: Annotated[
        str,
        typer.Option(
            "--message",
            help="Message payload sent to the RPC ping handler.",
        ),
    ] = "hello from host",
    timeout: Annotated[
        float,
        typer.Option(
            "--timeout",
            help="Seconds to wait for the ping RPC result.",
        ),
    ] = 30.0,
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
):
    """Verify RPC bridge health with a ping/pong roundtrip."""
    try:
        with FluxRPCSession(
            config_path=config,
            workdir=workdir,
            container_workdir=container_workdir,
        ) as session:
            sub_id = session.subscribe(
                lambda ev: console.print(
                    f"[yellow]event>[/yellow] {ev.event_type}: {ev.payload['message']}"
                )
            )
            try:
                result = session.call("ping", message=message).result(timeout=timeout)
            finally:
                session.unsubscribe(sub_id)
    except FutureTimeoutError:
        console.print(f"[red]RPC ping timed out after {timeout}s[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]RPC ping failed:[/red] {e}")
        raise typer.Exit(1)

    console.print(
        Panel.fit(
            f"Ping request: [bold]{message}[/bold]\n"
            f"Ping result: [bold]{result}[/bold]",
            title="[cyan]Adagio RPC Ping[/cyan]",
            border_style="cyan",
        )
    )


@debug_app.command("info")
def info_cmd(
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to compute-environment.json. Defaults to the installer output location.",
        ),
    ] = None,
):
    """Show compute environment configuration and how to update it."""
    try:
        env = load_compute_environment(config)
    except FileNotFoundError as e:
        path = Path(e.filename) if e.filename else (config or Path("compute-environment.json"))
        console.print(
            Panel.fit(
                f"Config file not found:\n[bold]{path}[/bold]\n\n"
                "Create it by running:\n"
                "[bold]adagio install --apply[/bold]\n\n"
                "Or provide an explicit path with:\n"
                "[bold]adagio debug info --config /path/to/compute-environment.json[/bold]",
                title="[red]Adagio Debug Info[/red]",
                border_style="red",
            )
        )
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Invalid compute environment config:[/red] {e}")
        raise typer.Exit(1)

    runtime = env.runtime
    console.print(
        Panel.fit(
            f"Config path: [bold]{env.path}[/bold]\n"
            f"Platform: [bold]{env.platform}[/bold]\n"
            f"Image: [bold]{env.image}[/bold]\n"
            f"Runtime engine: [bold]{runtime.engine}[/bold]\n"
            f"Bridge host: [bold]{runtime.bridge_host or 'auto'}[/bold]\n"
            f"Docker context: [bold]{runtime.docker_context or 'unset'}[/bold]\n"
            f"Docker host: [bold]{runtime.docker_host or 'unset'}[/bold]\n"
            f"Colima profile: [bold]{runtime.colima_profile or 'unset'}[/bold]\n"
            f"WSL wrapper: [bold]{runtime.via_wsl}[/bold]",
            title="[cyan]Adagio Debug Info[/cyan]",
            border_style="cyan",
        )
    )

    console.print("[bold]Raw config JSON[/bold]")
    console.print(Panel.fit(json.dumps(env.raw, indent=2), border_style="blue"))

    console.print("[bold]Modify this config[/bold]")
    console.print("1. Edit the JSON file shown in Config path.")
    console.print("2. Update values like `runtime.engine`, `runtime.bridge_host`, or `flux.image`.")
    console.print("3. Re-run `adagio debug info` to verify changes.")
    console.print("4. Use `--config <path>` with debug commands if you keep multiple configs.")


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
