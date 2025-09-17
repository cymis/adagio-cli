from typing import Annotated
import typer
from pathlib import Path

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
