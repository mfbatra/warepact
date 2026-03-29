"""DataPact CLI entry point."""

from __future__ import annotations

import typer

from warepact.cli.check import check_command
from warepact.cli.diff import diff_command
from warepact.cli.generate import generate_command
from warepact.cli.init import init_command
from warepact.cli.publish import publish_command
from warepact.cli.report import report_command
from warepact.cli.watch import watch_command

app = typer.Typer(
    name="warepact",
    help="The dbt of data contracts — define, enforce, and evolve data contracts.",
    add_completion=True,
    no_args_is_help=True,
)

app.command("init")(init_command)
app.command("check")(check_command)
app.command("watch")(watch_command)
app.command("generate")(generate_command)
app.command("publish")(publish_command)
app.command("diff")(diff_command)
app.command("report")(report_command)


@app.command("mcp")
def mcp_command(
    host: str = typer.Option("localhost", help="Host to bind to."),
    port: int = typer.Option(8765, help="Port to listen on."),
) -> None:
    """Start the DataPact MCP server for Claude / Cursor integration."""
    from warepact.cli._console import console
    try:
        from warepact.mcp.server import run_server
        console.print(f":satellite: Starting MCP server on [cyan]{host}:{port}[/]")
        run_server(host=host, port=port)
    except ImportError as exc:
        console.print(f"[red]MCP server dependencies not installed:[/] {exc}")
        raise typer.Exit(code=1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
