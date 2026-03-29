"""Shared Rich console and output helpers for the DataPact CLI."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich.text import Text

from warepact.core.engine import ContractCheckResult

console = Console()
err_console = Console(stderr=True)


def print_check_result(result: ContractCheckResult) -> None:
    """Render a contract check result to the terminal."""
    if result.passed:
        console.print(
            f":white_check_mark: [bold green]PASSED[/] — [cyan]{result.contract.name}[/]"
        )
    else:
        console.print(
            f":x: [bold red]FAILED[/] — [cyan]{result.contract.name}[/]"
        )

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("Check", style="dim")
    table.add_column("Status", width=8)
    table.add_column("Message")

    for r in result.results:
        if r.severity == "info" and r.passed:
            continue  # hide skipped/info-only passing checks for brevity
        status = Text("PASS", style="green") if r.passed else Text("FAIL", style="red")
        table.add_row(r.details.get("validator", ""), status, r.message)

    if table.row_count:
        console.print(table)

    if result.explanation:
        console.print(f"\n[bold]AI Explanation:[/]\n{result.explanation}")
