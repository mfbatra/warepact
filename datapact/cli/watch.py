"""datapact watch — continuous contract monitoring."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from datapact.cli._console import console, err_console, print_check_result
from datapact.cli._credentials import load_credentials
from datapact.cli.check import _autodiscover
from datapact.core.engine import ContractEngine
from datapact.core.exceptions import ContractCheckError
from datapact.core.registry import PluginRegistry
from datapact.parsers.yaml_parser import YAMLParser


def watch_command(
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory containing contract YAML files.",
    ),
    interval: int = typer.Option(
        300,
        "--interval", "-i",
        help="Seconds between checks (default: 300 = 5 minutes). Ignored when --cron is set.",
    ),
    cron: Optional[str] = typer.Option(
        None,
        "--cron",
        help='Cron expression for scheduling (e.g. "0 * * * *" for hourly). '
             "Overrides --interval.",
    ),
    contract_name: str = typer.Argument(
        None,
        help="Watch a single contract. Omit to watch all.",
    ),
) -> None:
    """Continuously re-check contracts on a schedule. Press Ctrl+C to stop."""
    _autodiscover()

    parser = YAMLParser()
    engine = ContractEngine(registry=PluginRegistry)
    directory = Path(contracts_dir)

    if cron:
        _validate_cron(cron)
        schedule_desc = f"cron [bold]{cron}[/]"
    else:
        schedule_desc = f"every [bold]{interval}s[/]"

    console.print(
        f":eyes: Watching [cyan]{directory}/[/] on {schedule_desc}. "
        "Press [bold]Ctrl+C[/] to stop.\n"
    )

    try:
        while True:
            now = datetime.now(tz=timezone.utc)
            console.rule(f"[dim]{now.strftime('%Y-%m-%d %H:%M:%S UTC')}[/]")

            _run_checks(parser, engine, directory, contract_name)

            sleep_seconds = _seconds_until_next(cron, interval)
            next_dt = datetime.fromtimestamp(
                now.timestamp() + sleep_seconds, tz=timezone.utc
            )
            console.print(
                f"\n[dim]Next check at {next_dt.strftime('%H:%M:%S UTC')} "
                f"(in {sleep_seconds}s)…[/]"
            )
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        console.print("\n[yellow]Watch stopped.[/]")
        raise typer.Exit(code=0)


def _validate_cron(expr: str) -> None:
    """Raise UsageError if the cron expression is invalid."""
    try:
        from croniter import croniter
        if not croniter.is_valid(expr):
            raise typer.BadParameter(
                f"Invalid cron expression: {expr!r}. "
                "Expected 5 fields, e.g. \"0 * * * *\"."
            )
    except ImportError:
        pass  # croniter not available — skip validation, it will fail at runtime


def _seconds_until_next(cron: str | None, interval: int) -> int:
    """Return how many seconds to sleep before the next run."""
    if not cron:
        return interval
    try:
        from croniter import croniter
        now = datetime.now(tz=timezone.utc)
        it = croniter(cron, now)
        next_run: datetime = it.get_next(datetime)
        diff = (next_run - now).total_seconds()
        return max(1, int(diff))
    except ImportError:
        return interval


def _run_checks(
    parser: YAMLParser,
    engine: ContractEngine,
    directory: Path,
    contract_name: str | None,
) -> None:
    try:
        if contract_name:
            contracts = [parser.parse_file(directory / f"{contract_name}.contract.yaml")]
        else:
            contracts = parser.parse_directory(directory)
    except Exception as exc:
        err_console.print(f"[red]Error loading contracts:[/] {exc}")
        return

    for contract in contracts:
        contract.credentials = load_credentials(contract.warehouse)
        try:
            result = engine.check(contract)
            print_check_result(result)
        except ContractCheckError as exc:
            err_console.print(f"[red]{contract.name}:[/] {exc}")
