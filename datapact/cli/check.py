"""datapact check — validate one or all contracts."""

from __future__ import annotations

from pathlib import Path

import typer

from datapact.cli._console import console, err_console, print_check_result
from datapact.cli._credentials import load_credentials
from datapact.core.engine import ContractEngine
from datapact.core.exceptions import (
    ContractCheckError,
    ContractNotFoundError,
    ContractValidationError,
)
from datapact.core.registry import PluginRegistry
from datapact.parsers.yaml_parser import YAMLParser


def check_command(
    contract_name: str = typer.Argument(
        None,
        help="Contract name (without .contract.yaml extension). Omit to use --all.",
    ),
    all_contracts: bool = typer.Option(
        False, "--all", "-a", help="Check all contracts in the contracts directory."
    ),
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory containing contract YAML files.",
    ),
    output_json: bool = typer.Option(
        False, "--json", help="Output results as JSON."
    ),
) -> None:
    """Check one contract or all contracts in the contracts directory."""
    # Ensure validators and adapters are registered
    _autodiscover()

    parser = YAMLParser()
    engine = ContractEngine(registry=PluginRegistry)
    directory = Path(contracts_dir)

    if all_contracts:
        _check_all(parser, engine, directory, output_json)
    elif contract_name:
        _check_one(parser, engine, directory, contract_name, output_json)
    else:
        err_console.print(
            "[red]Error:[/] Specify a contract name or use [bold]--all[/].\n"
            "  datapact check orders\n"
            "  datapact check --all"
        )
        raise typer.Exit(code=1)


def _check_one(
    parser: YAMLParser,
    engine: ContractEngine,
    directory: Path,
    name: str,
    output_json: bool,
) -> None:
    path = directory / f"{name}.contract.yaml"
    if not path.exists():
        # Also try .yml
        path = directory / f"{name}.contract.yml"
    try:
        contract = parser.parse_file(path)
    except ContractNotFoundError:
        err_console.print(f"[red]Contract not found:[/] {name}")
        err_console.print(f"  Expected: [dim]{directory}/{name}.contract.yaml[/]")
        raise typer.Exit(code=1)
    except ContractValidationError as exc:
        err_console.print(f"[red]Invalid contract:[/] {exc}")
        raise typer.Exit(code=1)

    contract.credentials = load_credentials(contract.warehouse)

    try:
        result = engine.check(contract)
    except ContractCheckError as exc:
        err_console.print(f"[red]Check error:[/] {exc}")
        raise typer.Exit(code=1)

    if output_json:
        import json
        console.print(json.dumps(result.to_dict(), indent=2))
    else:
        print_check_result(result)

    raise typer.Exit(code=0 if result.passed else 1)


def _check_all(
    parser: YAMLParser,
    engine: ContractEngine,
    directory: Path,
    output_json: bool,
) -> None:
    try:
        contracts = parser.parse_directory(directory)
    except ContractNotFoundError:
        err_console.print(f"[red]Contracts directory not found:[/] {directory}")
        raise typer.Exit(code=1)
    except ContractValidationError as exc:
        err_console.print(f"[red]Parse errors:[/]\n{exc}")
        raise typer.Exit(code=1)

    if not contracts:
        console.print(f"[yellow]No contracts found in[/] {directory}/")
        raise typer.Exit(code=0)

    results = []
    for contract in contracts:
        contract.credentials = load_credentials(contract.warehouse)
        try:
            result = engine.check(contract)
        except ContractCheckError as exc:
            err_console.print(f"[red]{contract.name}:[/] {exc}")
            continue
        results.append(result)
        if not output_json:
            print_check_result(result)

    if output_json:
        import json
        console.print(json.dumps([r.to_dict() for r in results], indent=2))

    failed = [r for r in results if not r.passed]
    if failed:
        console.print(
            f"\n[bold red]{len(failed)}/{len(results)} contract(s) failed.[/]"
        )
        raise typer.Exit(code=1)
    else:
        console.print(
            f"\n[bold green]All {len(results)} contract(s) passed.[/]"
        )
        raise typer.Exit(code=0)


def _autodiscover() -> None:
    """Import validators and adapters so their @register decorators fire."""
    try:
        import datapact.validators  # noqa: F401
    except ImportError:
        pass
    PluginRegistry.autodiscover()
