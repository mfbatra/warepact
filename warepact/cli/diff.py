"""warepact diff — compare two versions of a contract."""

from __future__ import annotations

from pathlib import Path

import typer

from warepact.cli._console import console, err_console
from warepact.core.contract import Contract
from warepact.core.exceptions import ContractNotFoundError, ContractValidationError
from warepact.parsers.yaml_parser import YAMLParser


def diff_command(
    contract_name: str = typer.Argument(..., help="Contract name to diff."),
    version_a: str = typer.Argument(..., help="First version file or 'current'."),
    version_b: str = typer.Argument(..., help="Second version file or 'current'."),
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory containing contract YAML files.",
    ),
) -> None:
    """Compare two versions of a contract and highlight schema/SLA changes."""
    parser = YAMLParser()
    directory = Path(contracts_dir)

    def _load(version: str) -> Contract:
        if version == "current":
            path = directory / f"{contract_name}.contract.yaml"
        else:
            path = Path(version) if Path(version).exists() else (
                directory / version
            )
        try:
            return parser.parse_file(path)
        except (ContractNotFoundError, ContractValidationError) as exc:
            err_console.print(f"[red]Could not load '{version}':[/] {exc}")
            raise typer.Exit(code=1)

    a = _load(version_a)
    b = _load(version_b)

    changes = _diff_contracts(a, b)

    if not changes:
        console.print(
            f"[green]No differences[/] between [cyan]{version_a}[/] and [cyan]{version_b}[/]."
        )
        raise typer.Exit(code=0)

    console.print(
        f"[bold]Diff:[/] [cyan]{contract_name}[/] "
        f"[dim]{version_a}[/] → [dim]{version_b}[/]\n"
    )
    for change in changes:
        console.print(f"  {change}")

    raise typer.Exit(code=0)


def _diff_contracts(a: Contract, b: Contract) -> list[str]:
    changes: list[str] = []

    # Top-level scalar fields
    for field in ("warehouse", "table", "owner", "description"):
        va = getattr(a, field, None)
        vb = getattr(b, field, None)
        if va != vb:
            changes.append(f"[yellow]~[/] [bold]{field}[/]: [red]{va}[/] → [green]{vb}[/]")

    # Schema changes
    cols_a = {c.column: c for c in (a.columns or [])}
    cols_b = {c.column: c for c in (b.columns or [])}

    for col in sorted(set(cols_a) - set(cols_b)):
        changes.append(f"[red]-[/] column [bold]{col}[/] removed")
    for col in sorted(set(cols_b) - set(cols_a)):
        changes.append(f"[green]+[/] column [bold]{col}[/] added ({cols_b[col].type})")
    for col in sorted(set(cols_a) & set(cols_b)):
        ca, cb = cols_a[col], cols_b[col]
        if ca.type != cb.type:
            changes.append(
                f"[yellow]~[/] column [bold]{col}[/] type: [red]{ca.type}[/] → [green]{cb.type}[/]"
            )
        if ca.not_null != cb.not_null:
            changes.append(
                f"[yellow]~[/] column [bold]{col}[/] not_null: "
                f"[red]{ca.not_null}[/] → [green]{cb.not_null}[/]"
            )

    # SLA changes
    if a.sla or b.sla:
        for attr in ("freshness_hours", "min_rows", "max_rows", "max_null_rate"):
            va = getattr(a.sla, attr, None) if a.sla else None
            vb = getattr(b.sla, attr, None) if b.sla else None
            if va != vb:
                changes.append(
                    f"[yellow]~[/] sla.[bold]{attr}[/]: [red]{va}[/] → [green]{vb}[/]"
                )

    return changes
