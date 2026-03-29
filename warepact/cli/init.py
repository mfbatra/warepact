"""warepact init — scaffold a contracts/ directory."""

from __future__ import annotations

from pathlib import Path

import typer

from warepact.cli._console import console
from warepact.cli._credentials import detect_warehouse_from_env, get_required_env_vars

_EXAMPLE_CONTRACT = """\
version: 1
name: {name}
# owner: data-team@company.com
warehouse: {warehouse}
table: your_schema.{name}

schema:
  - column: id
    type: integer
    not_null: true
    unique: true

sla:
  freshness_hours: 24
  min_rows: 1

# alerts:
#   - channel: slack
#     webhook_url: "${{SLACK_WEBHOOK}}"
#     on: [breach, recovery]
"""


def init_command(
    directory: str = typer.Argument("contracts", help="Directory to initialise."),
    warehouse: str = typer.Option(
        None,
        "--warehouse", "-w",
        help="Warehouse type (snowflake, duckdb, bigquery, …). Auto-detected from env if omitted.",
    ),
    name: str = typer.Option(
        "example",
        "--name", "-n",
        help="Name for the example contract.",
    ),
) -> None:
    """Scaffold a contracts/ directory with an example contract."""
    contracts_dir = Path(directory)

    detected = warehouse or detect_warehouse_from_env() or "duckdb"

    if contracts_dir.exists():
        console.print(f"[yellow]Directory already exists:[/] {contracts_dir}")
    else:
        contracts_dir.mkdir(parents=True)
        console.print(f":file_folder: Created [cyan]{contracts_dir}/[/]")

    example_path = contracts_dir / f"{name}.contract.yaml"
    if example_path.exists():
        console.print(f"[yellow]File already exists, skipping:[/] {example_path}")
    else:
        example_path.write_text(
            _EXAMPLE_CONTRACT.format(name=name, warehouse=detected),
            encoding="utf-8",
        )
        console.print(f":memo: Created [cyan]{example_path}[/]")

    required_vars = get_required_env_vars(detected)
    if required_vars:
        console.print(
            f"\n[bold]Detected warehouse:[/] [cyan]{detected}[/]\n"
            "Set these env vars before running [bold]warepact check[/]:\n"
            f"  {', '.join(required_vars)}"
        )

    console.print(
        f"\n[bold green]Done![/] Edit [cyan]{example_path}[/] "
        f"then run [bold]warepact check {name}[/]"
    )
