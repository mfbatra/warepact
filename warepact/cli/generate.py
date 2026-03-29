"""warepact generate — AI-generate a contract YAML from a warehouse table."""

from __future__ import annotations

from pathlib import Path

import typer
import yaml

import os

from warepact.cli._ai_enrichment import enrich_contract
from warepact.cli._console import console, err_console
from warepact.cli._credentials import load_credentials
from warepact.cli.check import _autodiscover
from warepact.core.exceptions import UnknownWarehouseError
from warepact.core.registry import PluginRegistry


def generate_command(
    table: str = typer.Argument(..., help="Fully qualified table name (e.g. db.schema.orders)."),
    warehouse: str = typer.Option(
        ...,
        "--warehouse", "-w",
        help="Warehouse type (snowflake, duckdb, bigquery, …).",
    ),
    output: str = typer.Option(
        None,
        "--output", "-o",
        help="Write contract to this file. Defaults to <table_leaf>.contract.yaml.",
    ),
    contracts_dir: str = typer.Option(
        "contracts",
        "--dir", "-d",
        help="Directory to write the generated contract.",
    ),
) -> None:
    """
    Introspect a warehouse table and generate a contract YAML scaffold.

    Connects to the warehouse, reads the schema, and writes a .contract.yaml
    with sensible defaults.  Extend it with SLA and alert config afterwards.
    """
    _autodiscover()

    try:
        adapter = PluginRegistry.get_warehouse(warehouse)
    except UnknownWarehouseError as exc:
        err_console.print(f"[red]{exc}[/]")
        raise typer.Exit(code=1)

    credentials = load_credentials(warehouse)
    try:
        adapter.connect(credentials)
    except Exception as exc:
        err_console.print(f"[red]Could not connect to {warehouse}:[/] {exc}")
        raise typer.Exit(code=1)

    try:
        schema = adapter.get_schema(table)
        row_count = adapter.get_row_count(table)
    except Exception as exc:
        err_console.print(f"[red]Could not introspect table '{table}':[/] {exc}")
        raise typer.Exit(code=1)

    contract_name = table.split(".")[-1].lower().replace("-", "_")
    columns = [
        {"column": col["name"], "type": _normalise_type(col["type"])}
        for col in schema
    ]

    contract_data = {
        "version": 1,
        "name": contract_name,
        "warehouse": warehouse,
        "table": table,
        "schema": columns,
        "sla": {
            "min_rows": max(1, row_count // 2),  # conservative floor
        },
    }

    # AI enrichment — only if ANTHROPIC_API_KEY is set
    if os.getenv("ANTHROPIC_API_KEY"):
        console.print("[dim]Enriching contract with AI...[/]")
        contract_data = enrich_contract(contract_data, adapter, table)

    # Determine output path
    out_file = output or f"{contract_name}.contract.yaml"
    out_path = Path(contracts_dir) / out_file
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_path.write_text(
        yaml.dump(contract_data, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )

    console.print(f":sparkles: Generated [cyan]{out_path}[/]")
    console.print(
        f"  {len(columns)} column(s) · {row_count:,} row(s) detected\n"
        f"  Edit the file to add SLA thresholds and alert channels, "
        f"then run [bold]warepact check {contract_name}[/]"
    )


def _normalise_type(raw: str) -> str:
    """Map warehouse-specific type strings to generic contract types."""
    t = raw.lower().split("(")[0].strip()
    mapping = {
        "number": "integer",
        "numeric": "float",
        "decimal": "float",
        "double": "float",
        "float4": "float",
        "float8": "float",
        "real": "float",
        "bigint": "integer",
        "smallint": "integer",
        "int": "integer",
        "int2": "integer",
        "int4": "integer",
        "int8": "integer",
        "int64": "integer",
        "varchar": "string",
        "text": "string",
        "char": "string",
        "nvarchar": "string",
        "string": "string",
        "bool": "boolean",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp_tz": "timestamp",
        "timestamptz": "timestamp",
        "datetime": "timestamp",
    }
    return mapping.get(t, t)
