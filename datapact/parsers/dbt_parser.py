"""dbt manifest parser — auto-generate Contract objects from dbt manifest.json.

Reads a dbt manifest.json (v4–v12) and turns each model node into a Contract
scaffold pre-populated with column names and types from the manifest.

Usage:
    parser = DbtParser()
    contracts = parser.parse_manifest("target/manifest.json", warehouse="snowflake")
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from datapact.core.contract import Contract, ColumnSpec
from datapact.core.exceptions import ContractNotFoundError, ContractValidationError


class DbtParser:
    """Generates Contract scaffolds from a dbt manifest.json file."""

    def parse_manifest(
        self,
        manifest_path: str | Path,
        warehouse: str = "snowflake",
        select: list[str] | None = None,
    ) -> list[Contract]:
        """
        Parse a dbt manifest and return one Contract per model.

        Args:
            manifest_path: Path to dbt's manifest.json.
            warehouse:     Warehouse type to embed in each contract.
            select:        Optional list of model names to include.
                           If None, all models are included.

        Returns:
            List of Contract objects (no SLA or alert config — add those manually).
        """
        path = Path(manifest_path)
        if not path.exists():
            raise ContractNotFoundError(f"dbt manifest not found: {path}")

        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ContractValidationError(f"Invalid manifest JSON: {exc}") from exc

        nodes: dict[str, Any] = manifest.get("nodes", {})
        sources: dict[str, Any] = manifest.get("sources", {})

        contracts: list[Contract] = []

        for node_id, node in {**nodes, **sources}.items():
            # Only include model/seed nodes (not tests, analyses, etc.)
            resource_type = node.get("resource_type", "")
            if resource_type not in ("model", "seed", "source"):
                continue

            model_name = node.get("name", "")
            if not model_name:
                continue

            # Normalise to slug
            slug = model_name.lower().replace("-", "_")
            if select and slug not in select and model_name not in select:
                continue

            # Build fully qualified table name
            database = node.get("database", "")
            schema = node.get("schema", "")
            table_parts = [p for p in (database, schema, model_name) if p]
            table = ".".join(table_parts) if table_parts else model_name

            # Extract columns
            columns = []
            for col_name, col_info in node.get("columns", {}).items():
                dtype = _map_dbt_type(col_info.get("data_type", ""))
                columns.append(
                    ColumnSpec(column=col_name.lower(), type=dtype or "string")
                )

            try:
                contract = Contract(
                    name=slug,
                    warehouse=warehouse,
                    table=table,
                    description=node.get("description") or None,
                    schema=columns or None,
                )
            except Exception:
                continue  # skip nodes that can't map to a valid contract

            contracts.append(contract)

        return contracts

    def write_contracts(
        self,
        contracts: list[Contract],
        output_dir: str | Path = "contracts",
    ) -> list[Path]:
        """Write each contract to a .contract.yaml file and return written paths."""
        import yaml

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        for contract in contracts:
            data = contract.model_dump(by_alias=True, exclude_none=True)
            path = output_dir / f"{contract.name}.contract.yaml"
            path.write_text(
                yaml.dump(data, default_flow_style=False, sort_keys=False),
                encoding="utf-8",
            )
            written.append(path)

        return written


def _map_dbt_type(dbt_type: str) -> str:
    t = dbt_type.lower().strip()
    mapping = {
        "text": "string", "varchar": "string", "character varying": "string",
        "char": "string", "nvarchar": "string", "string": "string",
        "int": "integer", "integer": "integer", "bigint": "integer",
        "smallint": "integer", "int64": "integer", "number": "integer",
        "float": "float", "double": "float", "numeric": "float",
        "decimal": "float", "float64": "float", "real": "float",
        "boolean": "boolean", "bool": "boolean",
        "date": "date", "timestamp": "timestamp",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamp",
        "timestamptz": "timestamp",
    }
    return mapping.get(t, t or "string")
