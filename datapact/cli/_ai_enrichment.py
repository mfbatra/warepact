"""AI enrichment for generated contract scaffolds.

Requires ANTHROPIC_API_KEY.  If the key is absent or the API call fails,
the original scaffold is returned unchanged.
"""

from __future__ import annotations

import json
import os
from typing import Any

from datapact.interfaces.warehouse import WarehouseAdapter


# Maximum distinct values to sample per column (keeps prompt small)
_MAX_DISTINCT = 20
# Only sample distinct values for columns with <= this many distinct entries
_CARDINALITY_LIMIT = 50


def enrich_contract(
    contract_data: dict[str, Any],
    adapter: WarehouseAdapter,
    table: str,
) -> dict[str, Any]:
    """
    Enrich *contract_data* using Claude AI.

    Adds to each column:
    - ``description`` — a human-readable explanation of the column
    - ``allowed_values`` — for low-cardinality string/boolean columns
    - ``min`` / ``max`` — for numeric columns based on actual data

    Returns the original dict unchanged if ``ANTHROPIC_API_KEY`` is not set
    or if the API call fails.
    """
    if not os.getenv("ANTHROPIC_API_KEY"):
        return contract_data

    stats = _gather_column_stats(adapter, table, contract_data.get("schema", []))
    enriched_schema = _call_claude(contract_data, stats)
    if enriched_schema is None:
        return contract_data
    return {**contract_data, "schema": enriched_schema}


# ── Statistics gathering ────────────────────────────────────────────────────────

def _gather_column_stats(
    adapter: WarehouseAdapter,
    table: str,
    columns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return per-column statistics gathered from the warehouse."""
    stats: list[dict[str, Any]] = []
    for col in columns:
        name = col["name"]
        col_type = col.get("type", "")
        entry: dict[str, Any] = {"name": name, "type": col_type}

        if _is_numeric(col_type):
            try:
                rows = adapter.run_query(
                    f"SELECT MIN({name}) AS min_val, MAX({name}) AS max_val FROM {table}"
                )
                if rows:
                    entry["min"] = rows[0].get("min_val")
                    entry["max"] = rows[0].get("max_val")
            except Exception:
                pass

        elif _is_low_cardinality_candidate(col_type):
            try:
                count_rows = adapter.run_query(
                    f"SELECT COUNT(DISTINCT {name}) AS cnt FROM {table}"
                )
                cnt = int(count_rows[0].get("cnt", 0)) if count_rows else 0
                if 0 < cnt <= _CARDINALITY_LIMIT:
                    distinct_rows = adapter.run_query(
                        f"SELECT DISTINCT {name} FROM {table} "
                        f"WHERE {name} IS NOT NULL LIMIT {_MAX_DISTINCT}"
                    )
                    entry["distinct_values"] = [
                        r.get(name) for r in distinct_rows if r.get(name) is not None
                    ]
            except Exception:
                pass

        stats.append(entry)
    return stats


def _is_numeric(col_type: str) -> bool:
    t = col_type.lower().split("(")[0].strip()
    return t in {
        "integer", "int", "bigint", "smallint", "float", "double", "decimal",
        "numeric", "real", "float4", "float8", "int2", "int4", "int8", "int64",
        "number",
    }


def _is_low_cardinality_candidate(col_type: str) -> bool:
    t = col_type.lower().split("(")[0].strip()
    return t in {"string", "varchar", "text", "char", "nvarchar", "boolean", "bool"}


# ── Claude API call ─────────────────────────────────────────────────────────────

def _call_claude(
    contract_data: dict[str, Any],
    stats: list[dict[str, Any]],
) -> list[dict[str, Any]] | None:
    """
    Ask Claude to enrich the column definitions.

    Returns the enriched schema list, or None on failure.
    """
    try:
        import anthropic
    except ImportError:
        return None

    prompt = _build_prompt(contract_data, stats)
    try:
        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text if message.content else ""
        return _parse_response(raw, contract_data.get("schema", []))
    except Exception:
        return None


def _build_prompt(
    contract_data: dict[str, Any],
    stats: list[dict[str, Any]],
) -> str:
    table = contract_data.get("table", "unknown")
    warehouse = contract_data.get("warehouse", "unknown")
    return f"""You are helping generate a data contract for the table `{table}` in a {warehouse} warehouse.

Here are the columns with statistics gathered from the live table:

{json.dumps(stats, indent=2, default=str)}

For each column, return a JSON array where each element is an object with:
- "column": the column name (unchanged)
- "type": the column type (unchanged)
- "description": a concise 1-sentence description of what this column represents
- "allowed_values": (optional) an array of allowed string values if the column is an enum/category
- "min": (optional) the minimum allowed numeric value based on the data
- "max": (optional) the maximum allowed numeric value based on the data
- "not_null": (optional) true if the column should never be null based on context

Return ONLY the JSON array, no explanation, no markdown fences."""


def _parse_response(
    raw: str,
    original_schema: list[dict[str, Any]],
) -> list[dict[str, Any]] | None:
    """Parse Claude's JSON response, falling back to original schema on error."""
    text = raw.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(
            line for line in lines if not line.startswith("```")
        ).strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return None
