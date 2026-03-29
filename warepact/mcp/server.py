"""Warepact MCP server — exposes contract tools to Claude, Cursor, and GPT.

Start it with:
    warepact mcp
    # or directly:
    python -m warepact.mcp.server

Then add to Claude Desktop / Cursor / any MCP-compatible client.
"""

from __future__ import annotations

from pathlib import Path

from fastmcp import FastMCP

from warepact.cli._credentials import load_credentials
from warepact.cli.check import _autodiscover
from warepact.core.engine import ContractCheckResult, ContractEngine
from warepact.core.exceptions import ContractCheckError, ContractNotFoundError
from warepact.core.registry import PluginRegistry
from warepact.parsers.yaml_parser import YAMLParser

mcp = FastMCP(
    "warepact",
    instructions=(
        "Warepact MCP server. Use these tools to check data contract health, "
        "list contracts, explain breaches, and generate contract scaffolds."
    ),
)

_parser = YAMLParser()
_CONTRACTS_DIR = Path("contracts")


def _engine() -> ContractEngine:
    _autodiscover()
    return ContractEngine(registry=PluginRegistry)


# ── Tools ──────────────────────────────────────────────────────────────────────

@mcp.tool()
def check_contract(contract_name: str) -> str:
    """
    Check if a data contract is currently passing all validations.

    Args:
        contract_name: The contract name (without .contract.yaml extension).

    Returns:
        A human-readable summary of pass/fail status and any failures.
    """
    try:
        contract = _parser.parse_file(
            _CONTRACTS_DIR / f"{contract_name}.contract.yaml"
        )
    except ContractNotFoundError:
        return f"Contract '{contract_name}' not found in {_CONTRACTS_DIR}/."
    except Exception as exc:
        return f"Error loading contract '{contract_name}': {exc}"

    contract.credentials = load_credentials(contract.warehouse)

    try:
        result = _engine().check(contract)
    except ContractCheckError as exc:
        return f"Check error for '{contract_name}': {exc}"

    return result.to_human_readable()


@mcp.tool()
def list_contracts() -> str:
    """
    List all registered contracts and their current health status.

    Returns:
        A formatted table of contract names, warehouses, and pass/fail counts.
    """
    try:
        contracts = _parser.parse_directory(_CONTRACTS_DIR)
    except Exception as exc:
        return f"Error listing contracts: {exc}"

    if not contracts:
        return f"No contracts found in {_CONTRACTS_DIR}/."

    engine = _engine()
    lines = [f"{'Contract':<24} {'Warehouse':<14} Status"]
    lines.append("-" * 50)

    for contract in contracts:
        contract.credentials = load_credentials(contract.warehouse)
        try:
            result = engine.check(contract)
            status = "PASSED" if result.passed else f"FAILED ({len(result.failures)} issue(s))"
        except ContractCheckError as exc:
            status = f"ERROR: {exc}"
        lines.append(f"{contract.name:<24} {contract.warehouse:<14} {status}")

    return "\n".join(lines)


@mcp.tool()
def explain_breach(contract_name: str) -> str:
    """
    Return a detailed explanation of why a contract is breached.

    Runs the contract check and returns all failure details with context
    to help diagnose the root cause.

    Args:
        contract_name: The contract name.

    Returns:
        Detailed failure messages and suggestions.
    """
    try:
        contract = _parser.parse_file(
            _CONTRACTS_DIR / f"{contract_name}.contract.yaml"
        )
    except ContractNotFoundError:
        return f"Contract '{contract_name}' not found."
    except Exception as exc:
        return f"Error loading contract: {exc}"

    contract.credentials = load_credentials(contract.warehouse)

    try:
        result = _engine().check(contract)
    except ContractCheckError as exc:
        return f"Could not check contract: {exc}"

    if result.passed:
        return f"Contract '{contract_name}' is currently passing — no breach to explain."

    lines = [f"Contract '{contract_name}' is FAILING with {len(result.failures)} issue(s):\n"]
    for i, failure in enumerate(result.failures, 1):
        lines.append(f"{i}. {failure.message}")
        if failure.details:
            for k, v in failure.details.items():
                if k != "failures":
                    lines.append(f"   {k}: {v}")

    if result.explanation:
        lines.append(f"\nAI Analysis:\n{result.explanation}")

    return "\n".join(lines)


@mcp.tool()
def get_contract_health() -> str:
    """
    Return an overall health dashboard across all contracts.

    Returns:
        Summary statistics: total, passing, failing, and a per-contract breakdown.
    """
    try:
        contracts = _parser.parse_directory(_CONTRACTS_DIR)
    except Exception as exc:
        return f"Error: {exc}"

    if not contracts:
        return "No contracts found."

    engine = _engine()
    results: list[tuple[str, ContractCheckResult | None]] = []
    for contract in contracts:
        contract.credentials = load_credentials(contract.warehouse)
        try:
            results.append((contract.name, engine.check(contract)))
        except ContractCheckError:
            results.append((contract.name, None))

    total = len(results)
    passed = sum(1 for _, r in results if r is not None and r.passed)
    failed = total - passed

    lines = [
        "Warepact Health Dashboard",
        f"{'─' * 30}",
        f"Total contracts : {total}",
        f"Passing         : {passed}",
        f"Failing         : {failed}",
        f"{'─' * 30}",
    ]
    for name, result in results:
        if result is None:
            lines.append(f"  ⚠ {name}: ERROR")
        elif result.passed:
            lines.append(f"  ✓ {name}")
        else:
            lines.append(f"  ✗ {name}: {len(result.failures)} failure(s)")

    return "\n".join(lines)


@mcp.tool()
def suggest_contract(table_name: str, warehouse: str = "duckdb") -> str:
    """
    Suggest a contract YAML scaffold for a warehouse table.

    Connects to the warehouse, introspects the schema, and returns a
    contract YAML string ready to be saved as <table>.contract.yaml.

    Args:
        table_name: Fully qualified table name (e.g. analytics.core.orders).
        warehouse:  Warehouse type (default: duckdb).

    Returns:
        A contract YAML string, or an error message.
    """
    _autodiscover()
    try:
        adapter = PluginRegistry.get_warehouse(warehouse)
    except Exception as exc:
        return f"Unknown warehouse '{warehouse}': {exc}"

    credentials = load_credentials(warehouse)
    try:
        adapter.connect(credentials)
        schema = adapter.get_schema(table_name)
        row_count = adapter.get_row_count(table_name)
    except Exception as exc:
        return f"Could not introspect table '{table_name}': {exc}"

    import os
    from warepact.cli._ai_enrichment import enrich_contract
    from warepact.cli.generate import _normalise_type
    import yaml as _yaml

    contract_name = table_name.split(".")[-1].lower()
    columns = [
        {"column": col["name"], "type": _normalise_type(col["type"])}
        for col in schema
    ]
    data: dict = {
        "version": 1,
        "name": contract_name,
        "warehouse": warehouse,
        "table": table_name,
        "schema": columns,
        "sla": {"min_rows": max(1, row_count // 2)},
    }
    if os.getenv("ANTHROPIC_API_KEY"):
        data = enrich_contract(data, adapter, table_name)
    return str(_yaml.dump(data, default_flow_style=False, sort_keys=False))


# ── Server entry point ─────────────────────────────────────────────────────────

def run_server(host: str = "localhost", port: int = 8765) -> None:
    """Start the MCP server (blocking)."""
    mcp.run()


if __name__ == "__main__":
    run_server()
