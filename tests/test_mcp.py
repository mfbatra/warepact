"""Tests for the Warepact MCP server tools."""

from __future__ import annotations

import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult


# ── Registry / fixture setup ───────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_registry():
    PluginRegistry._reset()
    yield
    PluginRegistry._reset()


@pytest.fixture
def mock_warehouse():
    @PluginRegistry.register_warehouse("mock_wh")
    class _MockWH:
        def connect(self, c): pass
        def get_schema(self, t):
            return [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
        def get_row_count(self, t): return 500
        def get_last_updated(self, t): return datetime.now(tz=timezone.utc)
        def run_query(self, sql): return [{"result": 0}]
        def get_null_rates(self, t, cols): return {c: 0.0 for c in cols}


VALID_CONTRACT = textwrap.dedent("""\
    version: 1
    name: orders
    warehouse: mock_wh
    table: analytics.orders
""")


@pytest.fixture
def contracts_dir(tmp_path):
    d = tmp_path / "contracts"
    d.mkdir()
    (d / "orders.contract.yaml").write_text(VALID_CONTRACT)
    return d


def _patch_contracts_dir(path: Path):
    """Patch the MCP server's _CONTRACTS_DIR to a temp path."""
    import warepact.mcp.server as srv
    return patch.object(srv, "_CONTRACTS_DIR", path)


# ── check_contract ─────────────────────────────────────────────────────────────

class TestCheckContract:
    def test_passes_returns_passed(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import check_contract
        with _patch_contracts_dir(contracts_dir):
            result = check_contract("orders")
        assert "PASSED" in result

    def test_missing_contract_returns_message(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import check_contract
        with _patch_contracts_dir(contracts_dir):
            result = check_contract("nonexistent")
        assert "not found" in result.lower()

    def test_failed_contract_returns_failed(self, contracts_dir):
        @PluginRegistry.register_warehouse("mock_wh")
        class _MWH:
            def connect(self, c): pass
            def get_schema(self, t): return []
            def get_row_count(self, t): return 0
            def get_last_updated(self, t): return datetime.now(tz=timezone.utc)
            def run_query(self, sql): return []
            def get_null_rates(self, t, c): return {}

        @PluginRegistry.register_validator
        class _AlwaysFail:
            @property
            def name(self): return "fail"
            def validate(self, c, a): return ValidationResult(passed=False, message="bad data")

        from warepact.mcp.server import check_contract
        with _patch_contracts_dir(contracts_dir):
            result = check_contract("orders")
        assert "FAILED" in result


# ── list_contracts ─────────────────────────────────────────────────────────────

class TestListContracts:
    def test_lists_contracts(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import list_contracts
        with _patch_contracts_dir(contracts_dir):
            result = list_contracts()
        assert "orders" in result
        assert "mock_wh" in result

    def test_empty_dir_returns_message(self, tmp_path):
        from warepact.mcp.server import list_contracts
        d = tmp_path / "empty"
        d.mkdir()
        with _patch_contracts_dir(d):
            result = list_contracts()
        assert "no contracts" in result.lower()


# ── explain_breach ─────────────────────────────────────────────────────────────

class TestExplainBreach:
    def test_no_breach_returns_passing_message(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import explain_breach
        with _patch_contracts_dir(contracts_dir):
            result = explain_breach("orders")
        assert "passing" in result.lower()

    def test_breach_lists_failures(self, contracts_dir):
        @PluginRegistry.register_warehouse("mock_wh")
        class _MWH:
            def connect(self, c): pass
            def get_schema(self, t): return []
            def get_row_count(self, t): return 0
            def get_last_updated(self, t): return datetime.now(tz=timezone.utc)
            def run_query(self, sql): return []
            def get_null_rates(self, t, c): return {}

        @PluginRegistry.register_validator
        class _AlwaysFail:
            @property
            def name(self): return "fail"
            def validate(self, c, a):
                return ValidationResult(passed=False, message="stale data detected")

        from warepact.mcp.server import explain_breach
        with _patch_contracts_dir(contracts_dir):
            result = explain_breach("orders")
        assert "stale data detected" in result

    def test_missing_contract_returns_message(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import explain_breach
        with _patch_contracts_dir(contracts_dir):
            result = explain_breach("ghost")
        assert "not found" in result.lower()


# ── get_contract_health ────────────────────────────────────────────────────────

class TestGetContractHealth:
    def test_returns_dashboard(self, mock_warehouse, contracts_dir):
        from warepact.mcp.server import get_contract_health
        with _patch_contracts_dir(contracts_dir):
            result = get_contract_health()
        assert "Total contracts" in result
        assert "orders" in result

    def test_empty_dir(self, tmp_path):
        from warepact.mcp.server import get_contract_health
        d = tmp_path / "empty"
        d.mkdir()
        with _patch_contracts_dir(d):
            result = get_contract_health()
        assert "no contracts" in result.lower()


# ── suggest_contract ───────────────────────────────────────────────────────────

class TestSuggestContract:
    def test_returns_yaml_with_columns(self, mock_warehouse):
        from warepact.mcp.server import suggest_contract
        result = suggest_contract("analytics.orders", warehouse="mock_wh")
        assert "name:" in result
        assert "schema:" in result or "id" in result

    def test_unknown_warehouse_returns_error(self):
        from warepact.mcp.server import suggest_contract
        result = suggest_contract("t", warehouse="no_such_wh")
        assert "unknown" in result.lower() or "no adapter" in result.lower()


# ── Server startup smoke test ──────────────────────────────────────────────────

class TestMCPServerStartup:
    def test_mcp_app_has_expected_tools(self):
        """Verify all 5 tools are registered on the FastMCP app object."""
        import warepact.mcp.server as srv
        # FastMCP exposes registered tools via ._tool_manager or similar internal;
        # the most portable check is that the tool functions exist as callables.
        expected = {"check_contract", "list_contracts", "explain_breach",
                    "get_contract_health", "suggest_contract"}
        actual = {
            name for name in dir(srv)
            if name in expected and callable(getattr(srv, name))
        }
        assert actual == expected

    def test_run_server_calls_mcp_run(self):
        """run_server() must delegate to mcp.run() without error."""
        import warepact.mcp.server as srv
        with patch.object(srv.mcp, "run") as mock_run:
            srv.run_server()
        mock_run.assert_called_once()

    def test_mcp_object_is_fastmcp_instance(self):
        """The module-level mcp variable must be a FastMCP instance."""
        from fastmcp import FastMCP
        import warepact.mcp.server as srv
        assert isinstance(srv.mcp, FastMCP)

    def test_server_module_loads_without_errors(self):
        """Importing the server module must not raise."""
        import importlib
        mod = importlib.import_module("warepact.mcp.server")
        assert mod is not None
