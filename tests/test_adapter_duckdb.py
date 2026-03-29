"""Integration tests for DuckDBAdapter using a real in-memory DuckDB database."""

from __future__ import annotations

import pytest
from datetime import datetime

from datapact.adapters.warehouses.duckdb import DuckDBAdapter
from datapact.core.exceptions import WarehouseConnectionError
from datapact.core.registry import PluginRegistry
from datapact.interfaces.warehouse import WarehouseAdapter


@pytest.fixture
def adapter():
    """Connected in-memory DuckDB adapter with a test table."""
    a = DuckDBAdapter()
    a.connect({"database": ":memory:"})
    a._conn.execute("""
        CREATE TABLE orders (
            order_id  INTEGER,
            status    VARCHAR,
            amount    DOUBLE,
            nullable_col VARCHAR
        )
    """)
    a._conn.execute("""
        INSERT INTO orders VALUES
            (1, 'pending',   10.0, 'a'),
            (2, 'shipped',   20.0, NULL),
            (3, 'delivered', 30.0, 'c'),
            (4, 'pending',   40.0, NULL),
            (5, 'shipped',   50.0, 'e')
    """)
    return a


class TestDuckDBConnection:
    def test_is_warehouse_adapter(self):
        assert isinstance(DuckDBAdapter(), WarehouseAdapter)

    def test_registered_in_plugin_registry(self):
        adapter = PluginRegistry.get_warehouse("duckdb")
        assert isinstance(adapter, DuckDBAdapter)

    def test_not_connected_raises(self):
        a = DuckDBAdapter()
        with pytest.raises(WarehouseConnectionError, match="connect"):
            a.get_row_count("orders")

    def test_connect_in_memory(self):
        a = DuckDBAdapter()
        a.connect({"database": ":memory:"})
        assert a._conn is not None

    def test_connect_default_is_in_memory(self):
        a = DuckDBAdapter()
        a.connect({})
        assert a._conn is not None


class TestDuckDBGetSchema:
    def test_returns_column_list(self, adapter):
        schema = adapter.get_schema("orders")
        assert isinstance(schema, list)
        assert len(schema) == 4

    def test_schema_has_name_and_type(self, adapter):
        schema = adapter.get_schema("orders")
        for col in schema:
            assert "name" in col
            assert "type" in col

    def test_column_names_correct(self, adapter):
        names = {c["name"] for c in adapter.get_schema("orders")}
        assert names == {"order_id", "status", "amount", "nullable_col"}

    def test_types_are_strings(self, adapter):
        for col in adapter.get_schema("orders"):
            assert isinstance(col["type"], str)


class TestDuckDBGetRowCount:
    def test_returns_correct_count(self, adapter):
        assert adapter.get_row_count("orders") == 5

    def test_empty_table(self, adapter):
        adapter._conn.execute("CREATE TABLE empty_t (id INTEGER)")
        assert adapter.get_row_count("empty_t") == 0


class TestDuckDBGetLastUpdated:
    def test_returns_datetime(self, adapter):
        ts = adapter.get_last_updated("orders")
        assert isinstance(ts, datetime)

    def test_returns_timezone_aware(self, adapter):
        ts = adapter.get_last_updated("orders")
        assert ts.tzinfo is not None


class TestDuckDBRunQuery:
    def test_select_returns_rows_as_dicts(self, adapter):
        rows = adapter.run_query("SELECT order_id, status FROM orders ORDER BY order_id")
        assert len(rows) == 5
        assert rows[0] == {"order_id": 1, "status": "pending"}

    def test_aggregate_query(self, adapter):
        rows = adapter.run_query("SELECT COUNT(*) AS n FROM orders")
        assert rows[0]["n"] == 5

    def test_filtered_query(self, adapter):
        rows = adapter.run_query("SELECT * FROM orders WHERE status = 'pending'")
        assert len(rows) == 2

    def test_empty_result(self, adapter):
        rows = adapter.run_query("SELECT * FROM orders WHERE 1=0")
        assert rows == []


class TestDuckDBGetNullRates:
    def test_no_columns_returns_empty(self, adapter):
        assert adapter.get_null_rates("orders", []) == {}

    def test_zero_null_rate(self, adapter):
        rates = adapter.get_null_rates("orders", ["order_id"])
        assert rates["order_id"] == 0.0

    def test_partial_null_rate(self, adapter):
        # nullable_col: 2 of 5 rows are NULL → 0.4
        rates = adapter.get_null_rates("orders", ["nullable_col"])
        assert abs(rates["nullable_col"] - 0.4) < 0.001

    def test_multiple_columns(self, adapter):
        rates = adapter.get_null_rates("orders", ["order_id", "nullable_col"])
        assert rates["order_id"] == 0.0
        assert abs(rates["nullable_col"] - 0.4) < 0.001

    def test_returns_floats(self, adapter):
        rates = adapter.get_null_rates("orders", ["order_id"])
        assert isinstance(rates["order_id"], float)


class TestDuckDBRegistration:
    def test_each_get_returns_fresh_instance(self):
        a1 = PluginRegistry.get_warehouse("duckdb")
        a2 = PluginRegistry.get_warehouse("duckdb")
        assert a1 is not a2
