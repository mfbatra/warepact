"""Unit tests for SnowflakeAdapter — mocked via patch.dict(sys.modules)."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from warepact.core.exceptions import WarehouseConnectionError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.warehouse import WarehouseAdapter


# ── Snowflake module mock helpers ──────────────────────────────────────────────

def _make_sf_mocks():
    """Return (mock_connector, mock_conn) with snowflake.connector pre-wired."""
    mock_conn = MagicMock()
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_conn
    mock_sf = MagicMock()
    mock_sf.connector = mock_connector
    return mock_connector, mock_conn, mock_sf


@contextmanager
def _patch_snowflake():
    """Context manager that injects fake snowflake into sys.modules."""
    mock_connector, mock_conn, mock_sf = _make_sf_mocks()
    modules = {
        "snowflake": mock_sf,
        "snowflake.connector": mock_connector,
    }
    with patch.dict(sys.modules, modules, clear=False):
        yield mock_connector, mock_conn


@pytest.fixture
def sf():
    """Yields (SnowflakeAdapter instance, mock_connector, mock_conn)."""
    with _patch_snowflake() as (mock_connector, mock_conn):
        # Import inside the patch so the adapter sees the mock on connect()
        # Fresh class to avoid module-level caching
        from warepact.adapters.warehouses.snowflake import SnowflakeAdapter
        a = SnowflakeAdapter()
        a.connect({"account": "xy12345", "user": "u", "password": "p"})
        yield a, mock_connector, mock_conn


# ── Interface / registration ───────────────────────────────────────────────────

def test_snowflake_is_warehouse_adapter():
    from warepact.adapters.warehouses.snowflake import SnowflakeAdapter
    assert issubclass(SnowflakeAdapter, WarehouseAdapter)


def test_snowflake_registered():
    from warepact.adapters.warehouses.snowflake import SnowflakeAdapter  # noqa: F401
    assert "snowflake" in PluginRegistry.list_warehouses()


# ── connect() ─────────────────────────────────────────────────────────────────

def test_connect_calls_sf_connector():
    with _patch_snowflake() as (mock_connector, _):
        from warepact.adapters.warehouses.snowflake import SnowflakeAdapter
        a = SnowflakeAdapter()
        a.connect({"account": "xy12345", "user": "u", "password": "p"})
        mock_connector.connect.assert_called_once()


def test_connect_failure_raises():
    with _patch_snowflake() as (mock_connector, _):
        mock_connector.connect.side_effect = Exception("auth failed")
        from warepact.adapters.warehouses.snowflake import SnowflakeAdapter
        a = SnowflakeAdapter()
        with pytest.raises(WarehouseConnectionError, match="auth failed"):
            a.connect({"user": "x"})


def test_not_connected_raises():
    from warepact.adapters.warehouses.snowflake import SnowflakeAdapter
    a = SnowflakeAdapter()
    with pytest.raises(WarehouseConnectionError, match="connect"):
        a.get_row_count("orders")


# ── get_schema() ───────────────────────────────────────────────────────────────

def test_get_schema_returns_column_list(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.__iter__ = MagicMock(return_value=iter([
        ("order_id", "NUMBER", "COLUMN", "Y", None, None),
        ("status", "TEXT", "COLUMN", "Y", None, None),
    ]))
    schema = a.get_schema("analytics.orders")
    assert schema == [
        {"name": "order_id", "type": "NUMBER"},
        {"name": "status", "type": "TEXT"},
    ]


def test_get_schema_executes_describe(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.__iter__ = MagicMock(return_value=iter([]))
    a.get_schema("mydb.myschema.orders")
    mock_cur.execute.assert_called_once_with("DESCRIBE TABLE mydb.myschema.orders")


# ── get_row_count() ────────────────────────────────────────────────────────────

def test_get_row_count_returns_integer(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (42,)
    assert a.get_row_count("orders") == 42


def test_get_row_count_none_returns_zero(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = None
    assert a.get_row_count("orders") == 0


# ── get_last_updated() ─────────────────────────────────────────────────────────

def test_get_last_updated_returns_aware_datetime(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_cur.fetchone.return_value = (ts,)
    result = a.get_last_updated("analytics.core.orders")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_get_last_updated_naive_gets_utc(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (datetime(2024, 6, 1, 12, 0, 0),)  # naive
    result = a.get_last_updated("orders")
    assert result.tzinfo == timezone.utc


def test_get_last_updated_none_returns_now(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (None,)
    result = a.get_last_updated("orders")
    assert isinstance(result, datetime)


def test_get_last_updated_uses_last_table_segment(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (datetime.now(tz=timezone.utc),)
    a.get_last_updated("db.schema.my_orders")
    sql = mock_cur.execute.call_args[0][0]
    assert "MY_ORDERS" in sql


# ── run_query() ────────────────────────────────────────────────────────────────

def test_run_query_returns_list_of_dicts(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchall.return_value = [{"order_id": 1}, {"order_id": 2}]
    rows = a.run_query("SELECT * FROM orders")
    assert rows == [{"order_id": 1}, {"order_id": 2}]


# ── get_null_rates() ───────────────────────────────────────────────────────────

def test_get_null_rates_empty_columns_returns_empty(sf):
    a, _, _ = sf
    assert a.get_null_rates("orders", []) == {}


def test_get_null_rates_returns_float_per_column(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (0.05, 0.10)
    rates = a.get_null_rates("orders", ["col_a", "col_b"])
    assert abs(rates["col_a"] - 0.05) < 0.001
    assert abs(rates["col_b"] - 0.10) < 0.001


def test_get_null_rates_none_defaults_to_zero(sf):
    a, _, mock_conn = sf
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (None,)
    rates = a.get_null_rates("orders", ["col_a"])
    assert rates["col_a"] == 0.0
