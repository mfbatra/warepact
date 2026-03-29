"""Unit tests for BigQuery, Redshift, and Postgres warehouse adapters.

All external connectors are mocked — no real credentials needed.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ── BigQuery ──────────────────────────────────────────────────────────────────

class TestBigQueryAdapter:
    @pytest.fixture(autouse=True)
    def mock_bigquery(self):
        """Inject fake google.cloud.bigquery and google.oauth2 modules."""
        bq_mock = MagicMock()
        oauth_mock = MagicMock()
        with patch.dict(sys.modules, {
            "google": MagicMock(),
            "google.cloud": MagicMock(),
            "google.cloud.bigquery": bq_mock,
            "google.oauth2": oauth_mock,
            "google.oauth2.service_account": oauth_mock.service_account,
        }):
            self.bq_mod = bq_mock
            yield

    def _make_adapter(self):
        from datapact.adapters.warehouses.bigquery import BigQueryAdapter
        adapter = BigQueryAdapter()
        adapter._client = MagicMock()
        return adapter

    def test_connect_without_creds_path(self):
        from datapact.adapters.warehouses.bigquery import BigQueryAdapter
        adapter = BigQueryAdapter()
        adapter.connect({"project": "my-project"})
        # After connect(), _client must be set (lazy import means we verify state not call)
        assert adapter._client is not None
        assert adapter._project == "my-project"

    def test_connect_missing_package_raises(self):
        from datapact.core.exceptions import WarehouseConnectionError
        with patch.dict(sys.modules, {"google.cloud.bigquery": None,
                                       "google.cloud": None, "google": None}):
            from datapact.adapters.warehouses.bigquery import BigQueryAdapter
            adapter = BigQueryAdapter()
            with pytest.raises(WarehouseConnectionError, match="google-cloud-bigquery"):
                adapter.connect({})

    def test_get_schema(self):
        adapter = self._make_adapter()
        mock_field = MagicMock()
        mock_field.name = "order_id"
        mock_field.field_type = "INTEGER"
        adapter._client.get_table.return_value.schema = [mock_field]
        result = adapter.get_schema("project.dataset.orders")
        assert result == [{"name": "order_id", "type": "INTEGER"}]

    def test_get_row_count(self):
        adapter = self._make_adapter()
        adapter._client.get_table.return_value.num_rows = 42
        assert adapter.get_row_count("project.dataset.orders") == 42

    def test_get_last_updated_returns_modified(self):
        adapter = self._make_adapter()
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        adapter._client.get_table.return_value.modified = ts
        result = adapter.get_last_updated("project.dataset.orders")
        assert result == ts

    def test_get_last_updated_fallback_when_no_modified(self):
        adapter = self._make_adapter()
        adapter._client.get_table.return_value.modified = None
        result = adapter.get_last_updated("project.dataset.orders")
        assert result.tzinfo is not None

    def test_get_last_updated_adds_utc_to_naive(self):
        adapter = self._make_adapter()
        naive_ts = datetime(2024, 1, 15, 12, 0, 0)  # no tzinfo
        adapter._client.get_table.return_value.modified = naive_ts
        result = adapter.get_last_updated("project.dataset.orders")
        assert result.tzinfo == timezone.utc

    def test_run_query(self):
        adapter = self._make_adapter()
        mock_row = MagicMock()
        mock_row.__iter__ = lambda self: iter({"status": "ok"}.items())
        # dict(row) path — return actual dict
        adapter._client.query.return_value.result.return_value = [{"status": "ok"}]
        rows = adapter.run_query("SELECT status FROM orders")
        assert rows[0] == {"status": "ok"}

    def test_get_null_rates_empty_columns(self):
        adapter = self._make_adapter()
        assert adapter.get_null_rates("orders", []) == {}

    def test_not_connected_raises(self):
        from datapact.adapters.warehouses.bigquery import BigQueryAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = BigQueryAdapter()
        with pytest.raises(WarehouseConnectionError, match="Not connected"):
            adapter.get_schema("orders")


# ── Redshift ──────────────────────────────────────────────────────────────────

class TestRedshiftAdapter:
    @pytest.fixture(autouse=True)
    def mock_redshift(self):
        self.rc_mock = MagicMock()
        with patch.dict(sys.modules, {"redshift_connector": self.rc_mock}):
            yield

    def _make_adapter(self):
        from datapact.adapters.warehouses.redshift import RedshiftAdapter
        adapter = RedshiftAdapter()
        mock_conn = MagicMock()
        adapter._conn = mock_conn
        self._mock_conn = mock_conn
        return adapter

    def test_connect_calls_connector(self):
        from datapact.adapters.warehouses.redshift import RedshiftAdapter
        adapter = RedshiftAdapter()
        creds = {"host": "rs.example.com", "user": "admin", "password": "secret",
                 "database": "prod", "port": "5439"}
        adapter.connect(creds)
        self.rc_mock.connect.assert_called_once_with(
            host="rs.example.com", database="prod",
            user="admin", password="secret", port=5439,
        )

    def test_connect_missing_package_raises(self):
        from datapact.core.exceptions import WarehouseConnectionError
        with patch.dict(sys.modules, {"redshift_connector": None}):
            from datapact.adapters.warehouses.redshift import RedshiftAdapter
            adapter = RedshiftAdapter()
            with pytest.raises(WarehouseConnectionError, match="redshift-connector"):
                adapter.connect({"host": "h", "user": "u", "password": "p"})

    def test_get_schema(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("order_id", "integer"), ("status", "varchar")]
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_schema("public.orders")
        assert result == [{"name": "order_id", "type": "integer"},
                          {"name": "status", "type": "varchar"}]

    def test_get_row_count(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (99,)
        self._mock_conn.cursor.return_value = mock_cur
        assert adapter.get_row_count("orders") == 99

    def test_get_last_updated_from_stl_insert(self):
        adapter = self._make_adapter()
        ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (ts,)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_last_updated("orders")
        assert result == ts

    def test_get_last_updated_fallback_on_error(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = Exception("no stl_insert")
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_last_updated("orders")
        assert result.tzinfo is not None

    def test_get_null_rates(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (0.1, 0.0)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_null_rates("orders", ["email", "name"])
        assert result == {"email": 0.1, "name": 0.0}

    def test_get_null_rates_empty(self):
        adapter = self._make_adapter()
        assert adapter.get_null_rates("orders", []) == {}

    def test_not_connected_raises(self):
        from datapact.adapters.warehouses.redshift import RedshiftAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = RedshiftAdapter()
        with pytest.raises(WarehouseConnectionError, match="Not connected"):
            adapter.get_row_count("orders")

    def test_run_query(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.description = [("col",)]
        mock_cur.fetchall.return_value = [("val",)]
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.run_query("SELECT col FROM tbl")
        assert result == [{"col": "val"}]


# ── Postgres ──────────────────────────────────────────────────────────────────

class TestPostgresAdapter:
    @pytest.fixture(autouse=True)
    def mock_psycopg2(self):
        self.pg_mock = MagicMock()
        self.pg_mock.extras = MagicMock()
        with patch.dict(sys.modules, {
            "psycopg2": self.pg_mock,
            "psycopg2.extras": self.pg_mock.extras,
        }):
            yield

    def _make_adapter(self):
        from datapact.adapters.warehouses.postgres import PostgresAdapter
        adapter = PostgresAdapter()
        mock_conn = MagicMock()
        adapter._conn = mock_conn
        self._mock_conn = mock_conn
        return adapter

    def test_connect_calls_psycopg2(self):
        from datapact.adapters.warehouses.postgres import PostgresAdapter
        adapter = PostgresAdapter()
        creds = {"host": "pg.example.com", "port": "5432", "database": "mydb",
                 "user": "admin", "password": "secret"}
        adapter.connect(creds)
        self.pg_mock.connect.assert_called_once_with(
            host="pg.example.com", port=5432, dbname="mydb",
            user="admin", password="secret",
        )

    def test_connect_missing_package_raises(self):
        from datapact.core.exceptions import WarehouseConnectionError
        with patch.dict(sys.modules, {"psycopg2": None, "psycopg2.extras": None}):
            from datapact.adapters.warehouses.postgres import PostgresAdapter
            adapter = PostgresAdapter()
            with pytest.raises(WarehouseConnectionError, match="psycopg2-binary"):
                adapter.connect({})

    def test_get_schema_public_schema(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("user_id", "integer"), ("email", "text")]
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_schema("users")
        # table_schema defaults to "public" when no schema prefix given
        assert result == [{"name": "user_id", "type": "integer"},
                          {"name": "email", "type": "text"}]

    def test_get_schema_with_explicit_schema(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("id", "bigint")]
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_schema("analytics.orders")
        # The query should be called with ("analytics", "orders")
        call_args = mock_cur.execute.call_args[0]
        assert call_args[1] == ("analytics", "orders")
        assert result == [{"name": "id", "type": "bigint"}]

    def test_get_row_count(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (123,)
        self._mock_conn.cursor.return_value = mock_cur
        assert adapter.get_row_count("orders") == 123

    def test_get_last_updated_from_pg_stat(self):
        adapter = self._make_adapter()
        ts = datetime(2024, 6, 1, tzinfo=timezone.utc)
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (ts,)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_last_updated("orders")
        assert result == ts

    def test_get_last_updated_fallback_on_null(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (None,)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_last_updated("orders")
        assert result.tzinfo is not None

    def test_get_last_updated_adds_utc_to_naive(self):
        adapter = self._make_adapter()
        naive_ts = datetime(2024, 1, 1, 10, 0, 0)
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (naive_ts,)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_last_updated("orders")
        assert result.tzinfo == timezone.utc

    def test_get_null_rates(self):
        adapter = self._make_adapter()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (0.2, 0.0)
        self._mock_conn.cursor.return_value = mock_cur
        result = adapter.get_null_rates("users", ["email", "name"])
        assert result == {"email": 0.2, "name": 0.0}

    def test_get_null_rates_empty(self):
        adapter = self._make_adapter()
        assert adapter.get_null_rates("users", []) == {}

    def test_not_connected_raises(self):
        from datapact.adapters.warehouses.postgres import PostgresAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = PostgresAdapter()
        with pytest.raises(WarehouseConnectionError, match="Not connected"):
            adapter.get_schema("orders")


# ── DatabricksAdapter ─────────────────────────────────────────────────────────

class TestDatabricksAdapter:
    @pytest.fixture(autouse=True)
    def mock_databricks_sql(self):
        """Inject a fake databricks.sql module."""
        self.db_mod = MagicMock()
        conn_mock = MagicMock()
        self.db_mod.connect.return_value = conn_mock
        self.conn_mock = conn_mock

        with patch.dict(sys.modules, {
            "databricks": MagicMock(sql=self.db_mod),
            "databricks.sql": self.db_mod,
        }):
            yield

    def _make_adapter(self):
        from datapact.adapters.warehouses.databricks import DatabricksAdapter
        adapter = DatabricksAdapter()
        adapter._conn = self.conn_mock
        return adapter

    def test_connect_calls_sql_connect(self):
        from datapact.adapters.warehouses.databricks import DatabricksAdapter
        adapter = DatabricksAdapter()
        creds = {
            "server_hostname": "host.azuredatabricks.net",
            "http_path": "/sql/1.0/warehouses/abc",
            "access_token": "dapi_token",
        }
        adapter.connect(creds)
        self.db_mod.connect.assert_called_once_with(
            server_hostname="host.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/abc",
            access_token="dapi_token",
        )

    def test_connect_missing_credentials_raises(self):
        from datapact.adapters.warehouses.databricks import DatabricksAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = DatabricksAdapter()
        with pytest.raises(WarehouseConnectionError, match="require"):
            adapter.connect({"server_hostname": "host"})  # missing http_path + token

    def test_connect_missing_package_raises(self):
        from datapact.adapters.warehouses.databricks import DatabricksAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = DatabricksAdapter()
        with patch.dict(sys.modules, {"databricks": None, "databricks.sql": None}):
            with pytest.raises((WarehouseConnectionError, ImportError)):
                adapter.connect({
                    "server_hostname": "h",
                    "http_path": "/p",
                    "access_token": "t",
                })

    def test_get_schema_parses_describe_output(self):
        adapter = self._make_adapter()
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.fetchall.return_value = [
            ("order_id", "INT", ""),
            ("status", "STRING", ""),
            ("# Partition Info", "", ""),  # should be skipped
        ]
        self.conn_mock.cursor.return_value = cursor_mock

        schema = adapter.get_schema("analytics.orders")
        assert len(schema) == 2
        assert schema[0] == {"name": "order_id", "type": "INT"}
        assert schema[1] == {"name": "status", "type": "STRING"}

    def test_get_row_count(self):
        adapter = self._make_adapter()
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.fetchone.return_value = (42,)
        self.conn_mock.cursor.return_value = cursor_mock

        assert adapter.get_row_count("analytics.orders") == 42

    def test_get_last_updated_uses_describe_history(self):
        adapter = self._make_adapter()
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        cursor_mock.fetchone.return_value = (ts, "WRITE", "user@example.com")
        self.conn_mock.cursor.return_value = cursor_mock

        result = adapter.get_last_updated("analytics.orders")
        assert result == ts

    def test_get_last_updated_falls_back_to_now_on_error(self):
        adapter = self._make_adapter()
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.execute.side_effect = Exception("DESCRIBE HISTORY not supported")
        self.conn_mock.cursor.return_value = cursor_mock

        result = adapter.get_last_updated("legacy.table")
        assert result.tzinfo is not None  # always tz-aware

    def test_run_query_returns_list_of_dicts(self):
        adapter = self._make_adapter()
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.description = [("id",), ("name",)]
        cursor_mock.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        self.conn_mock.cursor.return_value = cursor_mock

        rows = adapter.run_query("SELECT id, name FROM users")
        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_get_null_rates_empty_columns(self):
        adapter = self._make_adapter()
        assert adapter.get_null_rates("orders", []) == {}

    def test_not_connected_raises(self):
        from datapact.adapters.warehouses.databricks import DatabricksAdapter
        from datapact.core.exceptions import WarehouseConnectionError
        adapter = DatabricksAdapter()
        with pytest.raises(WarehouseConnectionError, match="Not connected"):
            adapter.get_row_count("orders")
