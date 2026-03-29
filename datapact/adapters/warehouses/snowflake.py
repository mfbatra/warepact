"""Snowflake warehouse adapter.

Required credential keys:
  account    — Snowflake account identifier (e.g. "xy12345.us-east-1")
  user       — login username
  password   — login password (or use private_key_path + private_key_passphrase)
  warehouse  — virtual warehouse name
  database   — default database
  schema     — default schema (optional, defaults to PUBLIC)
  role       — role to use (optional)

All keys are passed directly to snowflake.connector.connect().
"""

from __future__ import annotations

from datetime import datetime, timezone

from datapact.core.exceptions import WarehouseConnectionError
from datapact.core.registry import PluginRegistry
from datapact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_warehouse("snowflake")
class SnowflakeAdapter(WarehouseAdapter):
    """WarehouseAdapter implementation for Snowflake."""

    def __init__(self) -> None:
        self._conn = None

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self, credentials: dict) -> None:
        try:
            import snowflake.connector
        except ImportError as exc:
            raise WarehouseConnectionError(
                "snowflake-connector-python is not installed. "
                "Install it with: pip install datapact[snowflake]"
            ) from exc
        try:
            self._conn = snowflake.connector.connect(**credentials)
        except Exception as exc:
            raise WarehouseConnectionError(
                f"Snowflake connection failed: {exc}"
            ) from exc

    @property
    def _cursor(self):
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        return self._conn.cursor()

    @property
    def _dict_cursor(self):
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        from snowflake.connector import DictCursor
        return self._conn.cursor(DictCursor)

    # ── WarehouseAdapter methods ───────────────────────────────────────────────

    def get_schema(self, table: str) -> list[dict]:
        cur = self._cursor
        cur.execute(f"DESCRIBE TABLE {table}")
        # DESCRIBE TABLE returns: name, type, kind, null?, default, ...
        return [{"name": row[0], "type": row[1]} for row in cur]

    def get_row_count(self, table: str) -> int:
        cur = self._cursor
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_last_updated(self, table: str) -> datetime:
        """Query INFORMATION_SCHEMA for the table's last-altered timestamp."""
        table_name = table.split(".")[-1].upper()
        cur = self._cursor
        cur.execute(f"""
            SELECT MAX(LAST_ALTERED)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
        """)
        row = cur.fetchone()
        if row and row[0]:
            ts: datetime = row[0]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        cur = self._dict_cursor
        cur.execute(sql)
        return cur.fetchall()

    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        if not columns:
            return {}
        parts = [
            f"AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0.0 END) AS {col}"
            for col in columns
        ]
        cur = self._cursor
        cur.execute(f"SELECT {', '.join(parts)} FROM {table}")
        row = cur.fetchone()
        if row is None:
            return {col: 0.0 for col in columns}
        return {col: float(val) if val is not None else 0.0
                for col, val in zip(columns, row)}
