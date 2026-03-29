"""Databricks SQL warehouse adapter.

Uses the official databricks-sql-connector package.

Required credential keys:
  server_hostname — e.g. adb-1234567890.azuredatabricks.net
  http_path       — e.g. /sql/1.0/warehouses/abcdef123456
  access_token    — personal access token or OAuth token

Optional:
  catalog         — Unity Catalog name (default: no catalog switch)
  schema          — default schema (default: no schema switch)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from warepact.core.exceptions import WarehouseConnectionError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.warehouse import WarehouseAdapter

if TYPE_CHECKING:
    pass


@PluginRegistry.register_warehouse("databricks")
class DatabricksAdapter(WarehouseAdapter):
    """
    WarehouseAdapter implementation for Databricks SQL warehouses.

    Connects via databricks-sql-connector (JDBC-free Python driver).
    Supports both Unity Catalog (three-part names) and legacy Hive metastore.
    """

    def __init__(self) -> None:
        self._conn: Any = None

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self, credentials: dict[str, Any]) -> None:
        try:
            from databricks import sql as dbsql
        except ImportError as exc:
            raise WarehouseConnectionError(
                "databricks-sql-connector is not installed. "
                "Install it with: pip install warepact[databricks]"
            ) from exc

        server_hostname = credentials.get("server_hostname")
        http_path = credentials.get("http_path")
        access_token = credentials.get("access_token")

        if not server_hostname or not http_path or not access_token:
            raise WarehouseConnectionError(
                "Databricks credentials require: server_hostname, http_path, access_token."
            )

        try:
            self._conn = dbsql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
            )
        except Exception as exc:
            raise WarehouseConnectionError(
                f"Databricks connection failed: {exc}"
            ) from exc

        catalog = credentials.get("catalog")
        schema = credentials.get("schema")
        if catalog or schema:
            with self._conn.cursor() as cur:
                if catalog:
                    cur.execute(f"USE CATALOG {catalog}")
                if schema:
                    cur.execute(f"USE SCHEMA {schema}")

    def _cursor(self) -> Any:
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        return self._conn.cursor()

    # ── WarehouseAdapter methods ───────────────────────────────────────────────

    def get_schema(self, table: str) -> list[dict[str, Any]]:
        """Return column definitions using DESCRIBE TABLE."""
        with self._cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {table}")
            rows = cur.fetchall()
        # DESCRIBE TABLE returns (col_name, data_type, comment)
        # Skip partition divider rows (col_name starts with '#')
        return [
            {"name": row[0], "type": row[1]}
            for row in rows
            if row[0] and not row[0].startswith("#")
        ]

    def get_row_count(self, table: str) -> int:
        with self._cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_last_updated(self, table: str) -> datetime:
        """
        Return the timestamp of the most recent Delta Lake table operation.

        Falls back to now() for non-Delta tables or when DESCRIBE HISTORY
        is not available.
        """
        try:
            with self._cursor() as cur:
                cur.execute(f"DESCRIBE HISTORY {table} LIMIT 1")
                row = cur.fetchone()
            if row:
                ts = row[0]  # timestamp column is first in DESCRIBE HISTORY
                if isinstance(ts, datetime):
                    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        except Exception:
            pass
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute *sql* and return rows as a list of dicts."""
        with self._cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        """Compute null fraction for each column in a single query."""
        if not columns:
            return {}
        parts = [
            f"AVG(CASE WHEN `{col}` IS NULL THEN 1.0 ELSE 0.0 END) AS `{col}`"
            for col in columns
        ]
        sql = f"SELECT {', '.join(parts)} FROM {table}"
        with self._cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        if row is None:
            return {col: 0.0 for col in columns}
        return {
            col: float(val) if val is not None else 0.0
            for col, val in zip(columns, row)
        }
