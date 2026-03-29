"""Redshift warehouse adapter.

Required env vars:
  REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

from __future__ import annotations

from datetime import datetime, timezone

from warepact.core.exceptions import WarehouseConnectionError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_warehouse("redshift")
class RedshiftAdapter(WarehouseAdapter):

    def __init__(self) -> None:
        self._conn = None

    def connect(self, credentials: dict) -> None:
        try:
            import redshift_connector
        except ImportError as exc:
            raise WarehouseConnectionError(
                "redshift-connector is not installed. "
                "Install it with: pip install warepact[redshift]"
            ) from exc
        try:
            self._conn = redshift_connector.connect(
                host=credentials["host"],
                database=credentials.get("database", "dev"),
                user=credentials["user"],
                password=credentials["password"],
                port=int(credentials.get("port", 5439)),
            )
        except Exception as exc:
            raise WarehouseConnectionError(f"Redshift connection failed: {exc}") from exc

    @property
    def _cursor(self):
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        return self._conn.cursor()

    def get_schema(self, table: str) -> list[dict]:
        _, tbl = (table.split(".", 1) + ["public"])[:2], table.split(".")[-1]
        cur = self._cursor
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (tbl.lower(),),
        )
        return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        cur = self._cursor
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_last_updated(self, table: str) -> datetime:
        tbl = table.split(".")[-1]
        cur = self._cursor
        try:
            cur.execute(
                "SELECT max(endtime) FROM stl_insert WHERE tbl = "
                "(SELECT id FROM stv_tbl_perm WHERE name = %s LIMIT 1)",
                (tbl.lower(),),
            )
            row = cur.fetchone()
            if row and row[0]:
                ts = row[0]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts
        except Exception:
            pass
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        cur = self._cursor
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

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
        return {col: float(v) if v is not None else 0.0 for col, v in zip(columns, row)}
