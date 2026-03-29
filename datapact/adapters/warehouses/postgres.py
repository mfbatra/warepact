"""PostgreSQL warehouse adapter.

Required env vars (standard libpq):
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

from __future__ import annotations

from datetime import datetime, timezone

from datapact.core.exceptions import WarehouseConnectionError
from datapact.core.registry import PluginRegistry
from datapact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_warehouse("postgres")
class PostgresAdapter(WarehouseAdapter):

    def __init__(self) -> None:
        self._conn = None

    def connect(self, credentials: dict) -> None:
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError as exc:
            raise WarehouseConnectionError(
                "psycopg2-binary is not installed. "
                "Install it with: pip install datapact[postgres]"
            ) from exc
        try:
            self._conn = psycopg2.connect(
                host=credentials.get("host", "localhost"),
                port=int(credentials.get("port", 5432)),
                dbname=credentials.get("database", "postgres"),
                user=credentials.get("user"),
                password=credentials.get("password"),
            )
        except Exception as exc:
            raise WarehouseConnectionError(f"Postgres connection failed: {exc}") from exc

    @property
    def _cursor(self):
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        return self._conn.cursor()

    @property
    def _dict_cursor(self):
        if self._conn is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        import psycopg2.extras
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_schema(self, table: str) -> list[dict]:
        parts = table.split(".")
        tbl = parts[-1]
        schema = parts[-2] if len(parts) > 1 else "public"
        cur = self._cursor
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema, tbl),
        )
        return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        cur = self._cursor
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_last_updated(self, table: str) -> datetime:
        # Postgres doesn't track DML timestamps natively; use pg_stat_user_tables
        tbl = table.split(".")[-1]
        cur = self._cursor
        try:
            cur.execute(
                "SELECT greatest(last_vacuum, last_autovacuum, last_analyze, "
                "last_autoanalyze) FROM pg_stat_user_tables WHERE relname = %s",
                (tbl,),
            )
            row = cur.fetchone()
            if row and row[0]:
                ts: datetime = row[0]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts
        except Exception:
            pass
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        cur = self._dict_cursor
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]

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
