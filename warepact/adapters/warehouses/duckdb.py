"""DuckDB warehouse adapter.

No external credentials needed — ideal for local development and testing.

Supported credential keys:
  database  — path to a .duckdb file, or ":memory:" (default)
  read_only — bool, default False
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from warepact.core.exceptions import WarehouseConnectionError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.warehouse import WarehouseAdapter

if TYPE_CHECKING:
    import duckdb as _duckdb


@PluginRegistry.register_warehouse("duckdb")
class DuckDBAdapter(WarehouseAdapter):
    """WarehouseAdapter implementation for DuckDB."""

    def __init__(self) -> None:
        self._conn: _duckdb.DuckDBPyConnection | None = None

    # ── Connection ─────────────────────────────────────────────────────────────

    def connect(self, credentials: dict) -> None:
        try:
            import duckdb
        except ImportError as exc:
            raise WarehouseConnectionError(
                "duckdb package is not installed. "
                "Install it with: pip install warepact[duckdb]"
            ) from exc

        database = credentials.get("database", ":memory:")
        read_only = bool(credentials.get("read_only", False))
        try:
            self._conn = duckdb.connect(database=database, read_only=read_only)
        except Exception as exc:
            raise WarehouseConnectionError(
                f"DuckDB connection failed: {exc}"
            ) from exc

    @property
    def _connection(self) -> _duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise WarehouseConnectionError(
                "Not connected. Call connect() first."
            )
        return self._conn

    # ── WarehouseAdapter methods ───────────────────────────────────────────────

    def get_schema(self, table: str) -> list[dict]:
        """Return column definitions using DuckDB's DESCRIBE statement."""
        rows = self._connection.execute(f"DESCRIBE {table}").fetchall()
        # DESCRIBE returns: (column_name, column_type, null, key, default, extra)
        return [{"name": row[0], "type": row[1]} for row in rows]

    def get_row_count(self, table: str) -> int:
        row = self._connection.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()
        return int(row[0]) if row else 0

    def get_last_updated(self, table: str) -> datetime:
        """
        DuckDB doesn't track write timestamps natively.

        For file-backed databases, return the file's mtime.
        For in-memory databases (and when the file can't be read), return now.
        """
        # Try to get the actual database file path from the connection
        try:
            # DuckDB exposes database files via pragma
            result = self._connection.execute(
                "SELECT database_name, path FROM duckdb_databases()"
            ).fetchall()
            for _name, path in result:
                if path and path != "" and path != ":memory:" and os.path.isfile(path):
                    mtime = os.path.getmtime(path)
                    return datetime.fromtimestamp(mtime, tz=timezone.utc)
        except Exception as exc:
            import warnings
            warnings.warn(
                f"Could not determine last_updated for '{table}': {exc}. "
                "Falling back to current time — freshness checks may be unreliable.",
                RuntimeWarning,
                stacklevel=2,
            )
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        """Execute *sql* and return rows as a list of dicts."""
        rel = self._connection.execute(sql)
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]

    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        """Compute the null fraction for each column in one query."""
        if not columns:
            return {}
        parts = [
            f"AVG(CASE WHEN \"{col}\" IS NULL THEN 1.0 ELSE 0.0 END) AS \"{col}\""
            for col in columns
        ]
        sql = f"SELECT {', '.join(parts)} FROM {table}"
        row = self._connection.execute(sql).fetchone()
        if row is None:
            return {col: 0.0 for col in columns}
        return {col: float(val) if val is not None else 0.0
                for col, val in zip(columns, row)}
