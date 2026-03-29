"""BigQuery warehouse adapter.

Required credentials / env vars:
  GOOGLE_APPLICATION_CREDENTIALS — path to service-account JSON key file
  BIGQUERY_PROJECT               — GCP project ID

Or pass directly:
  credentials={"project": "my-project", "credentials_path": "/path/to/key.json"}
"""

from __future__ import annotations

from datetime import datetime, timezone

from warepact.core.exceptions import WarehouseConnectionError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_warehouse("bigquery")
class BigQueryAdapter(WarehouseAdapter):

    def __init__(self) -> None:
        self._client = None
        self._project: str | None = None

    def connect(self, credentials: dict) -> None:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError as exc:
            raise WarehouseConnectionError(
                "google-cloud-bigquery is not installed. "
                "Install it with: pip install warepact[bigquery]"
            ) from exc
        try:
            self._project = credentials.get("project")
            creds_path = credentials.get("credentials_path")
            if creds_path:
                creds = service_account.Credentials.from_service_account_file(creds_path)
                self._client = bigquery.Client(project=self._project, credentials=creds)
            else:
                self._client = bigquery.Client(project=self._project)
        except Exception as exc:
            raise WarehouseConnectionError(f"BigQuery connection failed: {exc}") from exc

    @property
    def _bq(self):
        if self._client is None:
            raise WarehouseConnectionError("Not connected. Call connect() first.")
        return self._client

    def get_schema(self, table: str) -> list[dict]:
        ref = self._bq.get_table(table)
        return [{"name": f.name, "type": f.field_type} for f in ref.schema]

    def get_row_count(self, table: str) -> int:
        ref = self._bq.get_table(table)
        return int(ref.num_rows or 0)

    def get_last_updated(self, table: str) -> datetime:
        ref = self._bq.get_table(table)
        if ref.modified:
            ts: datetime = ref.modified
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        return datetime.now(tz=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        rows = self._bq.query(sql).result()
        return [dict(row) for row in rows]

    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        if not columns:
            return {}
        parts = [
            f"COUNTIF({col} IS NULL) / COUNT(*) AS {col}"
            for col in columns
        ]
        sql = f"SELECT {', '.join(parts)} FROM `{table}`"
        rows = self.run_query(sql)
        if not rows:
            return {col: 0.0 for col in columns}
        return {col: float(rows[0].get(col) or 0.0) for col in columns}
