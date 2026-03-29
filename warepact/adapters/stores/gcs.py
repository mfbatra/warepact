"""GCS-backed ContractStore.

Contracts are stored as .contract.yaml blobs under a common prefix in a GCS
bucket. Requires the google-cloud-storage package.

Required credentials / env vars:
  GOOGLE_APPLICATION_CREDENTIALS — path to service-account JSON key file
  (or any standard ADC credential chain — Workload Identity, gcloud auth, etc.)

Usage::

    store = GCSContractStore(bucket="my-warepact-bucket", prefix="contracts/")
    store.save(contract)
    loaded = store.load("orders")
"""

from __future__ import annotations

import yaml

from warepact.core.contract import Contract
from warepact.core.exceptions import ContractNotFoundError
from warepact.interfaces.store import ContractStore
from warepact.parsers.yaml_parser import YAMLParser


class GCSContractStore(ContractStore):
    """
    Persists Contract objects as GCS blobs.

    Blob names follow the pattern::

        <prefix><name>.contract.yaml

    e.g. ``contracts/orders.contract.yaml`` with ``prefix="contracts/"``.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "contracts/",
        project: str | None = None,
        credentials_path: str | None = None,
    ) -> None:
        self._bucket_name = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._project = project
        self._credentials_path = credentials_path
        self._client = None
        self._bucket = None
        self._parser = YAMLParser()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _gcs(self):
        if self._client is None:
            try:
                from google.cloud import storage
            except ImportError as exc:
                raise ImportError(
                    "google-cloud-storage is not installed. "
                    "Install it with: pip install warepact[gcs]"
                ) from exc
            if self._credentials_path:
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(
                    self._credentials_path
                )
                self._client = storage.Client(project=self._project, credentials=creds)
            else:
                self._client = storage.Client(project=self._project)
        return self._client

    def _get_bucket(self):
        if self._bucket is None:
            self._bucket = self._gcs().bucket(self._bucket_name)
        return self._bucket

    def _blob_name(self, name: str) -> str:
        return f"{self._prefix}{name}.contract.yaml"

    def _serialize(self, contract: Contract) -> str:
        data = contract.model_dump(by_alias=True, exclude_none=True)
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    # ── ContractStore interface ────────────────────────────────────────────────

    def save(self, contract: Contract) -> None:
        """Upload *contract* to GCS as a YAML blob."""
        blob = self._get_bucket().blob(self._blob_name(contract.name))
        blob.upload_from_string(
            self._serialize(contract),
            content_type="application/x-yaml",
        )

    def load(self, name: str) -> Contract:
        """Download and parse the contract identified by *name*."""
        from google.cloud.exceptions import NotFound

        blob = self._get_bucket().blob(self._blob_name(name))
        try:
            content = blob.download_as_text(encoding="utf-8")
        except NotFound:
            raise ContractNotFoundError(
                f"Contract '{name}' not found in gs://{self._bucket_name}/{self._prefix}."
            )
        return self._parser.parse_string(content)

    def list_names(self) -> list[str]:
        """List all contract names stored under the configured prefix."""
        blobs = self._gcs().list_blobs(self._bucket_name, prefix=self._prefix)
        names = []
        for blob in blobs:
            if blob.name.endswith(".contract.yaml"):
                name = blob.name[len(self._prefix):].replace(".contract.yaml", "")
                names.append(name)
        return sorted(names)

    def delete(self, name: str) -> None:
        """Delete the GCS blob for *name*."""
        from google.cloud.exceptions import NotFound

        blob = self._get_bucket().blob(self._blob_name(name))
        try:
            blob.delete()
        except NotFound:
            raise ContractNotFoundError(
                f"Contract '{name}' not found in gs://{self._bucket_name}/{self._prefix}."
            )

    def exists(self, name: str) -> bool:
        """Return True if the GCS blob exists."""
        blob = self._get_bucket().blob(self._blob_name(name))
        return blob.exists()
