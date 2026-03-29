"""S3-backed ContractStore.

Contracts are stored as .contract.yaml objects under a common prefix in an S3
bucket. No local filesystem writes — fully serverless.

Required credentials / env vars:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  (or any standard boto3 credential chain — instance profile, SSO, etc.)

Usage::

    store = S3ContractStore(bucket="my-datapact-bucket", prefix="contracts/")
    store.save(contract)
    loaded = store.load("orders")
"""

from __future__ import annotations

from typing import Any

import yaml

from datapact.core.contract import Contract
from datapact.core.exceptions import ContractNotFoundError
from datapact.interfaces.store import ContractStore
from datapact.parsers.yaml_parser import YAMLParser


class S3ContractStore(ContractStore):
    """
    Persists Contract objects as S3 objects.

    Object keys follow the pattern::

        <prefix><name>.contract.yaml

    e.g. ``contracts/orders.contract.yaml`` with ``prefix="contracts/"``.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "contracts/",
        region: str | None = None,
        **boto3_kwargs: Any,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._region = region
        self._boto3_kwargs = boto3_kwargs
        self._client = None
        self._parser = YAMLParser()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _s3(self):
        if self._client is None:
            try:
                import boto3
            except ImportError as exc:
                raise ImportError(
                    "boto3 is not installed. "
                    "Install it with: pip install datapact[s3]"
                ) from exc
            kwargs = dict(self._boto3_kwargs)
            if self._region:
                kwargs["region_name"] = self._region
            self._client = boto3.client("s3", **kwargs)
        return self._client

    def _key(self, name: str) -> str:
        return f"{self._prefix}{name}.contract.yaml"

    def _serialize(self, contract: Contract) -> bytes:
        data = contract.model_dump(by_alias=True, exclude_none=True)
        return yaml.dump(data, default_flow_style=False, sort_keys=False).encode("utf-8")

    # ── ContractStore interface ────────────────────────────────────────────────

    def save(self, contract: Contract) -> None:
        """Upload *contract* to S3 as a YAML object."""
        self._s3().put_object(
            Bucket=self._bucket,
            Key=self._key(contract.name),
            Body=self._serialize(contract),
            ContentType="application/x-yaml",
        )

    def load(self, name: str) -> Contract:
        """Download and parse the contract identified by *name*."""
        import botocore.exceptions

        try:
            resp = self._s3().get_object(Bucket=self._bucket, Key=self._key(name))
        except botocore.exceptions.ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("NoSuchKey", "404"):
                raise ContractNotFoundError(
                    f"Contract '{name}' not found in s3://{self._bucket}/{self._prefix}."
                ) from exc
            raise
        content = resp["Body"].read().decode("utf-8")
        return self._parser.parse_string(content)

    def list_names(self) -> list[str]:
        """List all contract names stored under the configured prefix."""
        paginator = self._s3().get_paginator("list_objects_v2")
        names = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                if key.endswith(".contract.yaml"):
                    name = key[len(self._prefix):].replace(".contract.yaml", "")
                    names.append(name)
        return sorted(names)

    def delete(self, name: str) -> None:
        """Delete the S3 object for *name*."""
        if not self.exists(name):
            raise ContractNotFoundError(
                f"Contract '{name}' not found in s3://{self._bucket}/{self._prefix}."
            )
        self._s3().delete_object(Bucket=self._bucket, Key=self._key(name))

    def exists(self, name: str) -> bool:
        """Return True if the S3 object exists."""
        import botocore.exceptions

        try:
            self._s3().head_object(Bucket=self._bucket, Key=self._key(name))
            return True
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
