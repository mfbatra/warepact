"""Unit tests for S3ContractStore and GCSContractStore.

All cloud SDK calls are mocked — no real credentials needed.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest
import yaml

from warepact.core.contract import Contract
from warepact.core.exceptions import ContractNotFoundError


# ── Helpers ───────────────────────────────────────────────────────────────────

def _minimal_contract(name="orders") -> Contract:
    return Contract(name=name, warehouse="duckdb", table=f"raw.{name}")


def _yaml_for(contract: Contract) -> str:
    data = contract.model_dump(by_alias=True, exclude_none=True)
    return yaml.dump(data, default_flow_style=False, sort_keys=False)


# ── S3ContractStore ───────────────────────────────────────────────────────────

class TestS3ContractStore:
    @pytest.fixture(autouse=True)
    def mock_boto3(self):
        self.boto3_mock = MagicMock()
        self.s3_client = MagicMock()
        self.boto3_mock.client.return_value = self.s3_client

        self.botocore_mock = MagicMock()
        # Make ClientError constructable: botocore.exceptions.ClientError(resp, op)
        class _ClientError(Exception):
            def __init__(self, response, operation_name):
                self.response = response
                self.operation_name = operation_name
        self.botocore_mock.exceptions.ClientError = _ClientError

        with patch.dict(sys.modules, {
            "boto3": self.boto3_mock,
            "botocore": self.botocore_mock,
            "botocore.exceptions": self.botocore_mock.exceptions,
        }):
            yield

    def _make_store(self, prefix="contracts/"):
        from warepact.adapters.stores.s3 import S3ContractStore
        store = S3ContractStore(bucket="my-bucket", prefix=prefix)
        store._client = self.s3_client
        return store

    def test_save_calls_put_object(self):
        store = self._make_store()
        contract = _minimal_contract()
        store.save(contract)
        self.s3_client.put_object.assert_called_once()
        call_kwargs = self.s3_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["Key"] == "contracts/orders.contract.yaml"
        assert b"orders" in call_kwargs["Body"]

    def test_load_returns_contract(self):
        store = self._make_store()
        content = _yaml_for(_minimal_contract("orders"))
        mock_body = MagicMock()
        mock_body.read.return_value = content.encode("utf-8")
        self.s3_client.get_object.return_value = {"Body": mock_body}
        loaded = store.load("orders")
        assert loaded.name == "orders"

    def test_load_raises_not_found_on_missing_key(self):
        store = self._make_store()
        err = self.botocore_mock.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject"
        )
        self.s3_client.get_object.side_effect = err
        with pytest.raises(ContractNotFoundError, match="orders"):
            store.load("orders")

    def test_exists_true_when_head_succeeds(self):
        store = self._make_store()
        self.s3_client.head_object.return_value = {}
        assert store.exists("orders") is True

    def test_exists_false_on_404(self):
        store = self._make_store()
        err = self.botocore_mock.exceptions.ClientError(
            {"Error": {"Code": "404", "Message": ""}}, "HeadObject"
        )
        self.s3_client.head_object.side_effect = err
        assert store.exists("orders") is False

    def test_list_names_returns_sorted(self):
        store = self._make_store()
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [
                {"Key": "contracts/users.contract.yaml"},
                {"Key": "contracts/orders.contract.yaml"},
            ]
        }]
        self.s3_client.get_paginator.return_value = paginator
        names = store.list_names()
        assert names == ["orders", "users"]

    def test_list_names_empty_bucket(self):
        store = self._make_store()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        self.s3_client.get_paginator.return_value = paginator
        assert store.list_names() == []

    def test_delete_removes_object(self):
        store = self._make_store()
        # exists() check — head_object succeeds
        self.s3_client.head_object.return_value = {}
        store.delete("orders")
        self.s3_client.delete_object.assert_called_once_with(
            Bucket="my-bucket", Key="contracts/orders.contract.yaml"
        )

    def test_delete_raises_not_found_when_missing(self):
        store = self._make_store()
        err = self.botocore_mock.exceptions.ClientError(
            {"Error": {"Code": "404", "Message": ""}}, "HeadObject"
        )
        self.s3_client.head_object.side_effect = err
        with pytest.raises(ContractNotFoundError):
            store.delete("orders")

    def test_missing_boto3_raises_import_error(self):
        with patch.dict(sys.modules, {"boto3": None}):
            from warepact.adapters.stores.s3 import S3ContractStore
            store = S3ContractStore(bucket="b")
            store._client = None  # force lazy init
            with pytest.raises(ImportError, match="boto3"):
                store._s3()

    def test_key_uses_correct_prefix(self):
        store = self._make_store(prefix="team/contracts/")
        assert store._key("orders") == "team/contracts/orders.contract.yaml"

    def test_region_passed_to_boto3_client(self):
        from warepact.adapters.stores.s3 import S3ContractStore
        store = S3ContractStore(bucket="b", region="us-west-2")
        store._s3()
        self.boto3_mock.client.assert_called_once_with(
            "s3", region_name="us-west-2"
        )


# ── GCSContractStore ──────────────────────────────────────────────────────────

class TestGCSContractStore:
    @pytest.fixture(autouse=True)
    def mock_gcs(self):
        self.storage_mock = MagicMock()
        self.gcs_client = MagicMock()
        self.storage_mock.Client.return_value = self.gcs_client

        self.gcs_exceptions_mock = MagicMock()

        class _NotFound(Exception):
            pass

        self.gcs_exceptions_mock.NotFound = _NotFound

        with patch.dict(sys.modules, {
            "google": MagicMock(),
            "google.cloud": MagicMock(),
            "google.cloud.storage": self.storage_mock,
            "google.cloud.exceptions": self.gcs_exceptions_mock,
            "google.oauth2": MagicMock(),
            "google.oauth2.service_account": MagicMock(),
        }):
            yield

    def _make_store(self, prefix="contracts/"):
        from warepact.adapters.stores.gcs import GCSContractStore
        store = GCSContractStore(bucket="my-bucket", prefix=prefix)
        store._client = self.gcs_client
        mock_bucket = MagicMock()
        store._bucket = mock_bucket
        self._mock_bucket = mock_bucket
        return store

    def test_save_uploads_blob(self):
        store = self._make_store()
        mock_blob = MagicMock()
        self._mock_bucket.blob.return_value = mock_blob
        contract = _minimal_contract("orders")
        store.save(contract)
        self._mock_bucket.blob.assert_called_once_with("contracts/orders.contract.yaml")
        mock_blob.upload_from_string.assert_called_once()
        uploaded = mock_blob.upload_from_string.call_args.args[0]
        assert "orders" in uploaded

    def test_load_returns_contract(self):
        store = self._make_store()
        mock_blob = MagicMock()
        mock_blob.download_as_text.return_value = _yaml_for(_minimal_contract("orders"))
        self._mock_bucket.blob.return_value = mock_blob
        loaded = store.load("orders")
        assert loaded.name == "orders"

    def test_load_raises_not_found(self):
        store = self._make_store()
        mock_blob = MagicMock()
        mock_blob.download_as_text.side_effect = self.gcs_exceptions_mock.NotFound("404")
        self._mock_bucket.blob.return_value = mock_blob
        with pytest.raises(ContractNotFoundError, match="orders"):
            store.load("orders")

    def test_exists_true_when_blob_exists(self):
        store = self._make_store()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        self._mock_bucket.blob.return_value = mock_blob
        assert store.exists("orders") is True

    def test_exists_false_when_blob_missing(self):
        store = self._make_store()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        self._mock_bucket.blob.return_value = mock_blob
        assert store.exists("orders") is False

    def test_list_names_returns_sorted(self):
        store = self._make_store()
        blob_users = MagicMock()
        blob_users.name = "contracts/users.contract.yaml"
        blob_orders = MagicMock()
        blob_orders.name = "contracts/orders.contract.yaml"
        self.gcs_client.list_blobs.return_value = [blob_users, blob_orders]
        names = store.list_names()
        assert names == ["orders", "users"]

    def test_list_names_empty(self):
        store = self._make_store()
        self.gcs_client.list_blobs.return_value = []
        assert store.list_names() == []

    def test_delete_removes_blob(self):
        store = self._make_store()
        mock_blob = MagicMock()
        self._mock_bucket.blob.return_value = mock_blob
        store.delete("orders")
        mock_blob.delete.assert_called_once()

    def test_delete_raises_not_found(self):
        store = self._make_store()
        mock_blob = MagicMock()
        mock_blob.delete.side_effect = self.gcs_exceptions_mock.NotFound("404")
        self._mock_bucket.blob.return_value = mock_blob
        with pytest.raises(ContractNotFoundError):
            store.delete("orders")

    def test_missing_gcs_raises_import_error(self):
        with patch.dict(sys.modules, {
            "google.cloud.storage": None,
            "google.cloud": None,
            "google": None,
        }):
            from warepact.adapters.stores.gcs import GCSContractStore
            store = GCSContractStore(bucket="b")
            store._client = None
            store._bucket = None
            with pytest.raises(ImportError, match="google-cloud-storage"):
                store._gcs()

    def test_blob_name_uses_correct_prefix(self):
        store = self._make_store(prefix="team/")
        assert store._blob_name("orders") == "team/orders.contract.yaml"
