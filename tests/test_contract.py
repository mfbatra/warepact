"""Tests for core/contract.py."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from warepact.core.contract import Contract


MINIMAL = {
    "name": "orders",
    "warehouse": "snowflake",
    "table": "analytics.core.orders",
}


class TestContractParsing:
    def test_minimal_contract(self):
        c = Contract(**MINIMAL)
        assert c.name == "orders"
        assert c.warehouse == "snowflake"
        assert c.version == 1
        assert c.pii is False

    def test_full_contract(self):
        c = Contract(
            name="orders",
            warehouse="snowflake",
            table="analytics.core.orders",
            description="Core orders table",
            owner="data-team@company.com",
            version=1,
            pii=False,
            tags=["revenue", "critical"],
            schema=[
                {"column": "order_id", "type": "integer", "not_null": True, "unique": True},
                {"column": "status", "type": "string", "allowed_values": ["pending", "shipped"]},
            ],
            sla={"freshness_hours": 6, "min_rows": 1000, "max_null_rate": 0.01},
        )
        assert len(c.columns) == 2
        assert c.sla.freshness_hours == 6
        assert c.sla.min_rows == 1000

    def test_schema_alias(self):
        """The YAML key is 'schema'; internally accessed as schema_ or .columns."""
        c = Contract(
            **MINIMAL,
            schema=[{"column": "id", "type": "integer"}],
        )
        assert len(c.columns) == 1
        assert c.columns[0].column == "id"


class TestContractValidation:
    def test_invalid_name_rejects(self):
        with pytest.raises(ValidationError, match="name"):
            Contract(name="My Orders!", warehouse="snowflake", table="t")

    def test_name_with_hyphens_and_underscores_ok(self):
        c = Contract(name="my-orders_v2", warehouse="snowflake", table="t")
        assert c.name == "my-orders_v2"

    def test_version_must_be_positive(self):
        with pytest.raises(ValidationError, match="version"):
            Contract(**MINIMAL, version=0)

    def test_max_null_rate_must_be_fraction(self):
        with pytest.raises(ValidationError):
            Contract(**MINIMAL, sla={"max_null_rate": 1.5})

    def test_invalid_owner_email_rejects(self):
        with pytest.raises(ValidationError):
            Contract(**MINIMAL, owner="not-an-email")

    def test_valid_owner_email(self):
        c = Contract(**MINIMAL, owner="team@company.com")
        assert str(c.owner) == "team@company.com"


class TestAlertChannels:
    def test_alert_channels_property(self):
        c = Contract(
            **MINIMAL,
            alerts=[
                {"channel": "slack", "on": ["breach"]},
                {"channel": "email", "on": ["breach", "recovery"]},
                {"channel": "slack", "on": ["recovery"]},  # duplicate
            ],
        )
        # Deduped and ordered by first appearance
        assert c.alert_channels == ["slack", "email"]

    def test_env_var_expansion(self, monkeypatch):
        monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.example.com/abc")
        c = Contract(
            **MINIMAL,
            alerts=[{"channel": "slack", "webhook_url": "${SLACK_WEBHOOK}"}],
        )
        assert c.alerts[0].model_extra["webhook_url"] == "https://hooks.example.com/abc"

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(ValueError, match="MISSING_VAR"):
            Contract(
                **MINIMAL,
                alerts=[{"channel": "slack", "webhook_url": "${MISSING_VAR}"}],
            )


class TestContractDefaults:
    def test_empty_collections(self):
        c = Contract(**MINIMAL)
        assert c.consumers == []
        assert c.custom_checks == []
        assert c.alerts == []
        assert c.tags == []
        assert c.columns == []

    def test_credentials_excluded_from_serialisation(self):
        c = Contract(**MINIMAL, credentials={"password": "secret"})
        dumped = c.model_dump()
        assert "credentials" not in dumped
