"""Tests for core/registry.py."""

from __future__ import annotations

import pytest

from datapact.core.exceptions import UnknownAlertChannelError, UnknownWarehouseError
from datapact.core.registry import PluginRegistry
from datapact.interfaces.alerting import AlertChannel
from datapact.interfaces.validator import ValidationResult, Validator
from datapact.interfaces.warehouse import WarehouseAdapter

from datetime import datetime, timezone


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_registry():
    """Ensure each test starts with an empty registry."""
    PluginRegistry._reset()
    yield
    PluginRegistry._reset()


# ── Stub adapters / channels / validators ─────────────────────────────────────

def _make_warehouse_adapter(name: str) -> type:
    class _A(WarehouseAdapter):
        def connect(self, credentials): pass
        def get_schema(self, table): return []
        def get_row_count(self, table): return 0
        def get_last_updated(self, table): return datetime.now(tz=timezone.utc)
        def run_query(self, sql): return []
        def get_null_rates(self, table, columns): return {}
    _A.__name__ = name
    return _A


def _make_alert_channel(channel_name: str) -> type:
    class _C(AlertChannel):
        @property
        def channel_type(self): return channel_name
        def send(self, contract, results, config): return True
    _C.__name__ = channel_name
    return _C


def _make_validator(validator_name: str) -> type:
    class _V(Validator):
        @property
        def name(self): return validator_name
        def validate(self, contract, adapter): return ValidationResult(passed=True, message="ok")
    _V.__name__ = validator_name
    return _V


# ── Warehouse registration ─────────────────────────────────────────────────────

class TestWarehouseRegistration:
    def test_register_and_get(self):
        AdapterClass = _make_warehouse_adapter("DuckAdapter")
        PluginRegistry.register_warehouse("duck")(AdapterClass)
        adapter = PluginRegistry.get_warehouse("duck")
        assert isinstance(adapter, WarehouseAdapter)

    def test_each_call_returns_fresh_instance(self):
        AdapterClass = _make_warehouse_adapter("DuckAdapter")
        PluginRegistry.register_warehouse("duck")(AdapterClass)
        a1 = PluginRegistry.get_warehouse("duck")
        a2 = PluginRegistry.get_warehouse("duck")
        assert a1 is not a2

    def test_unknown_warehouse_raises(self):
        with pytest.raises(UnknownWarehouseError, match="no_such"):
            PluginRegistry.get_warehouse("no_such")

    def test_error_lists_available(self):
        PluginRegistry.register_warehouse("duck")(_make_warehouse_adapter("A"))
        PluginRegistry.register_warehouse("pg")(_make_warehouse_adapter("B"))
        with pytest.raises(UnknownWarehouseError, match="duck"):
            PluginRegistry.get_warehouse("no_such")

    def test_decorator_returns_class_unchanged(self):
        AdapterClass = _make_warehouse_adapter("DuckAdapter")
        result = PluginRegistry.register_warehouse("duck")(AdapterClass)
        assert result is AdapterClass

    def test_list_warehouses(self):
        PluginRegistry.register_warehouse("duck")(_make_warehouse_adapter("A"))
        PluginRegistry.register_warehouse("pg")(_make_warehouse_adapter("B"))
        assert PluginRegistry.list_warehouses() == ["duck", "pg"]

    def test_overwrite_registration(self):
        A = _make_warehouse_adapter("A")
        B = _make_warehouse_adapter("B")
        PluginRegistry.register_warehouse("duck")(A)
        PluginRegistry.register_warehouse("duck")(B)
        adapter = PluginRegistry.get_warehouse("duck")
        assert type(adapter).__name__ == "B"


# ── Alert channel registration ────────────────────────────────────────────────

class TestAlertChannelRegistration:
    def test_register_and_get(self):
        ChannelClass = _make_alert_channel("slack")
        PluginRegistry.register_alert_channel("slack")(ChannelClass)
        ch = PluginRegistry.get_alert_channel("slack")
        assert isinstance(ch, AlertChannel)
        assert ch.channel_type == "slack"

    def test_unknown_channel_raises(self):
        with pytest.raises(UnknownAlertChannelError, match="pagerduty"):
            PluginRegistry.get_alert_channel("pagerduty")

    def test_decorator_returns_class_unchanged(self):
        ChannelClass = _make_alert_channel("slack")
        result = PluginRegistry.register_alert_channel("slack")(ChannelClass)
        assert result is ChannelClass

    def test_list_alert_channels(self):
        PluginRegistry.register_alert_channel("slack")(_make_alert_channel("slack"))
        PluginRegistry.register_alert_channel("email")(_make_alert_channel("email"))
        assert PluginRegistry.list_alert_channels() == ["email", "slack"]


# ── Validator registration ─────────────────────────────────────────────────────

class TestValidatorRegistration:
    def test_register_and_get(self):
        V = _make_validator("schema")
        PluginRegistry.register_validator(V)
        assert V in PluginRegistry.get_validators()

    def test_no_duplicate_registration(self):
        V = _make_validator("schema")
        PluginRegistry.register_validator(V)
        PluginRegistry.register_validator(V)
        assert PluginRegistry.get_validators().count(V) == 1

    def test_get_validators_returns_copy(self):
        V = _make_validator("schema")
        PluginRegistry.register_validator(V)
        lst = PluginRegistry.get_validators()
        lst.clear()
        assert len(PluginRegistry.get_validators()) == 1

    def test_decorator_returns_class_unchanged(self):
        V = _make_validator("schema")
        result = PluginRegistry.register_validator(V)
        assert result is V


# ── Reset helper ───────────────────────────────────────────────────────────────

class TestReset:
    def test_reset_clears_everything(self):
        PluginRegistry.register_warehouse("duck")(_make_warehouse_adapter("A"))
        PluginRegistry.register_alert_channel("slack")(_make_alert_channel("slack"))
        PluginRegistry.register_validator(_make_validator("v"))
        PluginRegistry._reset()
        assert PluginRegistry.list_warehouses() == []
        assert PluginRegistry.list_alert_channels() == []
        assert PluginRegistry.get_validators() == []
