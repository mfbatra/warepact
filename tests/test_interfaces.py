"""Tests for warepact/interfaces/ — verify every abstract interface is correct."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from warepact.interfaces.warehouse import (
    IFreshnessCheckable,
    INullCheckable,
    IQueryable,
    ISchemaValidatable,
    IVolumeCheckable,
    WarehouseAdapter,
)
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.alerting import AlertChannel
from warepact.interfaces.store import ContractStore


# ── Concrete stubs (minimal implementations used only in this test module) ─────

class _StubWarehouseAdapter(WarehouseAdapter):
    def connect(self, credentials: dict) -> None:
        pass

    def get_schema(self, table: str) -> list[dict]:
        return [{"name": "id", "type": "integer"}]

    def get_row_count(self, table: str) -> int:
        return 42

    def get_last_updated(self, table: str) -> datetime:
        return datetime(2024, 1, 1, tzinfo=timezone.utc)

    def run_query(self, sql: str) -> list[dict]:
        return [{"result": 1}]

    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        return {c: 0.0 for c in columns}


class _StubValidator(Validator):
    @property
    def name(self) -> str:
        return "stub"

    def validate(self, contract, adapter) -> ValidationResult:
        return ValidationResult(passed=True, message="ok")


class _StubAlertChannel(AlertChannel):
    @property
    def channel_type(self) -> str:
        return "stub"

    def send(self, contract, results, config) -> bool:
        return True


class _StubContractStore(ContractStore):
    def __init__(self):
        self._data: dict = {}

    def save(self, contract) -> None:
        self._data[contract.name] = contract

    def load(self, name: str):
        return self._data[name]

    def list_names(self) -> list[str]:
        return list(self._data.keys())

    def delete(self, name: str) -> None:
        del self._data[name]

    def exists(self, name: str) -> bool:
        return name in self._data


# ── WarehouseAdapter tests ─────────────────────────────────────────────────────

class TestWarehouseAdapter:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            WarehouseAdapter()  # type: ignore[abstract]

    def test_stub_is_instantiable(self):
        adapter = _StubWarehouseAdapter()
        assert isinstance(adapter, WarehouseAdapter)

    def test_inherits_all_capability_interfaces(self):
        adapter = _StubWarehouseAdapter()
        assert isinstance(adapter, ISchemaValidatable)
        assert isinstance(adapter, IFreshnessCheckable)
        assert isinstance(adapter, IVolumeCheckable)
        assert isinstance(adapter, INullCheckable)
        assert isinstance(adapter, IQueryable)

    def test_get_schema_returns_list_of_dicts(self):
        adapter = _StubWarehouseAdapter()
        schema = adapter.get_schema("my_table")
        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)

    def test_get_row_count_returns_int(self):
        adapter = _StubWarehouseAdapter()
        assert isinstance(adapter.get_row_count("my_table"), int)

    def test_get_last_updated_returns_datetime(self):
        adapter = _StubWarehouseAdapter()
        assert isinstance(adapter.get_last_updated("my_table"), datetime)

    def test_run_query_returns_list_of_dicts(self):
        adapter = _StubWarehouseAdapter()
        rows = adapter.run_query("SELECT 1")
        assert isinstance(rows, list)

    def test_get_null_rates_returns_dict(self):
        adapter = _StubWarehouseAdapter()
        rates = adapter.get_null_rates("my_table", ["col_a", "col_b"])
        assert isinstance(rates, dict)
        assert set(rates.keys()) == {"col_a", "col_b"}


# ── Capability interface tests (partial implementations allowed) ───────────────

class TestCapabilityInterfaces:
    def test_schema_only_adapter(self):
        class SchemaOnlyAdapter(ISchemaValidatable):
            def get_schema(self, table: str) -> list[dict]:
                return []

        adapter = SchemaOnlyAdapter()
        assert isinstance(adapter, ISchemaValidatable)
        assert not isinstance(adapter, IVolumeCheckable)

    def test_freshness_only_adapter(self):
        class FreshnessOnlyAdapter(IFreshnessCheckable):
            def get_last_updated(self, table: str) -> datetime:
                return datetime.now(tz=timezone.utc)

        adapter = FreshnessOnlyAdapter()
        assert isinstance(adapter, IFreshnessCheckable)
        assert not isinstance(adapter, ISchemaValidatable)


# ── ValidationResult tests ─────────────────────────────────────────────────────

class TestValidationResult:
    def test_defaults(self):
        result = ValidationResult(passed=True, message="all good")
        assert result.details == {}
        assert result.severity == "error"

    def test_explicit_fields(self):
        result = ValidationResult(
            passed=False,
            message="row count too low",
            details={"actual": 5, "expected_min": 1000},
            severity="warning",
        )
        assert not result.passed
        assert result.severity == "warning"
        assert result.details["actual"] == 5


# ── Validator tests ────────────────────────────────────────────────────────────

class TestValidator:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            Validator()  # type: ignore[abstract]

    def test_stub_implements_interface(self):
        v = _StubValidator()
        assert v.name == "stub"

    def test_validate_returns_validation_result(self):
        v = _StubValidator()
        result = v.validate(contract=MagicMock(), adapter=MagicMock())
        assert isinstance(result, ValidationResult)
        assert result.passed is True


# ── AlertChannel tests ─────────────────────────────────────────────────────────

class TestAlertChannel:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            AlertChannel()  # type: ignore[abstract]

    def test_stub_implements_interface(self):
        ch = _StubAlertChannel()
        assert ch.channel_type == "stub"

    def test_send_returns_bool(self):
        ch = _StubAlertChannel()
        ok = ch.send(contract=MagicMock(), results=[], config={})
        assert ok is True


# ── ContractStore tests ────────────────────────────────────────────────────────

class TestContractStore:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            ContractStore()  # type: ignore[abstract]

    def test_save_and_load(self):
        store = _StubContractStore()
        contract = MagicMock()
        contract.name = "orders"
        store.save(contract)
        assert store.load("orders") is contract

    def test_list_names(self):
        store = _StubContractStore()
        c1, c2 = MagicMock(), MagicMock()
        c1.name, c2.name = "orders", "users"
        store.save(c1)
        store.save(c2)
        assert set(store.list_names()) == {"orders", "users"}

    def test_exists(self):
        store = _StubContractStore()
        contract = MagicMock()
        contract.name = "orders"
        assert not store.exists("orders")
        store.save(contract)
        assert store.exists("orders")

    def test_delete(self):
        store = _StubContractStore()
        contract = MagicMock()
        contract.name = "orders"
        store.save(contract)
        store.delete("orders")
        assert not store.exists("orders")
