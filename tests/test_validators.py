"""Unit tests for all built-in validators.

Every validator is tested in isolation with a lightweight mock adapter.
No real warehouse connections are made.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from warepact.core.contract import Contract, SLASpec
from warepact.core.registry import PluginRegistry
from warepact.validators.custom_sql import CustomSQLValidator as _CSV
from warepact.validators.distribution import DistributionValidator as _DV
from warepact.validators.freshness import FreshnessValidator as _FV
from warepact.validators.nulls import NullsValidator as _NV
from warepact.validators.schedule import ScheduleValidator as _ScV
from warepact.validators.schema import SchemaValidator as _SV
from warepact.validators.volume import VolumeValidator as _VV
from warepact.validators.schema import SchemaValidator
from warepact.validators.freshness import FreshnessValidator
from warepact.validators.volume import VolumeValidator
from warepact.validators.nulls import NullsValidator
from warepact.validators.custom_sql import CustomSQLValidator, _extract_scalar
from warepact.validators.distribution import DistributionValidator
from warepact.validators.schedule import ScheduleValidator, _parse_expected_by


# Re-register validators in case test_registry.py's autouse fixture cleared them.
@pytest.fixture(autouse=True, scope="module")
def ensure_validators_registered():
    for cls in (_SV, _FV, _VV, _NV, _CSV, _DV, _ScV):
        PluginRegistry.register_validator(cls)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _contract(**kwargs) -> Contract:
    base = {"name": "orders", "warehouse": "snowflake", "table": "analytics.orders"}
    base.update(kwargs)
    return Contract(**base)


def _adapter(**kwargs) -> MagicMock:
    """Build a mock adapter with sensible defaults."""
    adapter = MagicMock()
    adapter.get_schema.return_value = kwargs.get("schema", [
        {"name": "order_id", "type": "integer"},
        {"name": "status", "type": "string"},
        {"name": "amount", "type": "float"},
    ])
    adapter.get_row_count.return_value = kwargs.get("row_count", 5000)
    adapter.get_last_updated.return_value = kwargs.get(
        "last_updated", datetime.now(tz=timezone.utc) - timedelta(hours=1)
    )
    adapter.get_null_rates.return_value = kwargs.get("null_rates", {})
    adapter.run_query.return_value = kwargs.get("run_query", [])
    return adapter


# ── SchemaValidator ────────────────────────────────────────────────────────────

class TestSchemaValidator:
    def setup_method(self):
        self.v = SchemaValidator()

    def test_name(self):
        assert self.v.name == "schema"

    def test_no_schema_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed
        assert result.severity == "info"

    def test_all_columns_present_and_typed(self):
        contract = _contract(schema=[
            {"column": "order_id", "type": "integer"},
            {"column": "status", "type": "string"},
        ])
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_missing_column_fails(self):
        contract = _contract(schema=[{"column": "nonexistent", "type": "integer"}])
        result = self.v.validate(contract, _adapter())
        assert not result.passed
        assert "nonexistent" in result.message or "nonexistent" in str(result.details)

    def test_type_mismatch_fails(self):
        contract = _contract(schema=[{"column": "order_id", "type": "string"}])
        result = self.v.validate(contract, _adapter())
        assert not result.passed
        assert "type" in str(result.details).lower()

    def test_type_check_is_prefix_match(self):
        # warehouse returns "varchar(255)" — should match spec type "varchar"
        contract = _contract(schema=[{"column": "status", "type": "varchar"}])
        adapter = _adapter(schema=[{"name": "status", "type": "varchar(255)"}])
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_not_null_violation_fails(self):
        contract = _contract(schema=[{"column": "order_id", "type": "integer", "not_null": True}])
        adapter = _adapter()
        adapter.get_null_rates.return_value = {"order_id": 0.05}
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert any("null" in f.lower() for f in result.details["failures"])

    def test_not_null_passes_when_zero_nulls(self):
        contract = _contract(schema=[{"column": "order_id", "type": "integer", "not_null": True}])
        adapter = _adapter()
        adapter.get_null_rates.return_value = {"order_id": 0.0}
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_unique_violation_fails(self):
        contract = _contract(schema=[{"column": "order_id", "type": "integer", "unique": True}])
        adapter = _adapter()
        adapter.run_query.return_value = [{"duplicates": 3}]
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert any("duplicate" in f.lower() for f in result.details["failures"])

    def test_unique_passes_when_no_duplicates(self):
        contract = _contract(schema=[{"column": "order_id", "type": "integer", "unique": True}])
        adapter = _adapter()
        adapter.run_query.return_value = [{"duplicates": 0}]
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_allowed_values_violation_fails(self):
        contract = _contract(schema=[{
            "column": "status", "type": "string",
            "allowed_values": ["pending", "shipped"],
        }])
        adapter = _adapter()
        adapter.run_query.return_value = [{"invalid_count": 7}]
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert any("allowed_values" in f for f in result.details["failures"])

    def test_allowed_values_passes(self):
        contract = _contract(schema=[{
            "column": "status", "type": "string",
            "allowed_values": ["pending", "shipped"],
        }])
        adapter = _adapter()
        adapter.run_query.return_value = [{"invalid_count": 0}]
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_column_name_lookup_is_case_insensitive(self):
        contract = _contract(schema=[{"column": "Order_ID", "type": "integer"}])
        adapter = _adapter(schema=[{"name": "order_id", "type": "integer"}])
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_multiple_failures_all_reported(self):
        contract = _contract(schema=[
            {"column": "missing_col", "type": "integer"},
            {"column": "order_id", "type": "string"},  # type mismatch
        ])
        result = self.v.validate(contract, _adapter())
        assert not result.passed
        assert len(result.details["failures"]) == 2


# ── FreshnessValidator ─────────────────────────────────────────────────────────

class TestFreshnessValidator:
    def setup_method(self):
        self.v = FreshnessValidator()

    def test_name(self):
        assert self.v.name == "freshness"

    def test_no_sla_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed
        assert result.severity == "info"

    def test_no_freshness_hours_skips(self):
        contract = _contract(sla={"min_rows": 1000})
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_within_sla_passes(self):
        contract = _contract(sla={"freshness_hours": 6})
        adapter = _adapter(last_updated=datetime.now(tz=timezone.utc) - timedelta(hours=3))
        result = self.v.validate(contract, adapter)
        assert result.passed
        assert "OK" in result.message

    def test_stale_table_fails(self):
        contract = _contract(sla={"freshness_hours": 6})
        adapter = _adapter(last_updated=datetime.now(tz=timezone.utc) - timedelta(hours=10))
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "stale" in result.message.lower()

    def test_exactly_at_boundary_passes(self):
        contract = _contract(sla={"freshness_hours": 6})
        # 6h - 1s ago → should pass
        adapter = _adapter(
            last_updated=datetime.now(tz=timezone.utc) - timedelta(hours=6) + timedelta(seconds=1)
        )
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_naive_datetime_treated_as_utc(self):
        contract = _contract(sla={"freshness_hours": 6})
        adapter = _adapter(
            last_updated=datetime.utcnow() - timedelta(hours=2)  # naive
        )
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_details_contain_age_and_threshold(self):
        contract = _contract(sla={"freshness_hours": 6})
        adapter = _adapter(last_updated=datetime.now(tz=timezone.utc) - timedelta(hours=3))
        result = self.v.validate(contract, adapter)
        assert "age_hours" in result.details
        assert result.details["max_hours"] == 6


# ── VolumeValidator ────────────────────────────────────────────────────────────

class TestVolumeValidator:
    def setup_method(self):
        self.v = VolumeValidator()

    def test_name(self):
        assert self.v.name == "volume"

    def test_no_sla_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed

    def test_no_volume_bounds_skips(self):
        contract = _contract(sla={"freshness_hours": 6})
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_above_minimum_passes(self):
        contract = _contract(sla={"min_rows": 1000})
        adapter = _adapter(row_count=5000)
        result = self.v.validate(contract, adapter)
        assert result.passed
        assert "5,000" in result.message

    def test_below_minimum_fails(self):
        contract = _contract(sla={"min_rows": 1000})
        adapter = _adapter(row_count=500)
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "below minimum" in result.message

    def test_above_maximum_fails(self):
        contract = _contract(sla={"max_rows": 1000})
        adapter = _adapter(row_count=2000)
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "exceeds maximum" in result.message

    def test_within_both_bounds_passes(self):
        contract = _contract(sla={"min_rows": 100, "max_rows": 10000})
        adapter = _adapter(row_count=5000)
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_violates_both_bounds_reports_both(self):
        # Logically impossible to violate both, but test min > max edge case
        contract = _contract(sla={"min_rows": 9000, "max_rows": 100})
        adapter = _adapter(row_count=500)
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "below minimum" in result.message

    def test_details_contain_counts(self):
        contract = _contract(sla={"min_rows": 1000})
        adapter = _adapter(row_count=5000)
        result = self.v.validate(contract, adapter)
        assert result.details["row_count"] == 5000
        assert result.details["min_rows"] == 1000


# ── NullsValidator ─────────────────────────────────────────────────────────────

class TestNullsValidator:
    def setup_method(self):
        self.v = NullsValidator()

    def test_name(self):
        assert self.v.name == "nulls"

    def test_no_sla_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed

    def test_no_max_null_rate_skips(self):
        contract = _contract(sla={"min_rows": 1000})
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_no_columns_skips(self):
        contract = _contract(sla={"max_null_rate": 0.01})
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_all_under_threshold_passes(self):
        contract = _contract(
            sla={"max_null_rate": 0.05},
            schema=[
                {"column": "order_id", "type": "integer"},
                {"column": "status", "type": "string"},
            ],
        )
        adapter = _adapter(null_rates={"order_id": 0.01, "status": 0.02})
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_over_threshold_fails(self):
        contract = _contract(
            sla={"max_null_rate": 0.05},
            schema=[{"column": "status", "type": "string"}],
        )
        adapter = _adapter(null_rates={"status": 0.10})
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "status" in str(result.details)

    def test_multiple_violations_all_reported(self):
        contract = _contract(
            sla={"max_null_rate": 0.01},
            schema=[
                {"column": "col_a", "type": "string"},
                {"column": "col_b", "type": "string"},
            ],
        )
        adapter = _adapter(null_rates={"col_a": 0.05, "col_b": 0.10})
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert len(result.details["failures"]) == 2

    def test_adapter_called_with_correct_columns(self):
        contract = _contract(
            sla={"max_null_rate": 0.01},
            schema=[
                {"column": "order_id", "type": "integer"},
                {"column": "status", "type": "string"},
            ],
        )
        adapter = _adapter(null_rates={"order_id": 0.0, "status": 0.0})
        self.v.validate(contract, adapter)
        adapter.get_null_rates.assert_called_once_with(
            "analytics.orders", ["order_id", "status"]
        )


# ── CustomSQLValidator ─────────────────────────────────────────────────────────

class TestCustomSQLValidator:
    def setup_method(self):
        self.v = CustomSQLValidator()

    def test_name(self):
        assert self.v.name == "custom_sql"

    def test_no_checks_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed

    def test_check_passes_when_result_matches_expected(self):
        contract = _contract(
            custom_checks=[{"name": "no_negatives", "sql": "SELECT COUNT(*) FROM {table} WHERE amount < 0", "expected": 0}]
        )
        adapter = _adapter(run_query=[{"count": 0}])
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_check_fails_when_result_differs(self):
        contract = _contract(
            custom_checks=[{"name": "no_negatives", "sql": "SELECT COUNT(*) FROM {table} WHERE amount < 0", "expected": 0}]
        )
        adapter = _adapter(run_query=[{"count": 5}])
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "no_negatives" in result.message or "no_negatives" in str(result.details)

    def test_table_placeholder_substituted(self):
        contract = _contract(
            custom_checks=[{"name": "test", "sql": "SELECT 1 FROM {table}", "expected": 1}]
        )
        adapter = _adapter(run_query=[{"1": 1}])
        self.v.validate(contract, adapter)
        call_sql = adapter.run_query.call_args[0][0]
        assert "analytics.orders" in call_sql
        assert "{table}" not in call_sql

    def test_adapter_error_reported_as_failure(self):
        contract = _contract(
            custom_checks=[{"name": "boom", "sql": "SELECT bad_sql", "expected": 0}]
        )
        adapter = _adapter()
        adapter.run_query.side_effect = RuntimeError("syntax error")
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert any("errored" in f.lower() for f in result.details["failures"])

    def test_multiple_checks_all_run(self):
        contract = _contract(
            custom_checks=[
                {"name": "check_a", "sql": "SELECT 1 FROM {table}", "expected": 1},
                {"name": "check_b", "sql": "SELECT 2 FROM {table}", "expected": 2},
            ]
        )
        adapter = MagicMock()
        adapter.run_query.side_effect = [[{"v": 1}], [{"v": 99}]]  # check_b fails
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert len(result.details["failures"]) == 1
        assert "check_b" in result.details["failures"][0]


class TestExtractScalar:
    def test_returns_first_value_of_first_row(self):
        assert _extract_scalar([{"count": 42}]) == 42

    def test_empty_rows_returns_none(self):
        assert _extract_scalar([]) is None

    def test_empty_first_row_returns_none(self):
        assert _extract_scalar([{}]) is None


# ── PluginRegistry integration ─────────────────────────────────────────────────

class TestSchemaValidatorMinMax:
    """Tests for ColumnSpec min/max enforcement in SchemaValidator."""

    def setup_method(self):
        self.v = SchemaValidator()

    def _contract_with_col(self, **col_kwargs) -> Contract:
        col = {"column": "amount", "type": "float", **col_kwargs}
        return _contract(schema=[col])

    def test_min_passes_when_actual_above_minimum(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        adapter.run_query.return_value = [{"min_val": 5.0}]
        result = self.v.validate(self._contract_with_col(min=1.0), adapter)
        assert result.passed

    def test_min_fails_when_actual_below_minimum(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        adapter.run_query.return_value = [{"min_val": -3.0}]
        result = self.v.validate(self._contract_with_col(min=0.0), adapter)
        assert not result.passed
        assert any("min" in f.lower() for f in result.details["failures"])

    def test_max_passes_when_actual_below_maximum(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        adapter.run_query.return_value = [{"max_val": 50.0}]
        result = self.v.validate(self._contract_with_col(max=100.0), adapter)
        assert result.passed

    def test_max_fails_when_actual_above_maximum(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        adapter.run_query.return_value = [{"max_val": 999.0}]
        result = self.v.validate(self._contract_with_col(max=500.0), adapter)
        assert not result.passed
        assert any("max" in f.lower() for f in result.details["failures"])

    def test_min_and_max_both_violated_reported(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        # Simulate run_query called twice: first MIN, then MAX
        adapter.run_query.side_effect = [
            [{"min_val": -1.0}],  # violates min=0
            [{"max_val": 1000.0}],  # violates max=500
        ]
        result = self.v.validate(self._contract_with_col(min=0.0, max=500.0), adapter)
        assert not result.passed
        assert len(result.details["failures"]) == 2

    def test_no_min_max_no_queries(self):
        adapter = _adapter(schema=[{"name": "amount", "type": "float"}])
        self.v.validate(self._contract_with_col(), adapter)
        # run_query should not be called for min/max when neither is set
        # (it may be called for allowed_values or unique, but not min/max)
        for call_args in adapter.run_query.call_args_list:
            assert "min_val" not in str(call_args)
            assert "max_val" not in str(call_args)


# ── ScheduleValidator ──────────────────────────────────────────────────────────

class TestScheduleValidator:
    def setup_method(self):
        self.v = ScheduleValidator()

    def test_name(self):
        assert self.v.name == "schedule"

    def test_no_schedule_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed
        assert result.severity == "info"

    def test_no_expected_by_skips(self):
        contract = _contract(schedule={"frequency": "daily"})
        result = self.v.validate(contract, _adapter())
        assert result.passed
        assert result.severity == "info"

    def test_passes_when_updated_after_deadline(self):
        # Deadline was 1 minute ago, table updated 30 seconds ago → pass
        now = datetime.now(tz=timezone.utc)
        if now.hour == 0 and now.minute == 0:
            pytest.skip("Near midnight — arithmetic would wrap to prior day")
        # Deadline = 1 minute ago
        deadline = now - timedelta(minutes=1)
        expected_by = f"{deadline.hour:02d}:{deadline.minute:02d} UTC"
        updated_at = now - timedelta(seconds=30)  # updated after the deadline

        contract = _contract(schedule={"expected_by": expected_by})
        adapter = _adapter(last_updated=updated_at)
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_fails_when_not_updated_before_deadline(self):
        # Deadline was 1 minute ago, last updated was 2 minutes ago → fail
        now = datetime.now(tz=timezone.utc)
        if now.hour == 0 and now.minute < 2:
            pytest.skip("Near midnight — arithmetic would wrap to prior day")
        deadline = now - timedelta(minutes=1)
        expected_by = f"{deadline.hour:02d}:{deadline.minute:02d} UTC"
        updated_at = now - timedelta(minutes=2)  # updated BEFORE the deadline

        contract = _contract(schedule={"expected_by": expected_by})
        adapter = _adapter(last_updated=updated_at)
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "breach" in result.message.lower()
        assert "lag_minutes" in result.details

    def test_skips_when_before_deadline(self):
        # Use a time 1 minute from now (won't wrap across midnight unless it's 23:59)
        now = datetime.now(tz=timezone.utc)
        if now.hour == 23 and now.minute >= 58:
            pytest.skip("Near midnight — deadline arithmetic would wrap")
        future_minute = now.minute + 1
        expected_by = f"{now.hour:02d}:{future_minute:02d} UTC"

        contract = _contract(schedule={"expected_by": expected_by})
        result = self.v.validate(contract, _adapter())
        assert result.passed
        assert "not yet reached" in result.message

    def test_invalid_expected_by_returns_error_result(self):
        contract = _contract(schedule={"expected_by": "not-a-time"})
        result = self.v.validate(contract, _adapter())
        assert not result.passed
        assert "cannot parse" in result.message.lower()


class TestParseExpectedBy:
    def test_parses_hhmm_utc(self):
        dt = _parse_expected_by("06:00 UTC")
        assert dt.hour == 6
        assert dt.minute == 0
        assert dt.tzinfo is not None

    def test_parses_without_tz_suffix(self):
        dt = _parse_expected_by("14:30")
        assert dt.hour == 14
        assert dt.minute == 30

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            _parse_expected_by("not-a-time")

    def test_out_of_range_hour_raises(self):
        with pytest.raises(ValueError):
            _parse_expected_by("25:00 UTC")


# ── DistributionValidator ──────────────────────────────────────────────────────

class TestDistributionValidator:
    """
    Tests for DistributionValidator shift detection logic.

    max_distribution_shift is an extra SLA field not in the core model.
    We use a Pydantic subclass with extra="allow" to inject it cleanly.
    """

    def setup_method(self):
        self.v = DistributionValidator()

    @staticmethod
    def _contract_with_shift(columns, max_shift: float) -> Contract:
        """Build a contract whose SLA has max_distribution_shift set."""
        from pydantic import ConfigDict

        class _FlexSLA(SLASpec):
            model_config = ConfigDict(extra="allow")

        sla = _FlexSLA.model_validate({"max_distribution_shift": max_shift})
        c = Contract(name="orders", warehouse="w", table="tbl", schema=columns)
        object.__setattr__(c, "sla", sla)
        return c

    def test_name(self):
        assert self.v.name == "distribution"

    def test_no_sla_skips(self):
        result = self.v.validate(_contract(), _adapter())
        assert result.passed
        assert result.severity == "info"

    def test_no_columns_with_allowed_values_skips(self):
        contract = _contract(schema=[{"column": "id", "type": "integer"}])
        result = self.v.validate(contract, _adapter())
        assert result.passed

    def test_passes_when_distribution_within_tolerance(self):
        contract = self._contract_with_shift(
            columns=[{"column": "status", "type": "string",
                      "allowed_values": ["a", "b", "c", "d"]}],
            max_shift=0.2,
        )
        adapter = _adapter()
        # Near-uniform distribution (25 each, baseline 0.25)
        adapter.run_query.return_value = [
            {"status": "a", "cnt": 25},
            {"status": "b", "cnt": 26},
            {"status": "c", "cnt": 24},
            {"status": "d", "cnt": 25},
        ]
        result = self.v.validate(contract, adapter)
        assert result.passed

    def test_fails_when_distribution_exceeds_tolerance(self):
        contract = self._contract_with_shift(
            columns=[{"column": "status", "type": "string",
                      "allowed_values": ["a", "b"]}],
            max_shift=0.1,
        )
        adapter = _adapter()
        # Heavily skewed: 90% "a", 10% "b" → shift from 0.5 baseline = 0.4
        adapter.run_query.return_value = [
            {"status": "a", "cnt": 90},
            {"status": "b", "cnt": 10},
        ]
        result = self.v.validate(contract, adapter)
        assert not result.passed
        assert "shift" in result.message.lower()

    def test_skips_column_with_no_rows_matching(self):
        contract = self._contract_with_shift(
            columns=[{"column": "status", "type": "string",
                      "allowed_values": ["a", "b"]}],
            max_shift=0.1,
        )
        adapter = _adapter()
        adapter.run_query.return_value = []  # no matching rows
        result = self.v.validate(contract, adapter)
        assert result.passed  # skipped, not a failure

    def test_details_contain_distribution_info(self):
        contract = self._contract_with_shift(
            columns=[{"column": "status", "type": "string",
                      "allowed_values": ["x", "y"]}],
            max_shift=0.5,
        )
        adapter = _adapter()
        adapter.run_query.return_value = [
            {"status": "x", "cnt": 60},
            {"status": "y", "cnt": 40},
        ]
        result = self.v.validate(contract, adapter)
        assert result.passed
        assert "status" in result.details.get("columns", {})


# ── PluginRegistry integration ─────────────────────────────────────────────────

class TestValidatorRegistration:
    def test_all_validators_registered(self):
        # Import validators package to trigger registration
        import warepact.validators  # noqa: F401
        registered_names = {v().name for v in PluginRegistry.get_validators()}
        assert {"schema", "freshness", "volume", "nulls", "custom_sql",
                "distribution", "schedule"}.issubset(registered_names)
