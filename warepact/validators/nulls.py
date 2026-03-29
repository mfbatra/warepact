"""NullsValidator — checks column-level null rates against sla.max_null_rate."""

from __future__ import annotations

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class NullsValidator(Validator):
    """
    For every column in the contract schema, checks that the null rate
    does not exceed sla.max_null_rate.

    Skipped when no schema columns are defined or no max_null_rate is set.
    """

    @property
    def name(self) -> str:
        return "nulls"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        sla = contract.sla
        if sla is None or sla.max_null_rate is None:
            return ValidationResult(
                passed=True,
                message="No max_null_rate SLA defined — skipped.",
                severity="info",
            )

        columns = [c.column for c in contract.columns]
        if not columns:
            return ValidationResult(
                passed=True,
                message="No schema columns defined — skipped.",
                severity="info",
            )

        null_rates = adapter.get_null_rates(contract.table, columns)
        threshold = sla.max_null_rate
        failures: list[str] = []
        details: dict = {"threshold": threshold, "columns": {}}

        for col, rate in null_rates.items():
            details["columns"][col] = round(rate, 6)
            if rate > threshold:
                failures.append(
                    f"Column '{col}': null_rate={rate:.4f} exceeds "
                    f"max_null_rate={threshold}."
                )

        if failures:
            return ValidationResult(
                passed=False,
                message=f"Null rate check failed: {len(failures)} column(s) over threshold.",
                details={**details, "failures": failures},
                severity="error",
            )

        return ValidationResult(
            passed=True,
            message=(
                f"Null rates OK: all {len(columns)} column(s) "
                f"within max_null_rate={threshold}."
            ),
            details=details,
            severity="info",
        )
