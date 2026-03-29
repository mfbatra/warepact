"""VolumeValidator — checks that row count is within SLA bounds."""

from __future__ import annotations

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class VolumeValidator(Validator):
    """
    Checks that the table row count satisfies sla.min_rows and sla.max_rows.

    Skipped when neither bound is defined.
    """

    @property
    def name(self) -> str:
        return "volume"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        sla = contract.sla
        if sla is None or (sla.min_rows is None and sla.max_rows is None):
            return ValidationResult(
                passed=True,
                message="No volume SLA defined — skipped.",
                severity="info",
            )

        row_count = adapter.get_row_count(contract.table)
        details = {
            "row_count": row_count,
            "min_rows": sla.min_rows,
            "max_rows": sla.max_rows,
        }
        failures: list[str] = []

        if sla.min_rows is not None and row_count < sla.min_rows:
            failures.append(
                f"Row count {row_count:,} is below minimum {sla.min_rows:,}."
            )

        if sla.max_rows is not None and row_count > sla.max_rows:
            failures.append(
                f"Row count {row_count:,} exceeds maximum {sla.max_rows:,}."
            )

        if failures:
            return ValidationResult(
                passed=False,
                message=f"Volume check failed: {'; '.join(failures)}",
                details=details,
                severity="error",
            )

        return ValidationResult(
            passed=True,
            message=f"Volume OK: {row_count:,} row(s).",
            details=details,
            severity="info",
        )
