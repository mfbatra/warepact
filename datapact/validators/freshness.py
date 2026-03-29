"""FreshnessValidator — checks that the table was updated within the SLA window."""

from __future__ import annotations

from datetime import datetime, timezone

from datapact.core.contract import Contract
from datapact.core.registry import PluginRegistry
from datapact.interfaces.validator import ValidationResult, Validator
from datapact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class FreshnessValidator(Validator):
    """
    Checks that the table's last-updated timestamp is within the
    contract's sla.freshness_hours window.

    Skipped (passes with severity=info) when no freshness SLA is defined.
    """

    @property
    def name(self) -> str:
        return "freshness"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        if contract.sla is None or contract.sla.freshness_hours is None:
            return ValidationResult(
                passed=True,
                message="No freshness SLA defined — skipped.",
                severity="info",
            )

        max_hours = contract.sla.freshness_hours
        last_updated = adapter.get_last_updated(contract.table)

        # Normalise to UTC-aware for safe comparison
        now = datetime.now(tz=timezone.utc)
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)

        age_hours = (now - last_updated).total_seconds() / 3600
        details = {
            "last_updated": last_updated.isoformat(),
            "age_hours": round(age_hours, 4),
            "max_hours": max_hours,
        }

        if age_hours > max_hours:
            return ValidationResult(
                passed=False,
                message=(
                    f"Table '{contract.table}' is stale: "
                    f"last updated {age_hours:.1f}h ago "
                    f"(SLA: {max_hours}h)."
                ),
                details=details,
                severity="error",
            )

        return ValidationResult(
            passed=True,
            message=(
                f"Freshness OK: last updated {age_hours:.1f}h ago "
                f"(SLA: {max_hours}h)."
            ),
            details=details,
            severity="info",
        )
