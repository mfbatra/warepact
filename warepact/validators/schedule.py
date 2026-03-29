"""ScheduleValidator — checks that a table was delivered on time.

Reads contract.schedule.expected_by (e.g. "06:00 UTC") and verifies that
get_last_updated() falls within today's delivery window.

Contract YAML usage:
    schedule:
      expected_by: "06:00 UTC"
"""

from __future__ import annotations

from datetime import datetime, timezone

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class ScheduleValidator(Validator):
    """
    Verifies that the table was updated by the contract's expected delivery time.

    Skipped when contract.schedule or contract.schedule.expected_by is unset.
    Only evaluates when the current UTC time is past the expected_by time
    (i.e. the delivery window has opened for today).
    """

    @property
    def name(self) -> str:
        return "schedule"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        if contract.schedule is None or contract.schedule.expected_by is None:
            return ValidationResult(
                passed=True,
                message="No schedule defined — skipped.",
                severity="info",
            )

        expected_by_str = contract.schedule.expected_by
        try:
            expected_time = _parse_expected_by(expected_by_str)
        except ValueError as exc:
            return ValidationResult(
                passed=False,
                message=(
                    f"Schedule validator: cannot parse expected_by "
                    f"'{expected_by_str}': {exc}"
                ),
                severity="error",
            )

        now = datetime.now(tz=timezone.utc)

        # Delivery window has not opened yet — nothing to check
        if now < expected_time:
            return ValidationResult(
                passed=True,
                message=(
                    f"Schedule: delivery window not yet reached "
                    f"(expected by {expected_by_str})."
                ),
                details={"expected_by": expected_by_str, "now_utc": now.isoformat()},
                severity="info",
            )

        last_updated = adapter.get_last_updated(contract.table)
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)

        if last_updated >= expected_time:
            return ValidationResult(
                passed=True,
                message=(
                    f"Schedule: table delivered on time "
                    f"(updated {last_updated.strftime('%H:%M UTC')})."
                ),
                details={
                    "last_updated": last_updated.isoformat(),
                    "expected_by": expected_by_str,
                },
                severity="info",
            )

        lag_minutes = int((now - last_updated).total_seconds() // 60)
        return ValidationResult(
            passed=False,
            message=(
                f"Schedule breach: table not updated by {expected_by_str}. "
                f"Last updated: {last_updated.strftime('%H:%M UTC')} "
                f"({lag_minutes} minute(s) ago)."
            ),
            details={
                "last_updated": last_updated.isoformat(),
                "expected_by": expected_by_str,
                "lag_minutes": lag_minutes,
            },
            severity="error",
        )


def _parse_expected_by(expected_by: str) -> datetime:
    """
    Parse an expected_by string like "06:00 UTC" into today's datetime at that time.

    Only UTC is supported. The returned datetime is always timezone-aware.
    """
    parts = expected_by.strip().split()
    time_str = parts[0]

    try:
        hour_str, minute_str = time_str.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
    except (ValueError, AttributeError):
        raise ValueError(
            f"Expected HH:MM format, got '{time_str}'. "
            "Example: '06:00 UTC'"
        )

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Time {hour:02d}:{minute:02d} is out of range.")

    now_utc = datetime.now(tz=timezone.utc)
    return now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
