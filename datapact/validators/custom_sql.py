"""CustomSQLValidator — runs user-defined SQL assertions from the contract."""

from __future__ import annotations

from typing import Any

from datapact.core.contract import Contract
from datapact.core.registry import PluginRegistry
from datapact.interfaces.validator import ValidationResult, Validator
from datapact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class CustomSQLValidator(Validator):
    """
    Runs each custom_check defined in the contract and compares the
    single returned scalar to the expected value.

    SQL may include {table} which is substituted with contract.table.

    Example contract YAML:
        custom_checks:
          - name: no_negative_amounts
            sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
            expected: 0
    """

    @property
    def name(self) -> str:
        return "custom_sql"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        if not contract.custom_checks:
            return ValidationResult(
                passed=True,
                message="No custom SQL checks defined — skipped.",
                severity="info",
            )

        failures: list[str] = []
        details: dict = {}

        for check in contract.custom_checks:
            sql = check.sql.format(table=contract.table)
            try:
                rows = adapter.run_query(sql)
            except Exception as exc:
                failures.append(f"Check '{check.name}' errored: {exc}")
                details[check.name] = {"error": str(exc)}
                continue

            actual = _extract_scalar(rows)
            details[check.name] = {"sql": sql, "expected": check.expected, "actual": actual}

            if actual != check.expected:
                failures.append(
                    f"Check '{check.name}': expected {check.expected!r}, "
                    f"got {actual!r}."
                )

        if failures:
            return ValidationResult(
                passed=False,
                message=f"Custom SQL checks failed: {len(failures)} assertion(s).",
                details={**details, "failures": failures},
                severity="error",
            )

        return ValidationResult(
            passed=True,
            message=f"All {len(contract.custom_checks)} custom SQL check(s) passed.",
            details=details,
            severity="info",
        )


def _extract_scalar(rows: list[dict[str, Any]]) -> Any:
    """Return the first value of the first row, or None."""
    if not rows:
        return None
    first_row = rows[0]
    if not first_row:
        return None
    return next(iter(first_row.values()))
