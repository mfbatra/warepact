"""DistributionValidator — detects value distribution shifts.

Checks that the proportion of each top-N value in allowed_values columns
has not shifted beyond a configured tolerance.

Contract YAML usage — add per-column under schema:
    schema:
      - column: status
        type: string
        allowed_values: [pending, shipped, delivered, cancelled]

And in sla:
    sla:
      max_distribution_shift: 0.2   # allow up to 20pp shift per value

If max_distribution_shift is not set in the SLA this validator is skipped.
"""

from __future__ import annotations

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class DistributionValidator(Validator):
    """
    For columns with allowed_values, queries the actual value distribution
    and flags any value whose proportion differs from a uniform baseline
    by more than sla.max_distribution_shift.

    This catches silent data-quality issues such as a status column that
    suddenly has 90 % 'cancelled' records.
    """

    @property
    def name(self) -> str:
        return "distribution"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        sla = contract.sla
        if sla is None or not hasattr(sla, "max_distribution_shift"):
            return ValidationResult(
                passed=True,
                message="No max_distribution_shift SLA defined — skipped.",
                severity="info",
            )

        # max_distribution_shift is an extra field (not in the Pydantic model yet)
        # Access via model_extra if present, otherwise skip
        max_shift = None
        if sla.model_extra:
            max_shift = sla.model_extra.get("max_distribution_shift")
        if max_shift is None:
            return ValidationResult(
                passed=True,
                message="No max_distribution_shift SLA defined — skipped.",
                severity="info",
            )
        max_shift = float(max_shift)

        columns_with_values = [
            c for c in contract.columns if c.allowed_values
        ]
        if not columns_with_values:
            return ValidationResult(
                passed=True,
                message="No columns with allowed_values — skipped.",
                severity="info",
            )

        failures: list[str] = []
        details: dict = {}

        for col_spec in columns_with_values:
            col = col_spec.column
            allowed = col_spec.allowed_values
            assert allowed is not None  # guaranteed by columns_with_values filter

            # Query actual distribution
            quoted = ", ".join(f"'{v}'" for v in allowed)
            rows = adapter.run_query(
                f"SELECT {col}, COUNT(*) AS cnt FROM {contract.table} "
                f"WHERE {col} IN ({quoted}) GROUP BY {col}"
            )

            if not rows:
                details[col] = {"skipped": "no rows matched allowed_values"}
                continue

            total = sum(r.get("cnt", 0) for r in rows)
            if total == 0:
                continue

            distribution = {
                str(r[col]): r.get("cnt", 0) / total for r in rows
            }
            uniform = 1.0 / len(allowed)
            col_details: dict = {"distribution": distribution, "uniform_baseline": uniform}

            for value in allowed:
                actual = distribution.get(str(value), 0.0)
                shift = abs(actual - uniform)
                if shift > max_shift:
                    failures.append(
                        f"Column '{col}' value '{value}': "
                        f"proportion={actual:.3f}, baseline={uniform:.3f}, "
                        f"shift={shift:.3f} > max={max_shift}"
                    )

            details[col] = col_details

        if failures:
            return ValidationResult(
                passed=False,
                message=f"Distribution shift detected in {len(failures)} value(s).",
                details={"failures": failures, "columns": details},
                severity="warning",
            )

        return ValidationResult(
            passed=True,
            message=(
                f"Distribution OK across {len(columns_with_values)} "
                f"column(s) (max_shift={max_shift})."
            ),
            details={"columns": details},
            severity="info",
        )
