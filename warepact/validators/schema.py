"""SchemaValidator — checks column presence, types, nullability, uniqueness,
and allowed values against the warehouse schema."""

from __future__ import annotations

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult, Validator
from warepact.interfaces.warehouse import WarehouseAdapter


@PluginRegistry.register_validator
class SchemaValidator(Validator):
    """
    Validates the warehouse table schema against the contract's column specs.

    Checks per column:
    - Column exists in warehouse
    - Data type matches (case-insensitive prefix match)
    - not_null: warehouse reports no nulls for the column (via null rate)
    - unique: queries adapter for duplicate count
    - allowed_values: queries adapter for out-of-spec values
    """

    @property
    def name(self) -> str:
        return "schema"

    def validate(self, contract: Contract, adapter: WarehouseAdapter) -> ValidationResult:
        if not contract.columns:
            return ValidationResult(
                passed=True,
                message="No schema constraints defined — skipped.",
                severity="info",
            )

        warehouse_schema = adapter.get_schema(contract.table)
        warehouse_cols: dict[str, str] = {
            col["name"].lower(): col["type"].lower()
            for col in warehouse_schema
        }

        failures: list[str] = []
        details: dict = {}

        for spec in contract.columns:
            col = spec.column.lower()

            # ── Existence ────────────────────────────────────────────────────
            if col not in warehouse_cols:
                failures.append(f"Column '{spec.column}' not found in warehouse.")
                details[spec.column] = {"error": "missing"}
                continue

            actual_type = warehouse_cols[col]
            col_details: dict = {"actual_type": actual_type}

            # ── Type ─────────────────────────────────────────────────────────
            expected_type = spec.type.lower()
            if not actual_type.startswith(expected_type):
                failures.append(
                    f"Column '{spec.column}': expected type '{spec.type}', "
                    f"got '{actual_type}'."
                )
                col_details["type_mismatch"] = {
                    "expected": spec.type,
                    "actual": actual_type,
                }

            # ── Not-null ─────────────────────────────────────────────────────
            if spec.not_null:
                null_rates = adapter.get_null_rates(contract.table, [spec.column])
                null_rate = null_rates.get(spec.column, 0.0)
                if null_rate > 0.0:
                    failures.append(
                        f"Column '{spec.column}' has null_rate={null_rate:.4f} "
                        "but not_null=true."
                    )
                    col_details["null_rate"] = null_rate

            # ── Unique ───────────────────────────────────────────────────────
            if spec.unique:
                rows = adapter.run_query(
                    f"SELECT COUNT(*) - COUNT(DISTINCT {spec.column}) "
                    f"AS duplicates FROM {contract.table}"
                )
                duplicates = rows[0].get("duplicates", 0) if rows else 0
                if duplicates > 0:
                    failures.append(
                        f"Column '{spec.column}' has {duplicates} duplicate value(s) "
                        "but unique=true."
                    )
                    col_details["duplicates"] = duplicates

            # ── Min / Max range ──────────────────────────────────────────────
            if spec.min is not None:
                rows = adapter.run_query(
                    f"SELECT MIN({spec.column}) AS min_val FROM {contract.table}"
                )
                min_val = rows[0].get("min_val") if rows else None
                if min_val is not None and float(min_val) < spec.min:
                    failures.append(
                        f"Column '{spec.column}' min value {min_val} is below "
                        f"spec min={spec.min}."
                    )
                    col_details["min_violation"] = {
                        "actual": min_val,
                        "expected_min": spec.min,
                    }

            if spec.max is not None:
                rows = adapter.run_query(
                    f"SELECT MAX({spec.column}) AS max_val FROM {contract.table}"
                )
                max_val = rows[0].get("max_val") if rows else None
                if max_val is not None and float(max_val) > spec.max:
                    failures.append(
                        f"Column '{spec.column}' max value {max_val} exceeds "
                        f"spec max={spec.max}."
                    )
                    col_details["max_violation"] = {
                        "actual": max_val,
                        "expected_max": spec.max,
                    }

            # ── Allowed values ───────────────────────────────────────────────
            if spec.allowed_values is not None:
                quoted = ", ".join(f"'{v}'" for v in spec.allowed_values)
                rows = adapter.run_query(
                    f"SELECT COUNT(*) AS invalid_count FROM {contract.table} "
                    f"WHERE {spec.column} NOT IN ({quoted}) "
                    f"AND {spec.column} IS NOT NULL"
                )
                invalid_count = rows[0].get("invalid_count", 0) if rows else 0
                if invalid_count > 0:
                    failures.append(
                        f"Column '{spec.column}' has {invalid_count} row(s) with "
                        f"values outside allowed_values={spec.allowed_values}."
                    )
                    col_details["invalid_count"] = invalid_count

            details[spec.column] = col_details

        if failures:
            return ValidationResult(
                passed=False,
                message=f"Schema validation failed: {len(failures)} issue(s).",
                details={"failures": failures, "columns": details},
                severity="error",
            )

        return ValidationResult(
            passed=True,
            message=f"Schema validation passed ({len(contract.columns)} column(s) checked).",
            details={"columns": details},
            severity="info",
        )
