"""Tests for JSONParser — valid contracts, malformed JSON, and directory scanning."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from datapact.core.exceptions import ContractNotFoundError, ContractValidationError
from datapact.parsers.json_parser import JSONParser


VALID_CONTRACT = {
    "version": 1,
    "name": "orders",
    "warehouse": "snowflake",
    "table": "analytics.core.orders",
    "owner": "team@company.com",
}

VALID_JSON = json.dumps(VALID_CONTRACT)


@pytest.fixture
def parser():
    return JSONParser()


# ── parse_string ───────────────────────────────────────────────────────────────

class TestParseString:
    def test_valid_json_returns_contract(self, parser):
        contract = parser.parse_string(VALID_JSON)
        assert contract.name == "orders"
        assert contract.warehouse == "snowflake"

    def test_invalid_json_raises(self, parser):
        with pytest.raises(ContractValidationError, match="Invalid JSON"):
            parser.parse_string("{not valid json}")

    def test_non_object_json_raises(self, parser):
        with pytest.raises(ContractValidationError, match="JSON object"):
            parser.parse_string('["item1", "item2"]')

    def test_missing_required_field_raises(self, parser):
        data = {"name": "orders", "warehouse": "snowflake"}  # missing table
        with pytest.raises(ContractValidationError):
            parser.parse_string(json.dumps(data))

    def test_source_in_error_message_for_invalid_json(self, parser):
        with pytest.raises(ContractValidationError, match="my_source"):
            parser.parse_string("{bad}", source="my_source")

    def test_source_in_error_message_for_non_object(self, parser):
        with pytest.raises(ContractValidationError, match="my_source"):
            parser.parse_string("[1, 2, 3]", source="my_source")

    def test_with_schema_columns(self, parser):
        data = {**VALID_CONTRACT, "schema": [
            {"column": "id", "type": "integer", "not_null": True},
        ]}
        contract = parser.parse_string(json.dumps(data))
        assert len(contract.columns) == 1
        assert contract.columns[0].column == "id"

    def test_with_sla(self, parser):
        data = {**VALID_CONTRACT, "sla": {"freshness_hours": 12, "min_rows": 500}}
        contract = parser.parse_string(json.dumps(data))
        assert contract.sla.freshness_hours == 12
        assert contract.sla.min_rows == 500


# ── parse_file ─────────────────────────────────────────────────────────────────

class TestParseFile:
    def test_parses_existing_file(self, parser, tmp_path):
        f = tmp_path / "orders.contract.json"
        f.write_text(VALID_JSON)
        contract = parser.parse_file(f)
        assert contract.name == "orders"

    def test_accepts_string_path(self, parser, tmp_path):
        f = tmp_path / "orders.contract.json"
        f.write_text(VALID_JSON)
        contract = parser.parse_file(str(f))
        assert contract.name == "orders"

    def test_missing_file_raises_not_found(self, parser, tmp_path):
        with pytest.raises(ContractNotFoundError):
            parser.parse_file(tmp_path / "nonexistent.contract.json")

    def test_oserror_on_read_raises_validation_error(self, parser, tmp_path):
        f = tmp_path / "orders.contract.json"
        f.write_text(VALID_JSON)
        with patch.object(Path, "read_text", side_effect=OSError("permission denied")):
            with pytest.raises(ContractValidationError, match="Could not read"):
                parser.parse_file(f)

    def test_invalid_json_file_raises(self, parser, tmp_path):
        f = tmp_path / "bad.contract.json"
        f.write_text("{not valid}")
        with pytest.raises(ContractValidationError):
            parser.parse_file(f)


# ── parse_directory ────────────────────────────────────────────────────────────

class TestParseDirectory:
    def _write(self, directory: Path, filename: str, data: dict) -> Path:
        f = directory / filename
        f.write_text(json.dumps(data))
        return f

    def test_empty_directory_returns_empty_list(self, parser, tmp_path):
        result = parser.parse_directory(tmp_path)
        assert result == []

    def test_parses_all_contract_files(self, parser, tmp_path):
        self._write(tmp_path, "orders.contract.json", VALID_CONTRACT)
        self._write(tmp_path, "users.contract.json", {
            "name": "users", "warehouse": "duckdb", "table": "raw.users"
        })
        contracts = parser.parse_directory(tmp_path)
        names = {c.name for c in contracts}
        assert names == {"orders", "users"}

    def test_ignores_non_contract_json_files(self, parser, tmp_path):
        (tmp_path / "config.json").write_text('{"key": "value"}')
        self._write(tmp_path, "orders.contract.json", VALID_CONTRACT)
        contracts = parser.parse_directory(tmp_path)
        assert len(contracts) == 1

    def test_invalid_file_raises_with_filename_in_error(self, parser, tmp_path):
        self._write(tmp_path, "orders.contract.json", VALID_CONTRACT)
        (tmp_path / "bad.contract.json").write_text("{invalid}")
        with pytest.raises(ContractValidationError, match="bad.contract.json"):
            parser.parse_directory(tmp_path)

    def test_missing_directory_raises_not_found(self, parser, tmp_path):
        with pytest.raises(ContractNotFoundError):
            parser.parse_directory(tmp_path / "no_such_dir")

    def test_results_sorted_alphabetically(self, parser, tmp_path):
        self._write(tmp_path, "zzz.contract.json", {
            "name": "zzz", "warehouse": "x", "table": "t"
        })
        self._write(tmp_path, "aaa.contract.json", {
            "name": "aaa", "warehouse": "x", "table": "t"
        })
        contracts = parser.parse_directory(tmp_path)
        names = [c.name for c in contracts]
        assert names == sorted(names)

    def test_collects_multiple_errors(self, parser, tmp_path):
        (tmp_path / "bad1.contract.json").write_text("{invalid}")
        (tmp_path / "bad2.contract.json").write_text("{also invalid}")
        with pytest.raises(ContractValidationError, match="2 contract file"):
            parser.parse_directory(tmp_path)
