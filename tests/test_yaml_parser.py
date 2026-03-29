"""Tests for YAMLParser — valid contracts, malformed YAML, and directory scanning."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from datapact.core.exceptions import ContractNotFoundError, ContractValidationError
from datapact.parsers.yaml_parser import YAMLParser


VALID_YAML = textwrap.dedent("""\
    version: 1
    name: orders
    warehouse: snowflake
    table: analytics.core.orders
    owner: team@company.com

    schema:
      - column: order_id
        type: integer
        not_null: true
        unique: true
      - column: status
        type: string
        allowed_values: [pending, shipped, delivered]

    sla:
      freshness_hours: 6
      min_rows: 1000
      max_null_rate: 0.01

    alerts:
      - channel: slack
        on: [breach, recovery]
""")


@pytest.fixture
def parser():
    return YAMLParser()


@pytest.fixture
def contracts_dir(tmp_path):
    return tmp_path


# ── parse_string ───────────────────────────────────────────────────────────────

class TestParseString:
    def test_valid_yaml_returns_contract(self, parser):
        contract = parser.parse_string(VALID_YAML)
        assert contract.name == "orders"
        assert contract.warehouse == "snowflake"

    def test_schema_columns_parsed(self, parser):
        contract = parser.parse_string(VALID_YAML)
        assert len(contract.columns) == 2
        assert contract.columns[0].column == "order_id"
        assert contract.columns[0].not_null is True
        assert contract.columns[1].allowed_values == ["pending", "shipped", "delivered"]

    def test_sla_parsed(self, parser):
        contract = parser.parse_string(VALID_YAML)
        assert contract.sla.freshness_hours == 6
        assert contract.sla.min_rows == 1000
        assert contract.sla.max_null_rate == 0.01

    def test_alerts_parsed(self, parser):
        contract = parser.parse_string(VALID_YAML)
        assert len(contract.alerts) == 1
        assert contract.alerts[0].channel == "slack"

    def test_minimal_yaml(self, parser):
        yaml = "name: orders\nwarehouse: duckdb\ntable: raw.orders\n"
        contract = parser.parse_string(yaml)
        assert contract.name == "orders"

    def test_invalid_yaml_syntax_raises(self, parser):
        with pytest.raises(ContractValidationError, match="Invalid YAML"):
            parser.parse_string("name: orders\n  bad: indent: here\n")

    def test_empty_yaml_raises(self, parser):
        with pytest.raises(ContractValidationError, match="Empty"):
            parser.parse_string("")

    def test_non_mapping_yaml_raises(self, parser):
        with pytest.raises(ContractValidationError, match="mapping"):
            parser.parse_string("- item1\n- item2\n")

    def test_missing_required_field_raises(self, parser):
        with pytest.raises(ContractValidationError):
            parser.parse_string("name: orders\nwarehouse: snowflake\n")  # missing table

    def test_invalid_contract_field_raises(self, parser):
        with pytest.raises(ContractValidationError):
            parser.parse_string("name: BAD NAME!\nwarehouse: x\ntable: t\n")

    def test_source_included_in_error_message(self, parser):
        with pytest.raises(ContractValidationError, match="my_source"):
            parser.parse_string("- bad", source="my_source")


# ── parse_file ─────────────────────────────────────────────────────────────────

class TestParseFile:
    def test_parses_existing_file(self, parser, tmp_path):
        f = tmp_path / "orders.contract.yaml"
        f.write_text(VALID_YAML)
        contract = parser.parse_file(f)
        assert contract.name == "orders"

    def test_missing_file_raises_not_found(self, parser, tmp_path):
        with pytest.raises(ContractNotFoundError):
            parser.parse_file(tmp_path / "nonexistent.contract.yaml")

    def test_accepts_string_path(self, parser, tmp_path):
        f = tmp_path / "orders.contract.yaml"
        f.write_text(VALID_YAML)
        contract = parser.parse_file(str(f))
        assert contract.name == "orders"

    def test_invalid_yaml_file_raises(self, parser, tmp_path):
        f = tmp_path / "bad.contract.yaml"
        f.write_text("name: bad\n  broken:\n")
        with pytest.raises(ContractValidationError):
            parser.parse_file(f)


# ── parse_directory ────────────────────────────────────────────────────────────

class TestParseDirectory:
    def _write(self, directory: Path, filename: str, content: str) -> Path:
        f = directory / filename
        f.write_text(content)
        return f

    def test_empty_directory_returns_empty_list(self, parser, contracts_dir):
        result = parser.parse_directory(contracts_dir)
        assert result == []

    def test_parses_all_contract_files(self, parser, contracts_dir):
        yaml_a = VALID_YAML
        yaml_b = "name: users\nwarehouse: duckdb\ntable: raw.users\n"
        self._write(contracts_dir, "orders.contract.yaml", yaml_a)
        self._write(contracts_dir, "users.contract.yaml", yaml_b)
        contracts = parser.parse_directory(contracts_dir)
        names = {c.name for c in contracts}
        assert names == {"orders", "users"}

    def test_accepts_yml_extension(self, parser, contracts_dir):
        self._write(contracts_dir, "orders.contract.yml", VALID_YAML)
        contracts = parser.parse_directory(contracts_dir)
        assert len(contracts) == 1

    def test_ignores_non_contract_yaml_files(self, parser, contracts_dir):
        self._write(contracts_dir, "dbt_project.yaml", "name: myproject\n")
        self._write(contracts_dir, "orders.contract.yaml", VALID_YAML)
        contracts = parser.parse_directory(contracts_dir)
        assert len(contracts) == 1

    def test_invalid_file_raises_with_all_errors(self, parser, contracts_dir):
        self._write(contracts_dir, "orders.contract.yaml", VALID_YAML)
        self._write(contracts_dir, "bad.contract.yaml", "- not a mapping")
        with pytest.raises(ContractValidationError, match="bad.contract.yaml"):
            parser.parse_directory(contracts_dir)

    def test_missing_directory_raises(self, parser, tmp_path):
        with pytest.raises(ContractNotFoundError):
            parser.parse_directory(tmp_path / "no_such_dir")

    def test_results_sorted_by_name(self, parser, contracts_dir):
        self._write(contracts_dir, "zzz.contract.yaml", "name: zzz\nwarehouse: x\ntable: t\n")
        self._write(contracts_dir, "aaa.contract.yaml", "name: aaa\nwarehouse: x\ntable: t\n")
        contracts = parser.parse_directory(contracts_dir)
        names = [c.name for c in contracts]
        assert names == sorted(names)


# ── Environment variable expansion ────────────────────────────────────────────

class TestEnvVarExpansion:
    def test_env_var_expanded_in_alert(self, parser, monkeypatch):
        monkeypatch.setenv("WEBHOOK", "https://hooks.example.com/xyz")
        yaml_text = textwrap.dedent("""\
            name: orders
            warehouse: snowflake
            table: t
            alerts:
              - channel: slack
                webhook_url: "${WEBHOOK}"
                on: [breach]
        """)
        contract = parser.parse_string(yaml_text)
        assert contract.alerts[0].model_extra["webhook_url"] == "https://hooks.example.com/xyz"

    def test_missing_env_var_raises(self, parser, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        yaml_text = textwrap.dedent("""\
            name: orders
            warehouse: snowflake
            table: t
            alerts:
              - channel: slack
                webhook_url: "${MISSING_VAR}"
                on: [breach]
        """)
        # Pydantic wraps the ValueError in ValidationError → ContractValidationError
        with pytest.raises(ContractValidationError, match="MISSING_VAR"):
            parser.parse_string(yaml_text)
