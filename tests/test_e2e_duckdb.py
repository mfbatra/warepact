"""End-to-end integration tests using a real in-memory DuckDB database.

These tests exercise the full stack:
  YAML file → YAMLParser → ContractEngine → DuckDBAdapter → Validators → Result

No mocks. No external credentials. Runs anywhere.
"""

from __future__ import annotations

import textwrap

import pytest

# Force registration of all built-in validators and adapters
import datapact.validators  # noqa: F401
from datapact.adapters.warehouses.duckdb import DuckDBAdapter
from datapact.core.contract import Contract
from datapact.core.engine import ContractEngine
from datapact.core.registry import PluginRegistry
from datapact.parsers.yaml_parser import YAMLParser


# ── Shared fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def duckdb_conn():
    """In-memory DuckDB connection with test tables, shared across this module."""
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders (
            order_id  INTEGER,
            status    VARCHAR,
            amount    DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
            (1, 'pending',   10.0, CURRENT_TIMESTAMP),
            (2, 'shipped',   20.0, CURRENT_TIMESTAMP),
            (3, 'delivered', 30.0, CURRENT_TIMESTAMP),
            (4, 'pending',   40.0, CURRENT_TIMESTAMP),
            (5, 'shipped',   50.0, CURRENT_TIMESTAMP)
    """)
    conn.execute("""
        CREATE TABLE users (
            user_id   INTEGER,
            email     VARCHAR,
            name      VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO users VALUES
            (1, 'alice@example.com', 'Alice'),
            (2, NULL,                'Bob'),
            (3, 'carol@example.com', 'Carol')
    """)
    return conn


@pytest.fixture(scope="module")
def adapter(duckdb_conn):
    """Connected DuckDBAdapter reusing the shared connection."""
    a = DuckDBAdapter()
    a._conn = duckdb_conn
    # Prevent connect() from overwriting the pre-seeded in-memory connection
    a.connect = lambda _credentials: None
    return a


@pytest.fixture(autouse=True)
def reset_registry():
    PluginRegistry._reset()
    # Re-register validators explicitly (re-import is a no-op after first import)
    from datapact.validators.schema import SchemaValidator
    from datapact.validators.freshness import FreshnessValidator
    from datapact.validators.volume import VolumeValidator
    from datapact.validators.nulls import NullsValidator
    from datapact.validators.custom_sql import CustomSQLValidator
    from datapact.validators.distribution import DistributionValidator
    for v in (SchemaValidator, FreshnessValidator, VolumeValidator,
              NullsValidator, CustomSQLValidator, DistributionValidator):
        PluginRegistry.register_validator(v)
    # Re-register warehouse adapters
    from datapact.adapters.warehouses.bigquery import BigQueryAdapter
    from datapact.adapters.warehouses.redshift import RedshiftAdapter
    from datapact.adapters.warehouses.postgres import PostgresAdapter
    PluginRegistry.register_warehouse("duckdb")(DuckDBAdapter)
    PluginRegistry.register_warehouse("bigquery")(BigQueryAdapter)
    PluginRegistry.register_warehouse("redshift")(RedshiftAdapter)
    PluginRegistry.register_warehouse("postgres")(PostgresAdapter)
    # Re-register alert channels
    from datapact.adapters.alerting.email import EmailChannel
    from datapact.adapters.alerting.pagerduty import PagerDutyChannel
    from datapact.adapters.alerting.webhook import WebhookChannel
    PluginRegistry.register_alert_channel("email")(EmailChannel)
    PluginRegistry.register_alert_channel("pagerduty")(PagerDutyChannel)
    PluginRegistry.register_alert_channel("webhook")(WebhookChannel)
    yield
    PluginRegistry._reset()


@pytest.fixture
def engine():
    return ContractEngine(registry=PluginRegistry)


# ── Helper ─────────────────────────────────────────────────────────────────────

def _contract(yaml_text: str) -> Contract:
    parser = YAMLParser()
    c = parser.parse_string(yaml_text)
    c.credentials = {}
    return c


# ── Full stack: YAML → engine → result ────────────────────────────────────────

class TestE2EPassingContract:
    def test_minimal_contract_passes(self, engine, adapter):
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
        """))
        # Inject the shared adapter directly
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter

        result = engine.check(contract)
        assert result.passed

    def test_schema_contract_passes(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            schema:
              - column: order_id
                type: integer
              - column: status
                type: varchar
              - column: amount
                type: double
        """))
        result = engine.check(contract)
        assert result.passed

    def test_volume_contract_passes(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            sla:
              min_rows: 3
              max_rows: 100
        """))
        result = engine.check(contract)
        assert result.passed

    def test_freshness_contract_passes(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            sla:
              freshness_hours: 999
        """))
        result = engine.check(contract)
        assert result.passed

    def test_null_rate_passes_when_no_nulls(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            schema:
              - column: order_id
                type: integer
              - column: status
                type: varchar
            sla:
              max_null_rate: 0.01
        """))
        result = engine.check(contract)
        assert result.passed

    def test_custom_sql_passes(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            custom_checks:
              - name: no_negative_amounts
                sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
                expected: 0
        """))
        result = engine.check(contract)
        assert result.passed


class TestE2EFailingContract:
    def test_volume_too_low_fails(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            sla:
              min_rows: 10000
        """))
        result = engine.check(contract)
        assert not result.passed
        assert any("below minimum" in f.message for f in result.failures)

    def test_missing_column_fails(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            schema:
              - column: nonexistent_column
                type: integer
        """))
        result = engine.check(contract)
        assert not result.passed
        assert any("nonexistent_column" in str(f.details) for f in result.failures)

    def test_null_rate_exceeded_fails(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        # users.email has 1/3 nulls = 0.333
        contract = _contract(textwrap.dedent("""\
            name: users
            warehouse: duckdb
            table: users
            schema:
              - column: email
                type: varchar
            sla:
              max_null_rate: 0.01
        """))
        result = engine.check(contract)
        assert not result.passed

    def test_custom_sql_violation_fails(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            custom_checks:
              - name: exactly_ten_rows
                sql: "SELECT COUNT(*) FROM {table}"
                expected: 10
        """))
        result = engine.check(contract)
        assert not result.passed


class TestE2ECheckResult:
    def test_result_has_correct_contract_name(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract("name: orders\nwarehouse: duckdb\ntable: orders\n")
        result = engine.check(contract)
        assert result.contract.name == "orders"

    def test_to_human_readable_contains_name(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract("name: orders\nwarehouse: duckdb\ntable: orders\n")
        result = engine.check(contract)
        text = result.to_human_readable()
        assert "orders" in text

    def test_to_dict_is_serialisable(self, engine, adapter):
        import json
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract("name: orders\nwarehouse: duckdb\ntable: orders\n")
        result = engine.check(contract)
        d = result.to_dict()
        json.dumps(d)  # must not raise

    def test_all_validators_ran(self, engine, adapter):
        PluginRegistry._warehouse_adapters["duckdb"] = lambda: adapter
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            schema:
              - column: order_id
                type: integer
            sla:
              min_rows: 1
              freshness_hours: 999
              max_null_rate: 0.5
        """))
        result = engine.check(contract)
        # At least schema, volume, freshness, nulls should have run
        assert len(result.results) >= 4


class TestE2EFilesystemStore:
    def test_save_and_reload_roundtrip(self, tmp_path, adapter):
        from datapact.adapters.stores.filesystem import FilesystemContractStore

        store = FilesystemContractStore(root=tmp_path)
        contract = _contract(textwrap.dedent("""\
            name: orders
            warehouse: duckdb
            table: orders
            sla:
              min_rows: 1
        """))

        store.save(contract)
        assert store.exists("orders")
        assert "orders" in store.list_names()

        loaded = store.load("orders")
        assert loaded.name == "orders"
        assert loaded.sla.min_rows == 1

        store.delete("orders")
        assert not store.exists("orders")

    def test_load_missing_raises(self, tmp_path):
        from datapact.adapters.stores.filesystem import FilesystemContractStore
        from datapact.core.exceptions import ContractNotFoundError

        store = FilesystemContractStore(root=tmp_path)
        with pytest.raises(ContractNotFoundError):
            store.load("ghost")


class TestE2EDbtParser:
    def test_parse_manifest(self, tmp_path):
        import json
        from datapact.parsers.dbt_parser import DbtParser

        manifest = {
            "nodes": {
                "model.my_project.orders": {
                    "resource_type": "model",
                    "name": "orders",
                    "database": "analytics",
                    "schema": "core",
                    "description": "Core orders table",
                    "columns": {
                        "order_id": {"data_type": "integer"},
                        "status": {"data_type": "varchar"},
                    },
                }
            },
            "sources": {},
        }
        mf = tmp_path / "manifest.json"
        mf.write_text(json.dumps(manifest))

        parser = DbtParser()
        contracts = parser.parse_manifest(mf, warehouse="duckdb")
        assert len(contracts) == 1
        assert contracts[0].name == "orders"
        assert len(contracts[0].columns) == 2

    def test_write_contracts(self, tmp_path):
        import json
        from datapact.parsers.dbt_parser import DbtParser

        manifest = {
            "nodes": {
                "model.my_project.users": {
                    "resource_type": "model",
                    "name": "users",
                    "database": "db",
                    "schema": "raw",
                    "columns": {},
                }
            },
            "sources": {},
        }
        mf = tmp_path / "manifest.json"
        mf.write_text(json.dumps(manifest))

        parser = DbtParser()
        contracts = parser.parse_manifest(mf, warehouse="duckdb")
        written = parser.write_contracts(contracts, output_dir=tmp_path / "contracts")
        assert len(written) == 1
        assert written[0].exists()


class TestE2EJSONParser:
    def test_parse_valid_json(self, tmp_path):
        import json
        from datapact.parsers.json_parser import JSONParser

        data = {"name": "orders", "warehouse": "duckdb", "table": "raw.orders"}
        f = tmp_path / "orders.contract.json"
        f.write_text(json.dumps(data))

        parser = JSONParser()
        contract = parser.parse_file(f)
        assert contract.name == "orders"

    def test_invalid_json_raises(self):
        from datapact.parsers.json_parser import JSONParser
        from datapact.core.exceptions import ContractValidationError

        parser = JSONParser()
        with pytest.raises(ContractValidationError):
            parser.parse_string("{bad json")


class TestE2EAdditionalAdapters:
    """Verify BigQuery/Redshift/Postgres/Email/PagerDuty/Webhook register correctly."""

    def test_bigquery_registered(self):
        import datapact.adapters.warehouses.bigquery  # noqa: F401
        assert "bigquery" in PluginRegistry.list_warehouses()

    def test_redshift_registered(self):
        import datapact.adapters.warehouses.redshift  # noqa: F401
        assert "redshift" in PluginRegistry.list_warehouses()

    def test_postgres_registered(self):
        import datapact.adapters.warehouses.postgres  # noqa: F401
        assert "postgres" in PluginRegistry.list_warehouses()

    def test_email_registered(self):
        import datapact.adapters.alerting.email  # noqa: F401
        assert "email" in PluginRegistry.list_alert_channels()

    def test_pagerduty_registered(self):
        import datapact.adapters.alerting.pagerduty  # noqa: F401
        assert "pagerduty" in PluginRegistry.list_alert_channels()

    def test_webhook_registered(self):
        import datapact.adapters.alerting.webhook  # noqa: F401
        assert "webhook" in PluginRegistry.list_alert_channels()

    def test_distribution_validator_registered(self):
        import datapact.validators.distribution  # noqa: F401
        names = {v().name for v in PluginRegistry.get_validators()}
        assert "distribution" in names
