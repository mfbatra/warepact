# DataPact — Full Technical Architecture

## Vision
The dbt of data contracts. Define, enforce, and evolve contracts between
data producers and consumers — with the same simplicity dbt brought to
transformation. Free. Forever. Open source.

---
## Architecture Layers

```
┌────────────────────────────────────────────────┐
│             CLI  /  MCP Server                 │  ← Entry points
├────────────────────────────────────────────────┤
│             Contract Engine                    │  ← Core orchestration
├────────────┬──────────────┬────────────────────┤
│ Validators │   Checkers   │   Alert Router     │  ← Business logic
├────────────┴──────────────┴────────────────────┤
│             Plugin Registry                    │  ← Extension point
├────────────┬──────────────┬────────────────────┤
│  Warehouse │   Alerting   │  Contract Store    │  ← Swappable plugins
│  Adapters  │   Channels   │                    │
└────────────┴──────────────┴────────────────────┘
```

---

## Directory Structure

```
datapact/
├── core/
│   ├── contract.py         # Contract dataclass — pure data, no logic
│   ├── engine.py           # ContractEngine — orchestrates everything
│   ├── registry.py         # PluginRegistry — auto-discovers plugins
│   └── exceptions.py       # Typed exceptions
│
├── interfaces/
│   ├── warehouse.py        # Abstract WarehouseAdapter
│   ├── validator.py        # Abstract Validator + ValidationResult
│   ├── alerting.py         # Abstract AlertChannel
│   └── store.py            # Abstract ContractStore
│
├── validators/
│   ├── schema.py           # Column types, nulls, uniqueness
│   ├── freshness.py        # Last updated vs SLA
│   ├── volume.py           # Row count min/max
│   ├── nulls.py            # Null rate thresholds
│   ├── distribution.py     # Value distribution shifts
│   └── custom_sql.py       # User-defined SQL assertions
│
├── adapters/
│   ├── warehouses/
│   │   ├── snowflake.py
│   │   ├── bigquery.py
│   │   ├── redshift.py
│   │   ├── duckdb.py
│   │   └── postgres.py
│   └── alerting/
│       ├── slack.py
│       ├── email.py
│       ├── pagerduty.py
│       └── webhook.py
│
├── parsers/
│   ├── yaml_parser.py      # Parse .contract.yaml files
│   ├── json_parser.py      # Parse .contract.json files
│   └── dbt_parser.py       # Auto-generate from dbt manifest
│
├── mcp/
│   └── server.py           # MCP server — Claude/Cursor/GPT integration (tools inlined)
│
├── cli/
│   ├── main.py             # typer entry point
│   ├── init.py             # datapact init
│   ├── check.py            # datapact check
│   ├── generate.py         # datapact generate (AI-assisted)
│   ├── publish.py          # datapact publish
│   └── watch.py            # datapact watch
│
└── plugins/                # Community plugins drop in here
    └── README.md
```

---

## Core Interfaces

```python
# interfaces/warehouse.py
from abc import ABC, abstractmethod
from datetime import datetime


class WarehouseAdapter(ABC):

    @abstractmethod
    def connect(self, credentials: dict) -> None: ...

    @abstractmethod
    def get_schema(self, table: str) -> list[dict]: ...

    @abstractmethod
    def get_row_count(self, table: str) -> int: ...

    @abstractmethod
    def get_last_updated(self, table: str) -> datetime: ...

    @abstractmethod
    def run_query(self, sql: str) -> list[dict]: ...

    @abstractmethod
    def get_null_rates(self, table: str, columns: list[str]) -> dict: ...


# interfaces/validator.py
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ValidationResult:
    passed: bool
    message: str
    details: dict
    severity: str  # "error" | "warning" | "info"


class Validator(ABC):

    @abstractmethod
    def validate(
        self,
        contract: "Contract",
        adapter: WarehouseAdapter
    ) -> ValidationResult: ...

    @property
    @abstractmethod
    def name(self) -> str: ...


# interfaces/alerting.py
from abc import ABC, abstractmethod


class AlertChannel(ABC):

    @abstractmethod
    def send(
        self,
        contract: "Contract",
        results: list[ValidationResult],
        config: dict
    ) -> bool: ...

    @property
    @abstractmethod
    def channel_type(self) -> str: ...
```

---

## Plugin Registry — The Plug and Play Engine

```python
# core/registry.py
from importlib import import_module
from typing import Type


class PluginRegistry:
    """
    Auto-discovers plugins via import.
    Drop a file in adapters/warehouses/ and it just works.
    No core changes. Ever.
    """

    _warehouse_adapters: dict[str, Type[WarehouseAdapter]] = {}
    _alert_channels: dict[str, Type[AlertChannel]] = {}
    _validators: list[Type[Validator]] = []

    @classmethod
    def register_warehouse(cls, name: str):
        """@PluginRegistry.register_warehouse("snowflake")"""
        def decorator(adapter_class):
            cls._warehouse_adapters[name] = adapter_class
            return adapter_class
        return decorator

    @classmethod
    def register_alert_channel(cls, name: str):
        def decorator(channel_class):
            cls._alert_channels[name] = channel_class
            return channel_class
        return decorator

    @classmethod
    def register_validator(cls, validator_class):
        cls._validators.append(validator_class)
        return validator_class

    @classmethod
    def get_warehouse(cls, name: str) -> WarehouseAdapter:
        if name not in cls._warehouse_adapters:
            raise UnknownWarehouseError(
                f"No adapter for '{name}'. "
                f"Available: {list(cls._warehouse_adapters)}"
            )
        return cls._warehouse_adapters[name]()

    @classmethod
    def autodiscover(cls):
        """Scan adapters/ and plugins/ — imports trigger @register decorators."""
        for module_path in cls._discover_modules():
            import_module(module_path)


# ─── Adding Snowflake — this is ALL you write ─────────────────────────────────
# adapters/warehouses/snowflake.py

@PluginRegistry.register_warehouse("snowflake")
class SnowflakeAdapter(WarehouseAdapter):

    def connect(self, credentials: dict) -> None:
        import snowflake.connector
        self._conn = snowflake.connector.connect(**credentials)

    def get_schema(self, table: str) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(f"DESCRIBE TABLE {table}")
        return [{"name": row[0], "type": row[1]} for row in cur]

    def get_row_count(self, table: str) -> int:
        cur = self._conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]

    def get_last_updated(self, table: str) -> datetime:
        cur = self._conn.cursor()
        cur.execute(f"""
            SELECT MAX(LAST_ALTERED)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table.split('.')[-1].upper()}'
        """)
        return cur.fetchone()[0]

    def run_query(self, sql: str) -> list[dict]:
        cur = self._conn.cursor(DictCursor)
        cur.execute(sql)
        return cur.fetchall()

    def get_null_rates(self, table: str, columns: list[str]) -> dict:
        parts = [
            f"AVG(CASE WHEN {c} IS NULL THEN 1.0 ELSE 0.0 END) AS {c}"
            for c in columns
        ]
        cur = self._conn.cursor()
        cur.execute(f"SELECT {', '.join(parts)} FROM {table}")
        return dict(zip(columns, cur.fetchone()))
```

---

## Contract Engine — Core Orchestration

```python
# core/engine.py


class ContractEngine:
    """
    Orchestrates contract validation.
    Depends only on abstractions — never on concrete adapters.
    """

    def __init__(
        self,
        registry: PluginRegistry,
        llm_explainer: "LLMExplainer | None" = None
    ):
        self._registry = registry
        self._llm = llm_explainer

    def check(self, contract: Contract) -> ContractCheckResult:

        # 1. Connect to the right warehouse
        adapter = self._registry.get_warehouse(contract.warehouse_type)
        adapter.connect(contract.credentials)

        # 2. Run every registered validator
        results = [
            validator_class().validate(contract, adapter)
            for validator_class in self._registry.get_validators()
        ]

        # 3. AI explanation for failures (optional — only if LLM configured)
        failures = [r for r in results if not r.passed]
        explanation = self._llm.explain(contract, failures) if (
            failures and self._llm
        ) else None

        # 4. Dispatch alerts
        if failures:
            for channel_name in contract.alert_channels:
                channel = self._registry.get_alert_channel(channel_name)
                channel.send(contract, failures, contract.alert_config)

        return ContractCheckResult(
            contract=contract,
            results=results,
            explanation=explanation,
            passed=not failures
        )
```

---

## Contract YAML Schema

```yaml
version: 1
name: orders
description: "Core orders table — source of truth for revenue reporting"
owner: data-team@company.com

warehouse: snowflake
table: analytics.core.orders

consumers:
  - team: revenue-analytics
  - team: ml-team
  - dashboard: executive-dashboard

schedule:
  frequency: daily
  expected_by: "06:00 UTC"

schema:
  - column: order_id
    type: integer
    not_null: true
    unique: true
  - column: status
    type: string
    allowed_values: [pending, shipped, delivered, cancelled]
  - column: amount
    type: float
    not_null: true
    min: 0

sla:
  freshness_hours: 6
  min_rows: 1000
  max_null_rate: 0.01

custom_checks:
  - name: no_negative_amounts
    sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
    expected: 0

alerts:
  - channel: slack
    webhook_url: "${SLACK_WEBHOOK}"
    on: [breach, recovery]
  - channel: email
    to: [data-team@company.com]
    on: [breach]

tags: [revenue, critical]
pii: false
```

---

## MCP Server

```python
# mcp/server.py
from mcp import Server

app = Server("datapact")


@app.tool()
async def check_contract(contract_name: str) -> str:
    """Check if a data contract is currently passing."""
    engine = ContractEngine(PluginRegistry)
    result = engine.check(load_contract(contract_name))
    return result.to_human_readable()


@app.tool()
async def list_contracts() -> str:
    """List all registered contracts and their current status."""
    ...


@app.tool()
async def explain_breach(contract_name: str) -> str:
    """AI explanation of why a contract is breached and what to do next."""
    ...


@app.tool()
async def get_contract_health() -> str:
    """Overall health dashboard across all contracts."""
    ...


@app.tool()
async def suggest_contract(table_name: str) -> str:
    """Auto-generate a contract YAML from a warehouse table using AI."""
    ...
```

---

## CLI

```bash
datapact init                    # detect warehouse, scaffold contracts/
datapact check orders            # check one contract
datapact check --all             # check all contracts
datapact watch                   # continuous monitoring mode
datapact generate orders_raw     # AI-generate contract from table
datapact publish orders          # publish to contract registry
datapact diff orders v1 v2       # compare two versions
datapact report                  # HTML health report
datapact mcp                     # start MCP server for Claude/Cursor/GPT
```

---

## How to Extend — Plug and Play

### Add a new warehouse
1. Create `datapact/adapters/warehouses/databricks.py`
2. Implement `WarehouseAdapter` (6 methods)
3. Decorate: `@PluginRegistry.register_warehouse("databricks")`
4. Done. Zero other files touched.

### Add a new alert channel
1. Create `datapact/adapters/alerting/teams.py`
2. Implement `AlertChannel` (1 method)
3. Decorate: `@PluginRegistry.register_alert_channel("teams")`
4. Done.

### Add a new validator
1. Create `datapact/validators/distribution.py`
2. Implement `Validator` (1 method)
3. Decorate: `@PluginRegistry.register_validator`
4. Automatically included in all future contract checks.

### Community plugin packages
```bash
pip install datapact-databricks          # auto-registers on import
pip install datapact-teams
pip install datapact-great-expectations
pip install datapact-dbt                 # reads dbt manifest, generates contracts
pip install datapact-airflow             # Airflow operator for contract checks
```
Exact same model as dbt packages. Community builds the long tail.

---

## Tech Stack

| Layer        | Choice      | Why                                   |
|---|---|---|
| Language     | Python 3.12 | Universal in data engineering         |
| Contracts    | Pydantic v2 | Type-safe parsing, great errors       |
| CLI          | Typer       | FastAPI-style, auto-generates docs    |
| MCP Server   | FastMCP     | Fastest MCP server in Python          |
| SQL Parsing  | SQLGlot     | Warehouse-agnostic SQL                |
| Terminal     | Rich        | Beautiful, readable output            |
| Testing      | pytest      | Industry standard                     |
| Packaging    | uv          | Modern, fast                          |

---

## Installation

```bash
pip install datapact
pip install datapact[snowflake]
pip install datapact[bigquery]
pip install datapact[duckdb]
pip install datapact[all]
```
