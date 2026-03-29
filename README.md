# DataPact

**The dbt of data contracts.** Define, enforce, and evolve contracts between data producers and consumers — as code, in CI, with zero vendor lock-in.

```bash
pip install datapact[duckdb]
```

---

## Quick start

```bash
# Scaffold a contracts/ directory
datapact init

# Check a contract against your warehouse
datapact check orders

# Check all contracts
datapact check --all

# Generate a contract from a live table
datapact generate analytics.core.orders --warehouse snowflake

# Watch contracts continuously
datapact watch --interval 300
```

---

## Contract format

```yaml
version: 1
name: orders
owner: data-team@company.com
warehouse: snowflake
table: analytics.core.orders

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

sla:
  freshness_hours: 6
  min_rows: 1000
  max_null_rate: 0.01

alerts:
  - channel: slack
    webhook_url: "${SLACK_WEBHOOK}"
    on: [breach, recovery]

custom_checks:
  - name: no_negative_amounts
    sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
    expected: 0
```

---

## Supported warehouses

| Warehouse  | Install extra          |
|------------|------------------------|
| DuckDB     | `pip install datapact[duckdb]`         |
| Snowflake  | `pip install datapact[snowflake]`      |
| BigQuery   | `pip install datapact[bigquery]`       |
| Redshift   | `pip install datapact[redshift]`       |
| Postgres   | `pip install datapact[postgres]`       |
| Databricks | `pip install datapact[databricks]`     |

---

## Configuration

DataPact reads warehouse credentials from environment variables. No secrets belong in contract YAML files.

| Warehouse   | Required env vars |
|-------------|-------------------|
| `snowflake`   | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA` |
| `bigquery`    | `GOOGLE_APPLICATION_CREDENTIALS`, `BIGQUERY_PROJECT` |
| `redshift`    | `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DATABASE`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD` |
| `postgres`    | `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` |
| `databricks`  | `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH` |
| `duckdb`      | `DUCKDB_DATABASE` (path to `.duckdb` file, or `:memory:`) |

`datapact init` automatically detects your warehouse from these env vars and prints the full list of required variables.

## Supported alert channels

- **Slack** — Block Kit webhook
- **Email** — SMTP
- **PagerDuty** — Events API v2
- **Webhook** — Generic JSON POST/PUT

---

## CLI reference

| Command | Description |
|---------|-------------|
| `datapact init` | Scaffold `contracts/` with an example contract |
| `datapact check <name>` | Check one contract |
| `datapact check --all` | Check every contract in `contracts/` |
| `datapact check --all --json` | Output results as JSON |
| `datapact generate <table>` | Generate a contract YAML from a live table |
| `datapact watch` | Continuously re-check on a schedule |
| `datapact diff <name> <fileA> <fileB>` | Diff two contract versions |
| `datapact report` | Generate an HTML health dashboard |
| `datapact publish <name>` | Publish a contract to the registry |
| `datapact mcp` | Start the MCP server for Claude/Cursor integration |

---

## Python API

```python
from datapact import ContractEngine
from datapact.parsers import YAMLParser

parser = YAMLParser()
contract = parser.parse_file("contracts/orders.contract.yaml")

engine = ContractEngine()
result = engine.check(contract)

print(result.to_human_readable())
# Contract 'orders': PASSED
#   ✓ Schema matches expected definition
#   ✓ Row count 45231 is within bounds [1000, ∞)
#   ✓ Data is fresh (last updated 2.1 hours ago)
#   ✓ Null rates are within acceptable limits
```

---

## MCP integration

Start the MCP server and connect it to Claude or Cursor:

```bash
datapact mcp
```

Available tools: `check_contract`, `list_contracts`, `explain_breach`, `get_contract_health`, `suggest_contract`.

---

## Writing a custom adapter

```python
from datapact.core.registry import PluginRegistry
from datapact.interfaces.warehouse import WarehouseAdapter

@PluginRegistry.register_warehouse("mydb")
class MyDBAdapter(WarehouseAdapter):
    def connect(self, credentials: dict) -> None: ...
    def get_schema(self, table: str) -> list[dict]: ...
    def get_row_count(self, table: str) -> int: ...
    def get_last_updated(self, table: str): ...
    def run_query(self, sql: str) -> list[dict]: ...
    def get_null_rates(self, table: str, columns: list[str]) -> dict: ...
```

Drop the file in `datapact/plugins/` or install it as a package — `PluginRegistry.autodiscover()` will pick it up automatically.

---

## License

Apache 2.0 — free forever, no monetization.
