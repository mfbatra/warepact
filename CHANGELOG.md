# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `DatabricksAdapter` — warehouse adapter for Databricks SQL warehouses via `databricks-sql-connector`
- `TeamsChannel` — Microsoft Teams alert channel using Adaptive Cards webhooks
- `ScheduleValidator` — checks that tables are delivered by their `expected_by` time
- `SchemaValidator` now enforces `min`/`max` range constraints on numeric columns
- Consumer surfacing — affected consumers listed in `to_human_readable()` and Slack alerts on breach
- `S3ContractStore` — S3-backed contract persistence
- `GCSContractStore` — GCS-backed contract persistence
- `datapact publish` — HTTP publish to a contract registry
- `datapact watch --cron` — cron-based schedule support
- MCP server (`datapact mcp`) with 5 tools: `check_contract`, `list_contracts`, `explain_breach`, `get_contract_health`, `suggest_contract`
- CI/CD pipeline (GitHub Actions) — ruff + mypy + pytest on every PR
- `databricks`, `s3`, `gcs` optional dependency extras; `all` extra now includes all adapters

### Fixed
- `ruff`: removed all unused imports across adapter and CLI modules
- `mypy`: zero errors under `strict = true`; per-package overrides for implementation packages

[Unreleased]: https://github.com/datapact/datapact/compare/HEAD...HEAD
