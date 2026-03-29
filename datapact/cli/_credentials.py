"""Credential loader — reads warehouse credentials from environment variables.

Each warehouse type has a canonical set of env vars.  Users export them
before running `datapact check`; no secrets ever live in contract YAML.
"""

from __future__ import annotations

import os


_WAREHOUSE_ENV_VARS: dict[str, list[str]] = {
    "snowflake": [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
    ],
    "bigquery": ["GOOGLE_APPLICATION_CREDENTIALS", "BIGQUERY_PROJECT"],
    "redshift": [
        "REDSHIFT_HOST",
        "REDSHIFT_PORT",
        "REDSHIFT_DATABASE",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
    ],
    "postgres": [
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ],
    "databricks": [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HTTP_PATH",
    ],
    "duckdb": ["DUCKDB_DATABASE"],
}

_KEY_MAP: dict[str, dict[str, str]] = {
    "snowflake": {
        "SNOWFLAKE_ACCOUNT": "account",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PASSWORD": "password",
        "SNOWFLAKE_WAREHOUSE": "warehouse",
        "SNOWFLAKE_DATABASE": "database",
        "SNOWFLAKE_SCHEMA": "schema",
        "SNOWFLAKE_ROLE": "role",
    },
    "bigquery": {
        "GOOGLE_APPLICATION_CREDENTIALS": "credentials_path",
        "BIGQUERY_PROJECT": "project",
    },
    "redshift": {
        "REDSHIFT_HOST": "host",
        "REDSHIFT_PORT": "port",
        "REDSHIFT_DATABASE": "database",
        "REDSHIFT_USER": "user",
        "REDSHIFT_PASSWORD": "password",
    },
    "postgres": {
        "PGHOST": "host",
        "PGPORT": "port",
        "PGDATABASE": "database",
        "PGUSER": "user",
        "PGPASSWORD": "password",
    },
    "databricks": {
        "DATABRICKS_HOST": "server_hostname",
        "DATABRICKS_TOKEN": "access_token",
        "DATABRICKS_HTTP_PATH": "http_path",
    },
    "duckdb": {
        "DUCKDB_DATABASE": "database",
    },
}


def get_required_env_vars(warehouse_type: str) -> list[str]:
    """Return the list of env var names required for *warehouse_type*."""
    return _WAREHOUSE_ENV_VARS.get(warehouse_type.lower(), [])


def load_credentials(warehouse_type: str) -> dict:
    """
    Return a credentials dict for *warehouse_type* populated from env vars.

    Only includes keys that are actually set — adapters use sensible defaults
    for anything that's missing.
    """
    env_vars = _WAREHOUSE_ENV_VARS.get(warehouse_type.lower(), [])
    key_map = _KEY_MAP.get(warehouse_type.lower(), {})
    return {
        key_map[var]: os.environ[var]
        for var in env_vars
        if var in os.environ and var in key_map
    }


def detect_warehouse_from_env() -> str | None:
    """
    Guess which warehouse the user has configured based on which env vars
    are present.  Returns the warehouse type string or None.
    """
    checks = [
        ("snowflake", "SNOWFLAKE_ACCOUNT"),
        ("bigquery", "GOOGLE_APPLICATION_CREDENTIALS"),
        ("redshift", "REDSHIFT_HOST"),
        ("postgres", "PGHOST"),
        ("databricks", "DATABRICKS_HOST"),
        ("duckdb", "DUCKDB_DATABASE"),
    ]
    for warehouse, key in checks:
        if os.environ.get(key):
            return warehouse
    return None
