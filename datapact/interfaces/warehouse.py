"""Abstract warehouse adapter interfaces.

Thin capability interfaces (ISP) let adapters implement only what they
actually support. WarehouseAdapter is the full contract requiring all
capabilities.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


# ── Segregated capability interfaces (Interface Segregation Principle) ─────────

class ISchemaValidatable(ABC):
    """Adapter supports schema introspection."""

    @abstractmethod
    def get_schema(self, table: str) -> list[dict[str, Any]]:
        """Return column definitions: [{"name": str, "type": str}, ...]."""


class IFreshnessCheckable(ABC):
    """Adapter supports freshness / last-updated queries."""

    @abstractmethod
    def get_last_updated(self, table: str) -> datetime:
        """Return the timestamp of the most recent write to *table*."""


class IVolumeCheckable(ABC):
    """Adapter supports row-count queries."""

    @abstractmethod
    def get_row_count(self, table: str) -> int:
        """Return the number of rows in *table*."""


class INullCheckable(ABC):
    """Adapter supports null-rate queries."""

    @abstractmethod
    def get_null_rates(self, table: str, columns: list[str]) -> dict[str, float]:
        """Return {column_name: null_fraction} for each column in *columns*."""


class IQueryable(ABC):
    """Adapter supports arbitrary SQL execution."""

    @abstractmethod
    def run_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute *sql* and return rows as a list of dicts."""


# ── Full warehouse adapter ─────────────────────────────────────────────────────

class WarehouseAdapter(
    ISchemaValidatable,
    IFreshnessCheckable,
    IVolumeCheckable,
    INullCheckable,
    IQueryable,
    ABC,
):
    """
    Full warehouse adapter — implements every capability.

    Concrete subclasses live in adapters/warehouses/ and register themselves
    via @PluginRegistry.register_warehouse("name").  Core code never imports
    them directly.
    """

    @abstractmethod
    def connect(self, credentials: dict[str, Any]) -> None:
        """Open a connection using the provided *credentials* mapping."""
