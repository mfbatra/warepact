"""Typed exceptions for DataPact.

All public exceptions inherit from DataPactError so callers can catch the
entire hierarchy with a single except clause when needed.
"""


class DataPactError(Exception):
    """Base class for all DataPact exceptions."""


# ── Registry errors ────────────────────────────────────────────────────────────

class UnknownWarehouseError(DataPactError):
    """Raised when a warehouse type has no registered adapter."""


class UnknownAlertChannelError(DataPactError):
    """Raised when an alert channel type has no registered implementation."""


# ── Contract errors ────────────────────────────────────────────────────────────

class ContractNotFoundError(DataPactError):
    """Raised when a requested contract does not exist in the store."""


class ContractValidationError(DataPactError):
    """Raised when a contract YAML/JSON fails schema validation on load."""


# ── Engine errors ──────────────────────────────────────────────────────────────

class WarehouseConnectionError(DataPactError):
    """Raised when an adapter cannot establish a warehouse connection."""


class ContractCheckError(DataPactError):
    """Raised when the engine encounters a fatal error during a contract check."""


class ContractBreachError(DataPactError):
    """Raised when a contract is breached and the operation requires it to pass."""
