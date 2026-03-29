"""Typed exceptions for Warepact.

All public exceptions inherit from WarepactError so callers can catch the
entire hierarchy with a single except clause when needed.
"""


class WarepactError(Exception):
    """Base class for all Warepact exceptions."""


# ── Registry errors ────────────────────────────────────────────────────────────

class UnknownWarehouseError(WarepactError):
    """Raised when a warehouse type has no registered adapter."""


class UnknownAlertChannelError(WarepactError):
    """Raised when an alert channel type has no registered implementation."""


# ── Contract errors ────────────────────────────────────────────────────────────

class ContractNotFoundError(WarepactError):
    """Raised when a requested contract does not exist in the store."""


class ContractValidationError(WarepactError):
    """Raised when a contract YAML/JSON fails schema validation on load."""


# ── Engine errors ──────────────────────────────────────────────────────────────

class WarehouseConnectionError(WarepactError):
    """Raised when an adapter cannot establish a warehouse connection."""


class ContractCheckError(WarepactError):
    """Raised when the engine encounters a fatal error during a contract check."""


class ContractBreachError(WarepactError):
    """Raised when a contract is breached and the operation requires it to pass."""
