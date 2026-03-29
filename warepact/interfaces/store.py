"""Abstract contract store interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from warepact.core.contract import Contract


class ContractStore(ABC):
    """
    Persistence layer for Contract objects.

    Default implementation uses the local filesystem (contracts/ directory).
    Swap for a remote store (S3, database) by implementing this interface
    and registering via PluginRegistry.
    """

    @abstractmethod
    def save(self, contract: "Contract") -> None:
        """Persist *contract*, overwriting any existing record with the same name."""

    @abstractmethod
    def load(self, name: str) -> "Contract":
        """Return the Contract identified by *name*.

        Raises ContractNotFoundError if no such contract exists.
        """

    @abstractmethod
    def list_names(self) -> list[str]:
        """Return the names of all stored contracts."""

    @abstractmethod
    def delete(self, name: str) -> None:
        """Remove the contract identified by *name*.

        Raises ContractNotFoundError if no such contract exists.
        """

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Return True if a contract with *name* is stored."""
