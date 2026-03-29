"""Abstract validator interface and ValidationResult dataclass."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from warepact.core.contract import Contract
    from warepact.interfaces.warehouse import WarehouseAdapter


Severity = Literal["error", "warning", "info"]


@dataclass
class ValidationResult:
    """Outcome of a single validator run."""

    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    severity: Severity = "error"


class Validator(ABC):
    """
    A single contract check.

    Each subclass lives in validators/ and owns exactly one concern
    (schema, freshness, volume, nulls, …).  Register via
    @PluginRegistry.register_validator so the engine picks it up
    automatically.
    """

    @abstractmethod
    def validate(
        self,
        contract: "Contract",
        adapter: "WarehouseAdapter",
    ) -> ValidationResult:
        """Run the check and return its outcome."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable validator name used in reports and logs."""
