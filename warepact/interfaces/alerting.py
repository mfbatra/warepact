"""Abstract alert channel interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from warepact.core.contract import Contract
    from warepact.interfaces.validator import ValidationResult


class AlertChannel(ABC):
    """
    A notification destination (Slack, email, PagerDuty, …).

    Concrete subclasses live in adapters/alerting/ and register via
    @PluginRegistry.register_alert_channel("name").
    """

    @abstractmethod
    def send(
        self,
        contract: "Contract",
        results: list["ValidationResult"],
        config: dict[str, Any],
    ) -> bool:
        """
        Dispatch a notification.

        Returns True if the alert was delivered successfully, False otherwise.
        Implementations must not raise — log and return False on failure.
        """

    @property
    @abstractmethod
    def channel_type(self) -> str:
        """Canonical channel name (matches the key used in YAML, e.g. "slack")."""
