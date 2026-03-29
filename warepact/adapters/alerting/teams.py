"""Microsoft Teams alert channel — Adaptive Cards via incoming webhook.

Required alert config keys:
  webhook_url — Teams incoming webhook URL

Optional:
  title       — card title override (default: "Warepact Contract Check")
"""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import TYPE_CHECKING, Any

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.alerting import AlertChannel

if TYPE_CHECKING:
    from warepact.interfaces.validator import ValidationResult

logger = logging.getLogger(__name__)


@PluginRegistry.register_alert_channel("teams")
class TeamsChannel(AlertChannel):
    """AlertChannel implementation for Microsoft Teams Adaptive Cards webhooks."""

    @property
    def channel_type(self) -> str:
        return "teams"

    def send(
        self,
        contract: Contract,
        results: list[ValidationResult],
        config: dict[str, Any],
    ) -> bool:
        webhook_url = config.get("webhook_url")
        if not webhook_url:
            logger.error("TeamsChannel: 'webhook_url' is required in alert config.")
            return False

        failures = [r for r in results if not r.passed]
        passed = len(failures) == 0
        payload = _build_adaptive_card(contract, results, failures, passed, config)

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 202):
                    logger.error(
                        "TeamsChannel: webhook returned HTTP %s", resp.status
                    )
                    return False
            return True
        except Exception as exc:
            logger.error("TeamsChannel: failed to send alert: %s", exc)
            return False


def _build_adaptive_card(
    contract: Contract,
    results: list[ValidationResult],
    failures: list[ValidationResult],
    passed: bool,
    config: dict[str, Any],
) -> dict[str, Any]:
    status_color = "good" if passed else "attention"
    status_text = "✅ PASSED" if passed else f"❌ FAILED — {len(failures)} issue(s)"
    title = config.get("title", "Warepact Contract Check")

    body: list[dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": title,
            "weight": "Bolder",
            "size": "Medium",
        },
        {
            "type": "TextBlock",
            "text": f"**{contract.name}**: {status_text}",
            "color": status_color,
            "wrap": True,
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "Table", "value": contract.table},
                {"title": "Warehouse", "value": contract.warehouse},
            ],
        },
    ]

    if contract.owner:
        body[-1]["facts"].append({"title": "Owner", "value": str(contract.owner)})

    if failures:
        issue_lines = "\n".join(f"- {r.message}" for r in failures)
        body.append(
            {
                "type": "TextBlock",
                "text": f"**Issues:**\n{issue_lines}",
                "wrap": True,
                "color": "attention",
            }
        )

    if contract.consumers:
        consumer_names = [c.team or c.dashboard or "unknown" for c in contract.consumers]
        body.append(
            {
                "type": "TextBlock",
                "text": f"**Affected consumers:** {', '.join(consumer_names)}",
                "wrap": True,
                "isSubtle": True,
            }
        )

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": body,
                },
            }
        ],
    }
