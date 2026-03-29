"""Slack alert channel.

Sends a rich Block Kit message to a Slack webhook URL when a contract
breaches or recovers.

Required alert config keys (from contract YAML):
  webhook_url — Incoming Webhook URL (supports ${ENV_VAR} expansion)

Optional:
  username    — bot display name (default: "Warepact")
  icon_emoji  — bot icon (default: ":white_check_mark:" / ":x:")
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


@PluginRegistry.register_alert_channel("slack")
class SlackChannel(AlertChannel):
    """AlertChannel implementation for Slack Incoming Webhooks."""

    @property
    def channel_type(self) -> str:
        return "slack"

    def send(
        self,
        contract: Contract,
        results: list[ValidationResult],
        config: dict[str, Any],
    ) -> bool:
        webhook_url = config.get("webhook_url")
        if not webhook_url:
            logger.error("SlackChannel: 'webhook_url' is required in alert config.")
            return False

        failures = [r for r in results if not r.passed]
        passed = len(failures) == 0
        payload = self._build_payload(contract, results, failures, passed, config)

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(
                        "SlackChannel: webhook returned HTTP %s", resp.status
                    )
                    return False
            return True
        except Exception as exc:
            logger.error("SlackChannel: failed to send alert: %s", exc)
            return False

    # ── Payload builder ────────────────────────────────────────────────────────

    def _build_payload(
        self,
        contract: Contract,
        results: list[ValidationResult],
        failures: list[ValidationResult],
        passed: bool,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        status_emoji = ":white_check_mark:" if passed else ":x:"
        status_text = "PASSED" if passed else "FAILED"
        username = config.get("username", "Warepact")

        header = (
            f"{status_emoji} *Contract `{contract.name}` {status_text}*"
        )

        lines = [header, ""]
        if contract.table:
            lines.append(f"*Table:* `{contract.table}`")
        if contract.owner:
            lines.append(f"*Owner:* {contract.owner}")
        lines.append("")

        if failures:
            lines.append(f"*{len(failures)} check(s) failed:*")
            for r in failures:
                lines.append(f"  • `{r.message}`")
        else:
            lines.append(f"All {len(results)} check(s) passed.")

        if failures and contract.consumers:
            consumer_names = [
                c.team or c.dashboard or "unknown"
                for c in contract.consumers
            ]
            lines.append(f"\n*Affected consumers:* {', '.join(consumer_names)}")

        return {
            "username": username,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\n".join(lines),
                    },
                }
            ],
        }
