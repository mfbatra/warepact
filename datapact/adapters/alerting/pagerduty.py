"""PagerDuty alert channel — uses the Events API v2.

Required alert config keys:
  routing_key — PagerDuty integration key (32-char hex)

Optional:
  severity    — "critical" | "error" | "warning" | "info" (default: "error")
  source      — event source identifier (default: "datapact")
"""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import TYPE_CHECKING, Any

from datapact.core.contract import Contract
from datapact.core.registry import PluginRegistry
from datapact.interfaces.alerting import AlertChannel

if TYPE_CHECKING:
    from datapact.interfaces.validator import ValidationResult

logger = logging.getLogger(__name__)

_PD_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"


@PluginRegistry.register_alert_channel("pagerduty")
class PagerDutyChannel(AlertChannel):

    @property
    def channel_type(self) -> str:
        return "pagerduty"

    def send(self, contract: Contract, results: list[ValidationResult], config: dict[str, Any]) -> bool:
        routing_key = config.get("routing_key")
        if not routing_key:
            logger.error("PagerDutyChannel: 'routing_key' is required.")
            return False

        failures = [r for r in results if not r.passed]
        event_action = "resolve" if not failures else "trigger"
        severity = config.get("severity", "error")
        source = config.get("source", "datapact")

        payload = {
            "routing_key": routing_key,
            "event_action": event_action,
            "dedup_key": f"datapact-{contract.name}",
            "payload": {
                "summary": (
                    f"DataPact: '{contract.name}' passed"
                    if not failures
                    else f"DataPact: '{contract.name}' failed — "
                         f"{len(failures)} issue(s)"
                ),
                "source": source,
                "severity": severity,
                "custom_details": {
                    "table": contract.table,
                    "warehouse": contract.warehouse,
                    "failures": [r.message for r in failures],
                },
            },
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                _PD_EVENTS_URL,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 202):
                    logger.error("PagerDutyChannel: HTTP %s", resp.status)
                    return False
            return True
        except Exception as exc:
            logger.error("PagerDutyChannel: failed: %s", exc)
            return False
