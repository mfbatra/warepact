"""Generic webhook alert channel — POSTs a JSON payload to any URL.

Required alert config keys:
  url         — webhook endpoint URL

Optional:
  headers     — dict of extra HTTP headers (e.g. Authorization)
  method      — "POST" | "PUT" (default: "POST")
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


@PluginRegistry.register_alert_channel("webhook")
class WebhookChannel(AlertChannel):

    @property
    def channel_type(self) -> str:
        return "webhook"

    def send(self, contract: Contract, results: list[ValidationResult], config: dict[str, Any]) -> bool:
        url = config.get("url")
        if not url:
            logger.error("WebhookChannel: 'url' is required.")
            return False

        failures = [r for r in results if not r.passed]
        payload = {
            "contract": contract.name,
            "table": contract.table,
            "warehouse": contract.warehouse,
            "passed": len(failures) == 0,
            "failures": [
                {"message": r.message, "severity": r.severity}
                for r in failures
            ],
        }

        headers = {"Content-Type": "application/json"}
        headers.update(config.get("headers", {}))
        method = config.get("method", "POST").upper()

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status >= 400:
                    logger.error("WebhookChannel: HTTP %s", resp.status)
                    return False
            return True
        except Exception as exc:
            logger.error("WebhookChannel: failed: %s", exc)
            return False
