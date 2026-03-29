"""Email alert channel — sends via SMTP.

Required alert config keys:
  to          — list of recipient addresses
  from_addr   — sender address (default: warepact@localhost)
  smtp_host   — SMTP server host (default: localhost)
  smtp_port   — SMTP port (default: 587)
  smtp_user   — SMTP username (optional)
  smtp_pass   — SMTP password (optional)
  use_tls     — bool (default: true)
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText
from typing import TYPE_CHECKING, Any

from warepact.core.contract import Contract
from warepact.core.registry import PluginRegistry
from warepact.interfaces.alerting import AlertChannel

if TYPE_CHECKING:
    from warepact.interfaces.validator import ValidationResult

logger = logging.getLogger(__name__)


@PluginRegistry.register_alert_channel("email")
class EmailChannel(AlertChannel):

    @property
    def channel_type(self) -> str:
        return "email"

    def send(self, contract: Contract, results: list[ValidationResult], config: dict[str, Any]) -> bool:
        recipients = config.get("to", [])
        if not recipients:
            logger.error("EmailChannel: 'to' (list of addresses) is required.")
            return False

        from_addr = config.get("from_addr", "warepact@localhost")
        failures = [r for r in results if not r.passed]
        subject = (
            f"[Warepact] Contract '{contract.name}' "
            + ("PASSED" if not failures else f"FAILED — {len(failures)} issue(s)")
        )
        body = _build_body(contract, results, failures)

        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)

        try:
            host = config.get("smtp_host", "localhost")
            port = int(config.get("smtp_port", 587))
            use_tls = config.get("use_tls", True)

            with smtplib.SMTP(host, port) as smtp:
                if use_tls:
                    smtp.starttls()
                user = config.get("smtp_user")
                passwd = config.get("smtp_pass")
                if user and passwd:
                    smtp.login(user, passwd)
                smtp.sendmail(from_addr, recipients, msg.as_string())
            return True
        except Exception as exc:
            logger.error("EmailChannel: failed to send: %s", exc)
            return False


def _build_body(contract: Contract, results: list[ValidationResult], failures: list[ValidationResult]) -> str:
    lines = [
        f"Warepact Contract Check: {contract.name}",
        f"Table: {contract.table}",
        "",
    ]
    if failures:
        lines.append(f"{len(failures)} check(s) failed:")
        for r in failures:
            lines.append(f"  - {r.message}")
    else:
        lines.append(f"All {len(results)} check(s) passed.")
    return "\n".join(lines)
