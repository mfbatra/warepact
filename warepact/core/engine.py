"""ContractEngine — orchestrates contract validation.

Depends only on abstractions (PluginRegistry + interfaces).
Never imports a concrete adapter or alert channel directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from warepact.core.contract import Contract
from warepact.core.exceptions import ContractCheckError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult

if TYPE_CHECKING:
    pass


# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class ContractCheckResult:
    """Full outcome of a single contract check run."""

    contract: Contract
    results: list[ValidationResult]
    passed: bool
    failures: list[ValidationResult] = field(default_factory=list)
    explanation: str | None = None

    def to_human_readable(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [f"Contract '{self.contract.name}': {status}"]
        for r in self.results:
            icon = "✓" if r.passed else "✗"
            lines.append(f"  {icon} {r.message}")
        if not self.passed and self.contract.consumers:
            consumer_names = [
                c.team or c.dashboard or "unknown"
                for c in self.contract.consumers
            ]
            lines.append(f"\nAffected consumers: {', '.join(consumer_names)}")
        if self.explanation:
            lines.append(f"\nAI Explanation:\n{self.explanation}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract.name,
            "passed": self.passed,
            "results": [
                {
                    "passed": r.passed,
                    "message": r.message,
                    "severity": r.severity,
                    "details": r.details,
                }
                for r in self.results
            ],
            "explanation": self.explanation,
        }


# ── Engine ─────────────────────────────────────────────────────────────────────

class ContractEngine:
    """
    Orchestrates contract validation end-to-end.

    1. Resolves the right warehouse adapter via the registry.
    2. Runs every registered validator.
    3. Optionally generates an AI explanation for failures.
    4. Dispatches alerts to configured channels.

    Core rule: this class never imports a concrete adapter or channel —
    everything is resolved at runtime through PluginRegistry.
    """

    def __init__(
        self,
        registry: type[PluginRegistry] | None = None,
        llm_explainer: LLMExplainer | None = None,
    ) -> None:
        self._registry = registry or PluginRegistry
        self._llm = llm_explainer

    def check(self, contract: Contract) -> ContractCheckResult:
        """
        Run all registered validators against *contract*.

        Returns a ContractCheckResult regardless of outcome.
        Raises ContractCheckError only on unrecoverable infrastructure failures.
        """
        try:
            adapter = self._registry.get_warehouse(contract.warehouse)
            adapter.connect(contract.credentials)
        except Exception as exc:
            raise ContractCheckError(
                f"Could not connect to warehouse '{contract.warehouse}': {exc}"
            ) from exc

        try:
            results = [
                validator_class().validate(contract, adapter)
                for validator_class in self._registry.get_validators()
            ]
        except Exception as exc:
            raise ContractCheckError(
                f"Validation run failed for contract '{contract.name}': {exc}"
            ) from exc

        failures = [r for r in results if not r.passed]
        passed = len(failures) == 0

        # ── Optional AI explanation ─────────────────────────────────────────
        explanation: str | None = None
        if failures and self._llm is not None:
            try:
                explanation = self._llm.explain(contract, failures)
            except Exception:
                pass  # LLM is best-effort — never block on it

        # ── Alert dispatch ──────────────────────────────────────────────────
        event = "recovery" if passed else "breach"
        self._dispatch_alerts(contract, results, failures, event)

        return ContractCheckResult(
            contract=contract,
            results=results,
            passed=passed,
            failures=failures,
            explanation=explanation,
        )

    def _dispatch_alerts(
        self,
        contract: Contract,
        results: list[ValidationResult],
        failures: list[ValidationResult],
        event: str,
    ) -> None:
        """Send alerts for every configured channel that matches the event."""
        for alert_spec in contract.alerts:
            if event not in alert_spec.on:
                continue
            try:
                channel = self._registry.get_alert_channel(alert_spec.channel)
            except Exception:
                continue  # Unknown channel — skip, don't crash the check

            config = dict(alert_spec.model_extra or {})
            try:
                channel.send(contract, failures if event == "breach" else results, config)
            except Exception:
                pass  # Alert failure must never surface to the caller


# ── LLM explainer interface ────────────────────────────────────────────────────

class LLMExplainer:
    """
    Optional plug-in for AI-generated failure explanations.

    Subclass this and pass an instance to ContractEngine to enable
    the explain_breach MCP tool and AI explanations in reports.
    """

    def explain(
        self,
        contract: Contract,
        failures: list[ValidationResult],
    ) -> str:
        raise NotImplementedError
