"""Tests for ContractEngine — mocked adapters, validators, and alert channels."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from warepact.core.contract import Contract
from warepact.core.engine import ContractCheckResult, ContractEngine, LLMExplainer
from warepact.core.exceptions import ContractCheckError
from warepact.core.registry import PluginRegistry
from warepact.interfaces.validator import ValidationResult


# ── Fixtures ───────────────────────────────────────────────────────────────────

MINIMAL = {"name": "orders", "warehouse": "mock", "table": "t"}


def _contract(**kwargs) -> Contract:
    base = {**MINIMAL}
    base.update(kwargs)
    return Contract(**base)


def _pass_result(msg="ok") -> ValidationResult:
    return ValidationResult(passed=True, message=msg, severity="info")


def _fail_result(msg="failed") -> ValidationResult:
    return ValidationResult(passed=False, message=msg, severity="error")


@pytest.fixture
def registry():
    """Isolated registry with a mock warehouse and no validators."""
    PluginRegistry._reset()
    mock_adapter = MagicMock()
    mock_adapter.connect.return_value = None

    @PluginRegistry.register_warehouse("mock")
    class _MockAdapter:
        def connect(self, credentials): pass
        def get_schema(self, t): return []
        def get_row_count(self, t): return 0
        def get_last_updated(self, t): return None
        def run_query(self, sql): return []
        def get_null_rates(self, t, cols): return {}

    yield PluginRegistry
    PluginRegistry._reset()


@pytest.fixture
def engine(registry):
    return ContractEngine(registry=registry)


# ── ContractCheckResult ────────────────────────────────────────────────────────

class TestContractCheckResult:
    def test_to_human_readable_passed(self):
        contract = _contract()
        result = ContractCheckResult(
            contract=contract,
            results=[_pass_result("schema OK"), _pass_result("volume OK")],
            passed=True,
        )
        text = result.to_human_readable()
        assert "PASSED" in text
        assert "schema OK" in text

    def test_to_human_readable_failed(self):
        contract = _contract()
        result = ContractCheckResult(
            contract=contract,
            results=[_fail_result("row count too low")],
            passed=False,
            failures=[_fail_result("row count too low")],
        )
        text = result.to_human_readable()
        assert "FAILED" in text
        assert "row count too low" in text

    def test_to_human_readable_with_explanation(self):
        contract = _contract()
        result = ContractCheckResult(
            contract=contract,
            results=[_fail_result()],
            passed=False,
            explanation="The pipeline may have stalled.",
        )
        text = result.to_human_readable()
        assert "pipeline" in text

    def test_to_dict_structure(self):
        contract = _contract()
        result = ContractCheckResult(
            contract=contract,
            results=[_pass_result("ok")],
            passed=True,
        )
        d = result.to_dict()
        assert d["contract"] == "orders"
        assert d["passed"] is True
        assert isinstance(d["results"], list)
        assert d["results"][0]["message"] == "ok"


# ── ContractEngine.check() ─────────────────────────────────────────────────────

class TestContractEngineCheck:
    def test_returns_check_result(self, engine):
        contract = _contract()
        result = engine.check(contract)
        assert isinstance(result, ContractCheckResult)

    def test_passed_when_no_validators(self, engine):
        result = engine.check(_contract())
        assert result.passed is True

    def test_passed_when_all_validators_pass(self, registry, engine):
        @PluginRegistry.register_validator
        class _AlwaysPass:
            @property
            def name(self): return "always_pass"
            def validate(self, c, a): return _pass_result()

        result = engine.check(_contract())
        assert result.passed is True
        assert len(result.results) == 1

    def test_failed_when_any_validator_fails(self, registry, engine):
        @PluginRegistry.register_validator
        class _AlwaysFail:
            @property
            def name(self): return "always_fail"
            def validate(self, c, a): return _fail_result("bad data")

        result = engine.check(_contract())
        assert result.passed is False
        assert len(result.failures) == 1
        assert result.failures[0].message == "bad data"

    def test_all_validators_run(self, registry, engine):
        ran = []

        @PluginRegistry.register_validator
        class _V1:
            @property
            def name(self): return "v1"
            def validate(self, c, a):
                ran.append("v1")
                return _pass_result()

        @PluginRegistry.register_validator
        class _V2:
            @property
            def name(self): return "v2"
            def validate(self, c, a):
                ran.append("v2")
                return _pass_result()

        engine.check(_contract())
        assert "v1" in ran and "v2" in ran

    def test_credentials_passed_to_adapter(self, registry):
        received_creds = {}

        @PluginRegistry.register_warehouse("creds_test")
        class _CredsAdapter:
            def connect(self, credentials):
                received_creds.update(credentials)
            def get_schema(self, t): return []
            def get_row_count(self, t): return 0
            def get_last_updated(self, t): return None
            def run_query(self, sql): return []
            def get_null_rates(self, t, cols): return {}

        contract = Contract(
            name="orders", warehouse="creds_test", table="t",
            credentials={"password": "secret"}
        )
        ContractEngine(registry=registry).check(contract)
        assert received_creds["password"] == "secret"


class TestContractEngineConnectionFailure:
    def test_unknown_warehouse_raises_contract_check_error(self, engine):
        contract = Contract(name="x", warehouse="no_such_warehouse", table="t")
        with pytest.raises(ContractCheckError, match="no_such_warehouse"):
            engine.check(contract)

    def test_connect_failure_raises_contract_check_error(self, registry):
        @PluginRegistry.register_warehouse("bad_conn")
        class _BadAdapter:
            def connect(self, credentials): raise RuntimeError("timeout")
            def get_schema(self, t): return []
            def get_row_count(self, t): return 0
            def get_last_updated(self, t): return None
            def run_query(self, sql): return []
            def get_null_rates(self, t, cols): return {}

        contract = Contract(name="x", warehouse="bad_conn", table="t")
        with pytest.raises(ContractCheckError, match="timeout"):
            ContractEngine(registry=registry).check(contract)


class TestContractEngineAlerts:
    def test_breach_alert_dispatched_on_failure(self, registry, engine):
        sent = []

        @PluginRegistry.register_alert_channel("test_ch")
        class _TestChannel:
            @property
            def channel_type(self): return "test_ch"
            def send(self, contract, results, config):
                sent.append({"event": "breach", "config": config})
                return True

        @PluginRegistry.register_validator
        class _Fail:
            @property
            def name(self): return "fail"
            def validate(self, c, a): return _fail_result()

        contract = _contract(alerts=[{"channel": "test_ch", "on": ["breach"], "webhook_url": "http://x"}])
        engine.check(contract)
        assert len(sent) == 1
        assert sent[0]["config"]["webhook_url"] == "http://x"

    def test_recovery_alert_dispatched_on_pass(self, registry, engine):
        sent = []

        @PluginRegistry.register_alert_channel("rec_ch")
        class _RecChannel:
            @property
            def channel_type(self): return "rec_ch"
            def send(self, contract, results, config):
                sent.append("recovery")
                return True

        contract = _contract(alerts=[{"channel": "rec_ch", "on": ["recovery"]}])
        engine.check(contract)
        assert "recovery" in sent

    def test_breach_alert_not_sent_on_pass(self, registry, engine):
        sent = []

        @PluginRegistry.register_alert_channel("breach_only_ch")
        class _BreachCh:
            @property
            def channel_type(self): return "breach_only_ch"
            def send(self, contract, results, config):
                sent.append("breach")
                return True

        contract = _contract(alerts=[{"channel": "breach_only_ch", "on": ["breach"]}])
        engine.check(contract)
        assert sent == []

    def test_alert_failure_does_not_raise(self, registry, engine):
        @PluginRegistry.register_alert_channel("exploding_ch")
        class _ExplodingChannel:
            @property
            def channel_type(self): return "exploding_ch"
            def send(self, contract, results, config): raise RuntimeError("kaboom")

        @PluginRegistry.register_validator
        class _Fail:
            @property
            def name(self): return "fail"
            def validate(self, c, a): return _fail_result()

        contract = _contract(alerts=[{"channel": "exploding_ch", "on": ["breach"]}])
        result = engine.check(contract)  # must not raise
        assert not result.passed

    def test_unknown_alert_channel_silently_skipped(self, registry, engine):
        contract = _contract(alerts=[{"channel": "no_such_ch", "on": ["breach"]}])
        # Should not raise UnknownAlertChannelError
        result = engine.check(contract)
        assert result is not None


class TestContractEngineLLM:
    def test_llm_called_on_failure(self, registry, engine):
        called_with = []

        class _TestLLM(LLMExplainer):
            def explain(self, contract, failures):
                called_with.append(failures)
                return "Pipeline backfill looks stalled."

        @PluginRegistry.register_validator
        class _Fail:
            @property
            def name(self): return "fail"
            def validate(self, c, a): return _fail_result()

        eng = ContractEngine(registry=registry, llm_explainer=_TestLLM())
        result = eng.check(_contract())
        assert result.explanation == "Pipeline backfill looks stalled."
        assert len(called_with) == 1

    def test_llm_not_called_on_pass(self, registry):
        class _TestLLM(LLMExplainer):
            def explain(self, contract, failures): return "should not be called"

        eng = ContractEngine(registry=registry, llm_explainer=_TestLLM())
        result = eng.check(_contract())
        assert result.explanation is None

    def test_llm_failure_does_not_propagate(self, registry):
        class _BrokenLLM(LLMExplainer):
            def explain(self, contract, failures): raise RuntimeError("LLM timeout")

        @PluginRegistry.register_validator
        class _Fail:
            @property
            def name(self): return "fail"
            def validate(self, c, a): return _fail_result()

        eng = ContractEngine(registry=registry, llm_explainer=_BrokenLLM())
        result = eng.check(_contract())  # must not raise
        assert result.explanation is None
