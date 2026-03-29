"""Unit tests for SlackChannel using a mocked HTTP layer."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


from warepact.adapters.alerting.slack import SlackChannel
from warepact.core.registry import PluginRegistry
from warepact.interfaces.alerting import AlertChannel
from warepact.interfaces.validator import ValidationResult


# ── Helpers ────────────────────────────────────────────────────────────────────

def _contract(name="orders", table="analytics.orders", owner="team@co.com"):
    c = MagicMock()
    c.name = name
    c.table = table
    c.owner = owner
    return c


def _ok_result(message="schema OK"):
    return ValidationResult(passed=True, message=message, severity="info")


def _fail_result(message="row count too low"):
    return ValidationResult(passed=False, message=message, severity="error")


def _mock_response(status=200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ── Basic interface tests ──────────────────────────────────────────────────────

class TestSlackChannelInterface:
    def test_is_alert_channel(self):
        assert isinstance(SlackChannel(), AlertChannel)

    def test_channel_type(self):
        assert SlackChannel().channel_type == "slack"

    def test_registered_in_registry(self):
        # Force import to trigger registration
        from warepact.adapters.alerting import slack  # noqa: F401
        assert "slack" in PluginRegistry.list_alert_channels()


# ── send() tests ───────────────────────────────────────────────────────────────

class TestSlackChannelSend:
    def setup_method(self):
        self.ch = SlackChannel()

    def test_missing_webhook_url_returns_false(self):
        result = self.ch.send(_contract(), [], config={})
        assert result is False

    @patch("urllib.request.urlopen")
    def test_successful_send_returns_true(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        ok = self.ch.send(_contract(), [_ok_result()], config={"webhook_url": "https://hooks.example.com/test"})
        assert ok is True

    @patch("urllib.request.urlopen")
    def test_non_200_response_returns_false(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(500)
        ok = self.ch.send(_contract(), [], config={"webhook_url": "https://hooks.example.com/test"})
        assert ok is False

    @patch("urllib.request.urlopen")
    def test_network_error_returns_false(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("connection refused")
        ok = self.ch.send(_contract(), [], config={"webhook_url": "https://hooks.example.com/test"})
        assert ok is False

    @patch("urllib.request.urlopen")
    def test_posts_json(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(_contract(), [_ok_result()], config={"webhook_url": "https://hooks.example.com/test"})
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        assert "blocks" in body

    @patch("urllib.request.urlopen")
    def test_failure_payload_mentions_failed_checks(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(
            _contract(),
            [_fail_result("row count too low"), _fail_result("stale data")],
            config={"webhook_url": "https://hooks.example.com/test"},
        )
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        text = body["blocks"][0]["text"]["text"]
        assert "row count too low" in text
        assert "stale data" in text

    @patch("urllib.request.urlopen")
    def test_pass_payload_contains_contract_name(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(_contract(name="orders"), [_ok_result()], config={"webhook_url": "https://x"})
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        text = body["blocks"][0]["text"]["text"]
        assert "orders" in text

    @patch("urllib.request.urlopen")
    def test_custom_username_used(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(
            _contract(), [], config={"webhook_url": "https://x", "username": "MyBot"}
        )
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        assert body["username"] == "MyBot"

    @patch("urllib.request.urlopen")
    def test_default_username_is_warepact(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(_contract(), [], config={"webhook_url": "https://x"})
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        assert body["username"] == "Warepact"

    @patch("urllib.request.urlopen")
    def test_content_type_is_json(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response(200)
        self.ch.send(_contract(), [], config={"webhook_url": "https://x"})
        req = mock_urlopen.call_args[0][0]
        assert req.get_header("Content-type") == "application/json"
