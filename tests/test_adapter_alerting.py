"""Unit tests for Email, PagerDuty, and Webhook alert channels.

All network calls are mocked — no real SMTP servers or HTTP endpoints needed.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


from warepact.adapters.alerting.email import EmailChannel
from warepact.adapters.alerting.pagerduty import PagerDutyChannel
from warepact.adapters.alerting.webhook import WebhookChannel
from warepact.interfaces.validator import ValidationResult


# ── Helpers ───────────────────────────────────────────────────────────────────

def _contract(name="orders", table="raw.orders", warehouse="snowflake"):
    c = MagicMock()
    c.name = name
    c.table = table
    c.warehouse = warehouse
    return c


def _result(passed=True, message="ok"):
    return ValidationResult(passed=passed, message=message)


# ── EmailChannel ──────────────────────────────────────────────────────────────

class TestEmailChannel:
    def setup_method(self):
        self.channel = EmailChannel()

    def test_channel_type(self):
        assert self.channel.channel_type == "email"

    def test_send_returns_false_when_no_recipients(self):
        result = self.channel.send(_contract(), [_result()], config={})
        assert result is False

    def test_send_calls_smtp_and_returns_true(self):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=mock_smtp) as smtp_cls:
            ok = self.channel.send(
                _contract(),
                [_result()],
                config={
                    "to": ["alice@example.com"],
                    "smtp_host": "mail.example.com",
                    "smtp_port": "25",
                    "use_tls": False,
                },
            )
        assert ok is True
        smtp_cls.assert_called_once_with("mail.example.com", 25)
        mock_smtp.sendmail.assert_called_once()

    def test_send_uses_tls_by_default(self):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=mock_smtp):
            self.channel.send(
                _contract(),
                [_result()],
                config={"to": ["bob@example.com"]},
            )
        mock_smtp.starttls.assert_called_once()

    def test_send_logs_in_when_credentials_provided(self):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=mock_smtp):
            self.channel.send(
                _contract(),
                [_result()],
                config={
                    "to": ["carol@example.com"],
                    "smtp_user": "user",
                    "smtp_pass": "pass",
                    "use_tls": False,
                },
            )
        mock_smtp.login.assert_called_once_with("user", "pass")

    def test_send_returns_false_on_smtp_error(self):
        with patch("smtplib.SMTP", side_effect=OSError("connection refused")):
            ok = self.channel.send(
                _contract(),
                [_result()],
                config={"to": ["admin@example.com"]},
            )
        assert ok is False

    def test_subject_includes_failed_count_on_failure(self):
        sent_messages = []

        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_smtp.sendmail.side_effect = lambda frm, to, msg: sent_messages.append(msg)

        with patch("smtplib.SMTP", return_value=mock_smtp):
            self.channel.send(
                _contract(name="payments"),
                [_result(passed=False, message="Volume too low"),
                 _result(passed=False, message="Stale data")],
                config={"to": ["ops@example.com"], "use_tls": False},
            )
        assert sent_messages
        # Subject is RFC 2047-encoded when non-ASCII present; check body text instead
        assert "2 check(s) failed" in sent_messages[0]
        assert "Volume too low" in sent_messages[0]

    def test_subject_says_passed_on_success(self):
        sent_messages = []

        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_smtp.sendmail.side_effect = lambda frm, to, msg: sent_messages.append(msg)

        with patch("smtplib.SMTP", return_value=mock_smtp):
            self.channel.send(
                _contract(name="orders"),
                [_result(passed=True)],
                config={"to": ["ops@example.com"], "use_tls": False},
            )
        assert "PASSED" in sent_messages[0]


# ── PagerDutyChannel ──────────────────────────────────────────────────────────

class TestPagerDutyChannel:
    def setup_method(self):
        self.channel = PagerDutyChannel()

    def test_channel_type(self):
        assert self.channel.channel_type == "pagerduty"

    def test_send_returns_false_when_no_routing_key(self):
        result = self.channel.send(_contract(), [], config={})
        assert result is False

    def _mock_urlopen(self, status=200):
        mock_resp = MagicMock()
        mock_resp.status = status
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return patch("urllib.request.urlopen", return_value=mock_resp)

    def test_send_trigger_on_failures(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            ok = self.channel.send(
                _contract(name="orders"),
                [_result(passed=False, message="Volume too low")],
                config={"routing_key": "abc123"},
            )

        assert ok is True
        assert captured[0]["event_action"] == "trigger"
        assert captured[0]["dedup_key"] == "warepact-orders"
        assert "orders" in captured[0]["payload"]["summary"]

    def test_send_resolve_on_all_passed(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(name="orders"),
                [_result(passed=True)],
                config={"routing_key": "abc123"},
            )

        assert captured[0]["event_action"] == "resolve"

    def test_send_returns_false_on_non_200(self):
        with self._mock_urlopen(status=400):
            ok = self.channel.send(
                _contract(),
                [_result(passed=False)],
                config={"routing_key": "abc123"},
            )
        assert ok is False

    def test_send_returns_false_on_exception(self):
        with patch("urllib.request.urlopen", side_effect=OSError("timeout")):
            ok = self.channel.send(
                _contract(),
                [_result()],
                config={"routing_key": "abc123"},
            )
        assert ok is False

    def test_payload_includes_failure_messages(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result(passed=False, message="Null rate exceeded")],
                config={"routing_key": "k", "severity": "critical"},
            )

        details = captured[0]["payload"]["custom_details"]
        assert "Null rate exceeded" in details["failures"]
        assert captured[0]["payload"]["severity"] == "critical"

    def test_uses_custom_source(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result()],
                config={"routing_key": "k", "source": "my-pipeline"},
            )

        assert captured[0]["payload"]["source"] == "my-pipeline"


# ── WebhookChannel ────────────────────────────────────────────────────────────

class TestWebhookChannel:
    def setup_method(self):
        self.channel = WebhookChannel()

    def test_channel_type(self):
        assert self.channel.channel_type == "webhook"

    def test_send_returns_false_when_no_url(self):
        result = self.channel.send(_contract(), [], config={})
        assert result is False

    def test_send_posts_json_payload(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append({
                "url": req.full_url,
                "method": req.method,
                "body": json.loads(req.data.decode()),
            })
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            ok = self.channel.send(
                _contract(name="orders", table="raw.orders"),
                [_result(passed=True), _result(passed=False, message="Too few rows")],
                config={"url": "https://hooks.example.com/warepact"},
            )

        assert ok is True
        body = captured[0]["body"]
        assert body["contract"] == "orders"
        assert body["table"] == "raw.orders"
        assert body["passed"] is False
        assert len(body["failures"]) == 1
        assert body["failures"][0]["message"] == "Too few rows"

    def test_send_uses_put_method(self):
        captured_methods = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured_methods.append(req.method)
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result()],
                config={"url": "https://hooks.example.com/warepact", "method": "PUT"},
            )

        assert captured_methods[0] == "PUT"

    def test_send_includes_custom_headers(self):
        captured_headers = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured_headers.append(dict(req.headers))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result()],
                config={
                    "url": "https://hooks.example.com/warepact",
                    "headers": {"Authorization": "Bearer tok"},
                },
            )

        # Header keys are title-cased by urllib
        headers = {k.lower(): v for k, v in captured_headers[0].items()}
        assert headers.get("authorization") == "Bearer tok"

    def test_send_returns_false_on_4xx(self):
        mock_resp = MagicMock()
        mock_resp.status = 403
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            ok = self.channel.send(
                _contract(),
                [_result()],
                config={"url": "https://hooks.example.com/warepact"},
            )
        assert ok is False

    def test_send_returns_false_on_exception(self):
        with patch("urllib.request.urlopen", side_effect=OSError("conn refused")):
            ok = self.channel.send(
                _contract(),
                [_result()],
                config={"url": "https://hooks.example.com/warepact"},
            )
        assert ok is False

    def test_payload_passed_true_when_no_failures(self):
        captured = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            captured.append(json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result(passed=True), _result(passed=True)],
                config={"url": "https://hooks.example.com/warepact"},
            )

        assert captured[0]["passed"] is True
        assert captured[0]["failures"] == []


# ── TeamsChannel ──────────────────────────────────────────────────────────────

class TestTeamsChannel:
    def setup_method(self):
        from warepact.adapters.alerting.teams import TeamsChannel
        self.channel = TeamsChannel()

    def test_channel_type(self):
        assert self.channel.channel_type == "teams"

    def test_missing_webhook_url_returns_false(self):
        result = self.channel.send(_contract(), [_result()], config={})
        assert result is False

    def test_send_success_returns_true(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = self.channel.send(
                _contract(),
                [_result(passed=False, message="Volume too low")],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )
        assert result is True

    def test_send_http_error_returns_false(self):
        mock_resp = MagicMock()
        mock_resp.status = 400
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = self.channel.send(
                _contract(),
                [_result()],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )
        assert result is False

    def test_send_network_error_returns_false(self):
        with patch("urllib.request.urlopen", side_effect=OSError("network down")):
            result = self.channel.send(
                _contract(),
                [_result()],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )
        assert result is False

    def test_payload_is_adaptive_card_format(self):
        captured = []
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            import json as _json
            captured.append(_json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result(passed=False, message="Schema mismatch")],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )

        payload = captured[0]
        assert payload["type"] == "message"
        assert "attachments" in payload
        card = payload["attachments"][0]["content"]
        assert card["type"] == "AdaptiveCard"

    def test_failure_message_in_card_body(self):
        captured = []
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            import json as _json
            captured.append(_json.loads(req.data.decode()))
            return mock_resp

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                _contract(),
                [_result(passed=False, message="Volume below minimum")],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )

        body_text = str(captured[0]["attachments"][0]["content"]["body"])
        assert "Volume below minimum" in body_text

    def test_consumer_names_in_card_when_breach(self):
        captured = []
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        def fake_urlopen(req, timeout=None):
            import json as _json
            captured.append(_json.loads(req.data.decode()))
            return mock_resp

        contract = _contract()
        consumer = MagicMock()
        consumer.team = "analytics-team"
        consumer.dashboard = None
        contract.consumers = [consumer]

        with patch("urllib.request.urlopen", fake_urlopen):
            self.channel.send(
                contract,
                [_result(passed=False, message="Stale data")],
                config={"webhook_url": "https://teams.example.com/webhook"},
            )

        body_text = str(captured[0]["attachments"][0]["content"]["body"])
        assert "analytics-team" in body_text
