import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from incident_agent.agent import (Config, IncidentAgent, collect_delivery_evidence, collect_evidence,
                                  explain_delivery_with_model, explain_with_model, format_delivery_report,
                                  format_report)


def config() -> Config:
    return Config(
        namespace="notification-service",
        prometheus_url="http://prometheus:9090",
        smtp_host="smtp.gmail.com",
        smtp_port=587,
        smtp_user="sender@example.com",
        smtp_sender="sender@example.com",
        smtp_recipient="developer@example.com",
        smtp_password_file="/unused/smtp",
        webhook_token_file="/unused/webhook",
    )


def evidence(fail_open=5, app_up=1, errors=2):
    return {
        "fail_open_5m": fail_open,
        "app_up": app_up,
        "pods": [{"name": "notification-service-abc", "phase": "Running", "ready": True, "restarts": 0}],
        "redis_error_lines_15m": errors,
        "gaps": [],
    }


def delivery_evidence(overdue=2, age=8, app_up=1, warnings=0):
    return {
        "overdue_confirmation": overdue,
        "awaiting_confirmation": 4,
        "database_poll_age_seconds": age,
        "app_up": app_up,
        "pods": [{"name": "notification-service-abc", "phase": "Running", "ready": True, "restarts": 0}],
        "brevo_event_warning_lines_15m": warnings,
        "gaps": [],
    }


def alert(fingerprint="abc12345", status="firing", name="RedisRateLimiterFailOpen", starts_at=""):
    return {"alerts": [{"status": status, "fingerprint": fingerprint,
                        "startsAt": starts_at, "labels": {"alertname": name}}]}


class IncidentAgentTest(unittest.TestCase):
    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.explain_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_sends_once_per_fingerprint(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert()), 200)
        self.assertEqual(agent.accept(alert()), 200)
        collector.assert_called_once()
        model.assert_called_once()
        email.assert_called_once()
        self.assertIn("incident_agent_reports_sent_total 1", agent.metrics())

    @patch("incident_agent.agent.send_email")
    def test_ignores_unrelated_and_resolved_alerts(self, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert(name="SomeOtherAlert")), 204)
        self.assertEqual(agent.accept(alert(status="resolved")), 204)
        self.assertEqual(agent.accept(alert(fingerprint="not valid")), 400)
        email.assert_not_called()

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.explain_with_model", side_effect=TimeoutError)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_model_failure_keeps_rule_based_report(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert()), 200)
        body = email.call_args.args[1]
        self.assertIn("Rate limiting was bypassed", body)
        self.assertNotIn("AI-selected focus", body)
        self.assertIn("incident_agent_model_fallbacks_total 1", agent.metrics())

    @patch("incident_agent.agent.send_email", side_effect=OSError("SMTP failed"))
    @patch("incident_agent.agent.explain_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_email_failure_allows_alertmanager_retry(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert()), 503)
        self.assertEqual(agent.accept(alert()), 503)
        self.assertEqual(email.call_count, 2)
        self.assertIn("incident_agent_reports_failed_total 2", agent.metrics())

    def test_report_does_not_invent_redis_outage_or_notification_loss(self):
        body = format_report(evidence(fail_open=4, errors=0), None)
        self.assertIn("Rate limiting was bypassed", body)
        self.assertNotIn("Redis is down", body)
        self.assertNotIn("notifications were lost", body)
        recovered = format_report(evidence(fail_open=0, errors=0), None)
        self.assertIn("may have recovered", recovered)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.explain_delivery_with_model", return_value=None)
    @patch("incident_agent.agent.collect_delivery_evidence", return_value=delivery_evidence())
    def test_delivery_alert_uses_its_own_report_and_dedupe_key(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert(name="BrevoDeliveryConfirmationOverdue")), 200)
        self.assertEqual(agent.accept(alert(name="BrevoDeliveryConfirmationOverdue")), 200)
        collector.assert_called_once()
        model.assert_called_once()
        self.assertEqual(email.call_count, 1)
        self.assertEqual(email.call_args.args[2], "BrevoDeliveryConfirmationOverdue")
        self.assertIn("does not prove non-delivery", email.call_args.args[1])
        self.assertNotIn("Redis", email.call_args.args[1])

    def test_delivery_report_marks_stale_and_recovered_snapshots(self):
        stale = format_delivery_report(delivery_evidence(overdue=2, age=90), None)
        self.assertIn("stale or unavailable", stale)
        self.assertNotIn("still lack a recorded delivery", stale)
        recovered = format_delivery_report(delivery_evidence(overdue=0), None)
        self.assertIn("may have recovered", recovered)
        self.assertIn("Do not resend", recovered)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.explain_delivery_with_model", return_value=None)
    @patch("incident_agent.agent.collect_delivery_evidence", return_value=delivery_evidence())
    def test_new_alert_episode_is_not_suppressed(self, collector, model, email):
        agent = IncidentAgent(config())
        first = alert(name="BrevoDeliveryConfirmationOverdue", starts_at="2026-11-09T10:00:00Z")
        second = alert(name="BrevoDeliveryConfirmationOverdue", starts_at="2026-11-09T11:00:00.123456789Z")
        self.assertEqual(agent.accept(first), 200)
        self.assertEqual(agent.accept(first), 200)
        self.assertEqual(agent.accept(second), 200)
        self.assertEqual(email.call_count, 2)
        self.assertEqual(agent.accept(alert(name="BrevoDeliveryConfirmationOverdue",
                                            starts_at="not-a-date")), 400)

    @patch("incident_agent.agent.kubernetes_get")
    @patch("incident_agent.agent.prometheus_value")
    def test_delivery_evidence_counts_warnings_without_exposing_logs(self, metric, kube):
        metric.side_effect = [2.0, 4.0, 8.0, 1.0]
        kube.side_effect = [
            json.dumps({"items": [{"metadata": {"name": "notification-service-abc"},
                                   "status": {"phase": "Running", "containerStatuses": [
                                       {"name": "notification-service", "ready": True, "restartCount": 0}]}}]}),
            "Ignoring malformed Brevo event for notification private@example.com\npassword=hidden\n",
        ]
        result = collect_delivery_evidence(config())
        self.assertEqual(result["brevo_event_warning_lines_15m"], 1)
        self.assertNotIn("private@example.com", json.dumps(result))
        self.assertNotIn("password=hidden", json.dumps(result))

    @patch("incident_agent.agent.kubernetes_get")
    @patch("incident_agent.agent.prometheus_value")
    def test_collects_only_counts_not_raw_logs(self, metric, kube):
        metric.side_effect = [6.0, 1.0]
        kube.side_effect = [
            json.dumps({"items": [{"metadata": {"name": "notification-service-abc"},
                                   "status": {"phase": "Running", "containerStatuses": [
                                       {"name": "notification-service", "ready": True, "restartCount": 0}]}}]}),
            "recipient=private@example.com Redis connection timed out\npassword=hidden\n",
        ]
        result = collect_evidence(config())
        self.assertEqual(result["redis_error_lines_15m"], 1)
        self.assertNotIn("private@example.com", json.dumps(result))
        self.assertNotIn("password=hidden", json.dumps(result))

    def test_model_output_must_match_contract(self):
        model_config = Config(**{**config().__dict__, "model_url": "http://ollama:11434/api/chat",
                                 "model_name": "gemma3:1b"})
        response = {"message": {"content": json.dumps({
            "focus": "redis_connectivity", "confidence": "medium",
            "evidence_keys": ["fail_open_5m", "redis_error_lines_15m"]})}}
        class FakeResponse:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def read(self, *_):
                return json.dumps(response).encode()
        with patch("incident_agent.agent.urlopen", return_value=FakeResponse()):
            result = explain_with_model(model_config, evidence())
            self.assertIn("unverified", result)
            self.assertIn("fail_open_5m, redis_error_lines_15m", result)
            self.assertNotIn("app_up", result)

    def test_model_rejects_unsupported_claim_and_evidence(self):
        model_config = Config(**{**config().__dict__, "model_url": "http://ollama:11434/api/chat",
                                 "model_name": "gemma3:1b"})

        class FakeResponse:
            def __init__(self, result):
                self.result = result
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def read(self, *_):
                return json.dumps({"message": {"content": json.dumps(self.result)}}).encode()

        for result in (
            {"focus": "redis_connectivity", "confidence": "high", "evidence_keys": ["pods_total"]},
            {"focus": "redis_connectivity", "confidence": "low", "evidence_keys": ["gaps"]},
            {"focus": "application_health", "confidence": "medium", "evidence_keys": ["pods_ready"]},
        ):
            with self.subTest(result=result), patch("incident_agent.agent.urlopen", return_value=FakeResponse(result)):
                with self.assertRaises(ValueError):
                    explain_with_model(model_config, evidence())

    def test_delivery_model_requires_fresh_direct_evidence(self):
        model_config = Config(**{**config().__dict__, "model_url": "http://ollama:11434/api/chat",
                                 "model_name": "gemma3:1b"})

        class FakeResponse:
            def __init__(self, result):
                self.result = result
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def read(self, *_):
                return json.dumps({"message": {"content": json.dumps(self.result)}}).encode()

        valid = {"focus": "delivery_confirmation", "confidence": "low",
                 "evidence_keys": ["overdue_confirmation"]}
        with patch("incident_agent.agent.urlopen", return_value=FakeResponse(valid)):
            self.assertIn("unverified", explain_delivery_with_model(model_config, delivery_evidence()))
        for facts, result in (
            (delivery_evidence(age=90), valid),
            (delivery_evidence(), {"focus": "delivery_confirmation", "confidence": "high",
                                   "evidence_keys": ["brevo_event_warning_lines_15m"]}),
            (delivery_evidence(overdue=0), valid),
        ):
            with self.subTest(facts=facts, result=result), patch(
                    "incident_agent.agent.urlopen", return_value=FakeResponse(result)):
                with self.assertRaises(ValueError):
                    explain_delivery_with_model(model_config, facts)

    def test_config_rejects_token_sent_over_plain_http(self):
        with tempfile.TemporaryDirectory() as directory:
            smtp = Path(directory) / "smtp"
            webhook = Path(directory) / "webhook"
            smtp.write_text("secret")
            webhook.write_text("secret")
            env = {
                "SMTP_USER": "sender@example.com", "SMTP_SENDER": "sender@example.com",
                "SMTP_RECIPIENT": "developer@example.com", "SMTP_PASSWORD_FILE": str(smtp),
                "WEBHOOK_TOKEN_FILE": str(webhook), "INCIDENT_MODEL_URL": "http://model/v1/chat/completions",
                "INCIDENT_MODEL_NAME": "test", "INCIDENT_MODEL_KEY_FILE": str(smtp),
            }
            with patch.dict("os.environ", env, clear=True):
                with self.assertRaises(ValueError):
                    Config.from_env()


if __name__ == "__main__":
    unittest.main()
