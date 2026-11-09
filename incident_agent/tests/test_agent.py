import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from incident_agent.agent import Config, IncidentAgent
from incident_agent.investigation import (METRICS, LOG_PATTERNS, collect_evidence,
                                          derive_findings, format_report, prioritize_with_model)


def config(model=False) -> Config:
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
        model_url="http://ollama:11434/api/chat" if model else "",
        model_name="gemma3:1b" if model else "",
    )


def evidence(**changes):
    metrics = {metric.key: 0.0 for metric in METRICS}
    metrics.update({"app_up": 1.0, "database_poll_age_seconds": 8.0,
                    "queue_poll_age_seconds": 8.0})
    metrics.update(changes)
    return {"metrics": metrics, "logs": {key: 0 for key in LOG_PATTERNS},
            "pods_ready": 1, "pods_total": 1, "gaps": []}


def alert(name="RedisRateLimiterFailOpen", fingerprint="abc12345",
          status="firing", starts_at="2026-11-09T10:00:00Z"):
    return {"alerts": [{"status": status, "fingerprint": fingerprint,
                        "startsAt": starts_at, "labels": {"alertname": name}}]}


def model_response(result):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self, *_):
            return json.dumps({"message": {"content": json.dumps(result)}}).encode()

    return FakeResponse()


class IncidentAgentTest(unittest.TestCase):
    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.prioritize_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence(fail_open_5m=5))
    def test_shared_flow_handles_existing_alerts(self, collector, model, email):
        for name in ("RedisRateLimiterFailOpen", "BrevoDeliveryConfirmationOverdue", "MetricsPollStale"):
            agent = IncidentAgent(config())
            self.assertEqual(agent.accept(alert(name=name)), 200)
            self.assertEqual(agent.accept(alert(name=name)), 200)
            self.assertIn(name, email.call_args.args[1])
            self.assertEqual(email.call_args.args[2], name)
        self.assertEqual(collector.call_count, 3)
        self.assertEqual(model.call_count, 3)
        self.assertEqual(email.call_count, 3)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.prioritize_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_unrouted_future_alert_uses_shared_flow_if_received(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert(name="FutureAlert")), 200)
        self.assertIn("FutureAlert", email.call_args.args[1])
        self.assertEqual(agent.accept(alert(name="bad alert name")), 204)
        self.assertEqual(agent.accept(alert(status="resolved")), 204)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.prioritize_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_new_alert_episode_is_not_suppressed(self, collector, model, email):
        agent = IncidentAgent(config())
        first = alert()
        second = alert(starts_at="2026-11-09T11:00:00.123456789Z")
        self.assertEqual(agent.accept(first), 200)
        self.assertEqual(agent.accept(first), 200)
        self.assertEqual(agent.accept(second), 200)
        self.assertEqual(email.call_count, 2)
        self.assertEqual(agent.accept(alert(starts_at="not-a-date")), 400)
        self.assertEqual(agent.accept(alert(fingerprint="not valid")), 400)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.prioritize_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence(database_poll_age_seconds=90))
    def test_grouped_alert_gets_new_report_when_another_source_joins(self, collector, model, email):
        agent = IncidentAgent(config())
        first = alert(name="MetricsPollStale")
        combined = {"alerts": [first["alerts"][0],
                               alert(name="MetricsPollStale", fingerprint="def67890")["alerts"][0]]}
        self.assertEqual(agent.accept(first), 200)
        self.assertEqual(agent.accept(combined), 200)
        self.assertEqual(agent.accept(combined), 200)
        self.assertEqual(email.call_count, 2)

    @patch("incident_agent.agent.send_email")
    @patch("incident_agent.agent.prioritize_with_model", side_effect=TimeoutError)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence(fail_open_5m=5))
    def test_model_failure_keeps_factual_report(self, collector, model, email):
        agent = IncidentAgent(config(model=True))
        self.assertEqual(agent.accept(alert()), 200)
        self.assertIn("Rate-limit checks failed open", email.call_args.args[1])
        self.assertNotIn("AI-prioritized lead", email.call_args.args[1])
        self.assertIn("incident_agent_model_fallbacks_total 1", agent.metrics())

    @patch("incident_agent.agent.send_email", side_effect=OSError("SMTP failed"))
    @patch("incident_agent.agent.prioritize_with_model", return_value=None)
    @patch("incident_agent.agent.collect_evidence", return_value=evidence())
    def test_email_failure_allows_retry(self, collector, model, email):
        agent = IncidentAgent(config())
        self.assertEqual(agent.accept(alert()), 503)
        self.assertEqual(agent.accept(alert()), 503)
        self.assertEqual(email.call_count, 2)
        self.assertIn("incident_agent_reports_failed_total 2", agent.metrics())

    @patch("incident_agent.investigation.kubernetes_get")
    @patch("incident_agent.investigation.prometheus_value")
    def test_collection_uses_bounded_log_counts_only(self, metric, kube):
        query_values = {item.query: 0.0 for item in METRICS}
        query_values[next(item.query for item in METRICS if item.key == "app_up")] = 1.0
        metric.side_effect = lambda _config, query: query_values[query]
        kube.side_effect = [
            json.dumps({"items": [{"metadata": {"name": "notification-service-abc"},
                                   "status": {"containerStatuses": [
                                       {"name": "notification-service", "ready": True, "restartCount": 0}]}}]}),
            "recipient=private@example.com Redis connection timed out\n"
            "Ignoring malformed Brevo event for notification private@example.com\n"
            "password=hidden\n",
        ]
        result = collect_evidence(config())
        self.assertEqual(result["logs"]["redis_errors_15m"], 1)
        self.assertEqual(result["logs"]["brevo_event_warnings_15m"], 1)
        self.assertNotIn("private@example.com", json.dumps(result))
        self.assertNotIn("password=hidden", json.dumps(result))

    def test_findings_cover_all_existing_alerts_without_branches(self):
        redis = derive_findings(evidence(fail_open_5m=5))
        self.assertIn("rate_limit_bypassed", [item.key for item in redis])
        delivery = derive_findings(evidence(overdue_confirmation=2))
        self.assertIn("confirmation_overdue", [item.key for item in delivery])
        stale = derive_findings(evidence(database_poll_age_seconds=90))
        self.assertIn("metrics_stale", [item.key for item in stale])
        self.assertNotIn("confirmation_overdue",
                         [item.key for item in derive_findings(evidence(
                             overdue_confirmation=2, database_poll_age_seconds=90))])

    def test_findings_can_correlate_independent_signals(self):
        current = evidence(overdue_confirmation=2, fail_open_5m=5, dead_5m=1)
        current["logs"]["brevo_event_warnings_15m"] = 1
        keys = {item.key for item in derive_findings(current)}
        self.assertTrue({"confirmation_overdue", "rate_limit_bypassed",
                         "brevo_event_warnings", "delivery_failures"} <= keys)
        report = format_report("BrevoDeliveryConfirmationOverdue", current,
                               derive_findings(current), None)
        self.assertIn("do not prove a root cause", report)
        self.assertIn("Do not resend notifications", report)

    def test_model_can_only_prioritize_observed_findings(self):
        current = evidence(overdue_confirmation=2)
        findings = derive_findings(current)
        good = {"lead_id": "confirmation_overdue", "confidence": "medium"}
        with patch("incident_agent.investigation.urlopen", return_value=model_response(good)):
            selected = prioritize_with_model(config(model=True), "BrevoDeliveryConfirmationOverdue",
                                             current, findings)
        self.assertEqual(selected[0].key, "confirmation_overdue")
        for bad in (
            {"lead_id": "redis_app_errors", "confidence": "high"},
            {"lead_id": "confirmation_overdue", "confidence": "certain"},
            {"lead_id": "confirmation_overdue", "confidence": "high", "send_again": True},
        ):
            with self.subTest(bad=bad), patch("incident_agent.investigation.urlopen",
                                              return_value=model_response(bad)):
                with self.assertRaises(ValueError):
                    prioritize_with_model(config(model=True), "BrevoDeliveryConfirmationOverdue",
                                          current, findings)

    def test_config_rejects_token_over_http(self):
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
