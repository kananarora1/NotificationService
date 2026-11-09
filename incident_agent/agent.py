from __future__ import annotations

import hmac
import json
import os
import re
import smtplib
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .investigation import collect_evidence, derive_findings, format_report, prioritize_with_model

MAX_BODY_BYTES = 65536


@dataclass(frozen=True)
class Config:
    namespace: str
    prometheus_url: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_sender: str
    smtp_recipient: str
    smtp_password_file: str
    webhook_token_file: str
    model_url: str = ""
    model_name: str = ""
    model_key_file: str = ""
    service_account_dir: str = "/var/run/secrets/kubernetes.io/serviceaccount"

    @classmethod
    def from_env(cls) -> Config:
        config = cls(
            namespace=os.environ.get("INCIDENT_NAMESPACE", "notification-service"),
            prometheus_url=os.environ.get("PROMETHEUS_URL", "http://prometheus:9090"),
            smtp_host=os.environ.get("SMTP_HOST", "smtp.gmail.com"),
            smtp_port=int(os.environ.get("SMTP_PORT", "587")),
            smtp_user=os.environ["SMTP_USER"],
            smtp_sender=os.environ["SMTP_SENDER"],
            smtp_recipient=os.environ["SMTP_RECIPIENT"],
            smtp_password_file=os.environ["SMTP_PASSWORD_FILE"],
            webhook_token_file=os.environ["WEBHOOK_TOKEN_FILE"],
            model_url=os.environ.get("INCIDENT_MODEL_URL", ""),
            model_name=os.environ.get("INCIDENT_MODEL_NAME", ""),
            model_key_file=os.environ.get("INCIDENT_MODEL_KEY_FILE", ""),
        )
        if bool(config.model_url) != bool(config.model_name):
            raise ValueError("model URL and model name must be configured together")
        if config.model_key_file and urlparse(config.model_url).scheme != "https":
            raise ValueError("model API with a key must use HTTPS")
        for path in (config.smtp_password_file, config.webhook_token_file):
            if not Path(path).read_text(encoding="utf-8").strip():
                raise ValueError("configured secret file is empty")
        return config


def send_email(config: Config, body: str, alert_name: str) -> None:
    message = EmailMessage()
    message["From"] = config.smtp_sender
    message["To"] = config.smtp_recipient
    message["Subject"] = f"RelayNest incident analysis: {alert_name}"
    message.set_content(body)
    password = Path(config.smtp_password_file).read_text(encoding="utf-8").strip()
    with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=10) as client:
        client.starttls(context=ssl.create_default_context())
        client.login(config.smtp_user, password)
        client.send_message(message)


def log_event(event: str, fingerprint: str, **fields: object) -> None:
    print(json.dumps({"time": datetime.now(timezone.utc).isoformat(), "event": event,
                      "fingerprint": fingerprint, **fields}), flush=True)


class IncidentAgent:
    def __init__(self, config: Config):
        self.config = config
        self.lock = threading.Lock()
        self.completed: dict[tuple[tuple[str, str, str], ...], float] = {}
        self.inflight: set[tuple[tuple[str, str, str], ...]] = set()
        self.counters = {"webhooks": 0, "reports_sent": 0, "reports_failed": 0, "model_fallbacks": 0}

    def accept(self, payload: dict) -> int:
        alerts = payload.get("alerts")
        if not isinstance(alerts, list):
            return 400
        relevant = [alert for alert in alerts if isinstance(alert, dict)
                    and alert.get("status") == "firing"
                    and isinstance(alert.get("labels"), dict)
                    and isinstance(alert["labels"].get("alertname"), str)
                    and re.fullmatch(r"[A-Za-z_:][A-Za-z0-9_:]{0,79}", alert["labels"]["alertname"])]
        if not relevant:
            return 204
        alert_name = relevant[0]["labels"]["alertname"]
        episodes = []
        for current in relevant:
            if current["labels"]["alertname"] != alert_name:
                return 400
            fingerprint = current.get("fingerprint")
            starts_at = current.get("startsAt", "")
            if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{8,64}", fingerprint):
                return 400
            if not isinstance(starts_at, str) or len(starts_at) > 64:
                return 400
            if starts_at:
                parts = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
                                     starts_at)
                if not parts:
                    return 400
                normalized = parts[1] + ("." + parts[2][:6] if parts[2] else "")
                normalized += "+00:00" if parts[3] == "Z" else parts[3]
                try:
                    datetime.fromisoformat(normalized)
                except ValueError:
                    return 400
            episodes.append((alert_name, fingerprint, starts_at))
        fingerprint = episodes[0][1]
        key = tuple(sorted(set(episodes)))
        now = time.monotonic()
        with self.lock:
            self.counters["webhooks"] += 1
            self.completed = {item: finished for item, finished in self.completed.items()
                              if now - finished < 14400}
            if now - self.completed.get(key, -1e9) < 14400:
                log_event("duplicate_ignored", fingerprint, alert_name=alert_name)
                return 200
            if key in self.inflight:
                return 503
            self.inflight.add(key)
        log_event("analysis_started", fingerprint, alert_name=alert_name)
        try:
            evidence = collect_evidence(self.config)
            findings = derive_findings(evidence)
            priority = None
            try:
                priority = prioritize_with_model(self.config, alert_name, evidence, findings)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                with self.lock:
                    self.counters["model_fallbacks"] += 1
                log_event("model_fallback", fingerprint, alert_name=alert_name,
                          error_type=type(error).__name__)
            send_email(self.config, format_report(alert_name, evidence, findings, priority), alert_name)
        except (OSError, ValueError, smtplib.SMTPException) as error:
            with self.lock:
                self.counters["reports_failed"] += 1
            log_event("report_failed", fingerprint, alert_name=alert_name,
                      error_type=type(error).__name__)
            return 503
        finally:
            with self.lock:
                self.inflight.discard(key)
        with self.lock:
            self.completed[key] = time.monotonic()
            self.counters["reports_sent"] += 1
        log_event("report_sent", fingerprint, alert_name=alert_name, model_used=bool(priority))
        return 200

    def metrics(self) -> str:
        with self.lock:
            values = self.counters.copy()
        return "".join(f"incident_agent_{name}_total {value}\n" for name, value in values.items())


class Handler(BaseHTTPRequestHandler):
    agent: IncidentAgent

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.respond(200, b"ok\n", "text/plain")
        elif self.path == "/metrics":
            self.respond(200, self.agent.metrics().encode(), "text/plain; version=0.0.4")
        else:
            self.respond(404)

    def do_POST(self) -> None:
        if self.path != "/alerts":
            self.respond(404)
            return
        expected = Path(self.agent.config.webhook_token_file).read_text(encoding="utf-8").strip()
        supplied = self.headers.get("Authorization", "")
        if not hmac.compare_digest(supplied, f"Bearer {expected}"):
            self.respond(401)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.respond(400)
            return
        if size < 1 or size > MAX_BODY_BYTES:
            self.respond(413)
            return
        try:
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise ValueError("payload must be an object")
        except (ValueError, UnicodeDecodeError):
            self.respond(400)
            return
        self.respond(self.agent.accept(payload))

    def respond(self, status: int, body: bytes = b"", content_type: str = "text/plain") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


def main() -> None:
    config = Config.from_env()
    agent = IncidentAgent(config)
    handler = type("AgentHandler", (Handler,), {"agent": agent})
    server = ThreadingHTTPServer(("0.0.0.0", 8080), handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
