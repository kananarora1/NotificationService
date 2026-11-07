from __future__ import annotations

import hmac
import json
import math
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
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


ALERT_NAME = "RedisRateLimiterFailOpen"
MAX_BODY_BYTES = 65536
MAX_LOG_BYTES = 32768
REDIS_ERROR = re.compile(r"redis.{0,120}(timeout|timed out|refused|connect|unavailable)|"
                         r"(timeout|timed out|refused|connect).{0,120}redis", re.IGNORECASE)


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


def read_text(url: str, *, token: str = "", timeout: float = 4, limit: int = 262144) -> str:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=timeout) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("upstream response too large")
    return data.decode("utf-8", errors="replace")


def prometheus_value(config: Config, expression: str) -> float | None:
    url = config.prometheus_url.rstrip("/") + "/api/v1/query?" + urlencode({"query": expression})
    result = json.loads(read_text(url))
    if result.get("status") != "success":
        raise ValueError("Prometheus query failed")
    series = result.get("data", {}).get("result", [])
    if not series:
        return None
    value = float(series[0]["value"][1])
    return value if math.isfinite(value) else None


def kubernetes_get(config: Config, path: str, *, limit: int = 262144) -> str:
    service_account = Path(config.service_account_dir)
    token = (service_account / "token").read_text(encoding="utf-8").strip()
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    context = ssl.create_default_context(cafile=str(service_account / "ca.crt"))
    url = f"https://{host}:{port}{path}"
    request = Request(url, headers={"Authorization": f"Bearer {token}"})
    with urlopen(request, timeout=4, context=context) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Kubernetes response too large")
    return data.decode("utf-8", errors="replace")


def collect_evidence(config: Config) -> dict:
    evidence = {"fail_open_5m": None, "app_up": None, "pods": [], "redis_error_lines_15m": 0, "gaps": []}
    for field, expression in (
        ("fail_open_5m", "sum(increase(ratelimit_fail_open_total[5m]))"),
        ("app_up", 'min(up{job="notification-service"})'),
    ):
        try:
            evidence[field] = prometheus_value(config, expression)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            evidence["gaps"].append(f"{field} metric unavailable")

    label_selector = quote("app.kubernetes.io/name=notification-service", safe="")
    path = f"/api/v1/namespaces/{quote(config.namespace, safe='')}/pods?labelSelector={label_selector}"
    try:
        pods = json.loads(kubernetes_get(config, path)).get("items", [])[:3]
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        evidence["gaps"].append("application pod data unavailable")
        return evidence

    for pod in pods:
        name = pod.get("metadata", {}).get("name", "")
        if not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", name):
            continue
        statuses = pod.get("status", {}).get("containerStatuses") or []
        app_status = next((item for item in statuses if item.get("name") == "notification-service"), {})
        evidence["pods"].append({
            "name": name,
            "phase": pod.get("status", {}).get("phase", "Unknown"),
            "ready": bool(app_status.get("ready", False)),
            "restarts": int(app_status.get("restartCount", 0)),
        })
        base = f"/api/v1/namespaces/{quote(config.namespace, safe='')}/pods/{quote(name, safe='')}/log"
        for previous in (False, True) if app_status.get("restartCount", 0) else (False,):
            query = urlencode({"container": "notification-service", "sinceSeconds": 900,
                               "tailLines": 200, "limitBytes": MAX_LOG_BYTES, "previous": str(previous).lower()})
            try:
                log = kubernetes_get(config, base + "?" + query, limit=MAX_LOG_BYTES)
                evidence["redis_error_lines_15m"] += sum(bool(REDIS_ERROR.search(line)) for line in log.splitlines())
            except (OSError, ValueError):
                evidence["gaps"].append(f"{'previous' if previous else 'current'} log unavailable for {name}")
    return evidence


def explain_with_model(config: Config, evidence: dict) -> str | None:
    if not config.model_url:
        return None
    facts = {
        "fail_open_5m": evidence["fail_open_5m"],
        "app_up": evidence["app_up"],
        "pods_ready": sum(pod["ready"] for pod in evidence["pods"]),
        "pods_total": len(evidence["pods"]),
        "redis_error_lines_15m": evidence["redis_error_lines_15m"],
        "gaps": evidence["gaps"],
    }
    evidence_fields = ["fail_open_5m", "app_up", "pods_ready", "pods_total", "redis_error_lines_15m"]
    schema = {
        "type": "object",
        "properties": {
            "focus": {"type": "string", "enum": ["redis_connectivity", "application_health", "insufficient_evidence"]},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "evidence_keys": {"type": "array", "items": {"type": "string", "enum": evidence_fields}},
        },
        "required": ["focus", "confidence", "evidence_keys"],
        "additionalProperties": False,
    }
    prompt = (
        "Classify the diagnostic focus for a Redis rate-limiter fail-open alert using only these counts. "
        "redis_connectivity means failed Redis calls or Redis connection errors in app logs. "
        "application_health means the app is down or pods are unready. Otherwise choose insufficient_evidence. "
        "Do not infer traffic spikes, Redis server status, or notification delivery. "
        "Choose at most two direct supporting evidence_keys, never healthy signals for a failure focus. "
        "Do not choose redis_connectivity when both fail_open_5m and redis_error_lines_15m are zero. "
        "Facts: " + json.dumps(facts)
    )
    payload = json.dumps({"model": config.model_name, "stream": False, "format": schema,
                          "options": {"temperature": 0, "num_predict": 80},
                          "messages": [{"role": "user", "content": prompt}]}).encode()
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if config.model_key_file:
        key = Path(config.model_key_file).read_text(encoding="utf-8").strip()
        headers["Authorization"] = f"Bearer {key}"
    request = Request(config.model_url, data=payload, headers=headers, method="POST")
    with urlopen(request, timeout=35) as response:
        answer = json.loads(response.read(16384))
    content = answer["message"]["content"]
    result = json.loads(content)
    focus = result.get("focus")
    confidence = result.get("confidence")
    evidence_keys = result.get("evidence_keys")
    supported_keys = {key for key, value in facts.items()
                      if key != "gaps" and value not in (None, 0, [], "")}
    if facts["app_up"] == 0:
        supported_keys.add("app_up")
    if facts["pods_total"] and facts["pods_ready"] == 0:
        supported_keys.add("pods_ready")
    if confidence not in ("low", "medium", "high"):
        raise ValueError("invalid model confidence")
    if not isinstance(evidence_keys, list) or not all(
            isinstance(key, str) and key in supported_keys for key in evidence_keys):
        raise ValueError("invalid model evidence keys")
    if focus == "redis_connectivity" and (
            (facts["fail_open_5m"] or 0) >= 3 or facts["redis_error_lines_15m"] > 0):
        evidence_keys = [key for key in evidence_keys
                         if key in ("fail_open_5m", "redis_error_lines_15m")]
        if not evidence_keys:
            raise ValueError("unsupported model evidence")
        hypothesis = "Failed Redis calls may be disrupting rate-limit checks; inspect the app-to-Redis path."
    elif focus == "application_health" and (
            facts["app_up"] == 0 or (facts["pods_total"] > 0 and
                                     facts["pods_ready"] < facts["pods_total"])):
        evidence_keys = [key for key in evidence_keys if
                         (key == "app_up" and facts["app_up"] == 0) or
                         (key == "pods_ready" and facts["pods_total"] > 0 and
                          facts["pods_ready"] < facts["pods_total"])]
        if not evidence_keys:
            raise ValueError("unsupported model evidence")
        hypothesis = "Application health may be contributing; inspect the affected pods."
    elif focus == "insufficient_evidence" and (facts["fail_open_5m"] or 0) < 3 and (
            facts["redis_error_lines_15m"] == 0 and facts["app_up"] != 0 and
            (facts["pods_total"] == 0 or facts["pods_ready"] == facts["pods_total"])):
        evidence_keys = []
        hypothesis = "The available evidence does not identify a likely cause."
    else:
        raise ValueError("unsupported model focus")
    return f"{hypothesis} (model self-rated confidence: {confidence}; evidence: {', '.join(evidence_keys) or 'none'}; unverified)"


def format_report(evidence: dict, model_hypothesis: str | None) -> str:
    fail_open = evidence["fail_open_5m"]
    app_up = evidence["app_up"]
    pods = evidence["pods"]
    lines = [
        "RelayNest incident analysis: RedisRateLimiterFailOpen",
        "",
        "Observed facts",
        f"- Rate-limit fail-open events in the last 5 minutes: {fail_open if fail_open is not None else 'unavailable'}",
        f"- Prometheus application up value: {app_up if app_up is not None else 'unavailable'}",
        f"- Application pods ready: {sum(p['ready'] for p in pods)}/{len(pods)}",
        f"- Recent log lines matching Redis connection/timeout errors: {evidence['redis_error_lines_15m']}",
    ]
    if evidence["gaps"]:
        lines.append("- Evidence gaps: " + "; ".join(evidence["gaps"]))
    lines.extend(["", "Assessment"])
    if fail_open is not None and fail_open >= 3:
        lines.append("- Rate limiting was bypassed after Redis operations failed; notification requests may still be accepted.")
    else:
        lines.append("- Current metrics do not confirm an ongoing fail-open condition; the alert may have recovered.")
    if evidence["redis_error_lines_15m"]:
        lines.append("- Application logs are consistent with a Redis connectivity problem; they do not prove Redis itself is down.")
    if model_hypothesis:
        lines.append("- AI-selected focus: " + model_hypothesis)
    lines.extend(["", "Next checks",
                  "- Check Redis health and the application-to-Redis network path.",
                  "- After recovery, verify the fail-open counter stops rising.",
                  "- Do not resend notifications or change delivery statuses based on this report."])
    return "\n".join(lines)


def send_email(config: Config, body: str) -> None:
    message = EmailMessage()
    message["From"] = config.smtp_sender
    message["To"] = config.smtp_recipient
    message["Subject"] = "RelayNest incident analysis: Redis rate limiter fail-open"
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
        self.completed: dict[str, float] = {}
        self.inflight: set[str] = set()
        self.counters = {"webhooks": 0, "reports_sent": 0, "reports_failed": 0, "model_fallbacks": 0}

    def accept(self, payload: dict) -> int:
        alerts = payload.get("alerts")
        if not isinstance(alerts, list):
            return 400
        relevant = [alert for alert in alerts if isinstance(alert, dict)
                    and alert.get("status") == "firing"
                    and isinstance(alert.get("labels"), dict)
                    and alert["labels"].get("alertname") == ALERT_NAME]
        if not relevant:
            return 204
        fingerprint = relevant[0].get("fingerprint")
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{8,64}", fingerprint):
            return 400
        now = time.monotonic()
        with self.lock:
            self.counters["webhooks"] += 1
            if now - self.completed.get(fingerprint, -1e9) < 14400:
                log_event("duplicate_ignored", fingerprint)
                return 200
            if fingerprint in self.inflight:
                return 503
            self.inflight.add(fingerprint)
        log_event("analysis_started", fingerprint)
        try:
            evidence = collect_evidence(self.config)
            hypothesis = None
            try:
                hypothesis = explain_with_model(self.config, evidence)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                with self.lock:
                    self.counters["model_fallbacks"] += 1
                log_event("model_fallback", fingerprint, error_type=type(error).__name__)
            send_email(self.config, format_report(evidence, hypothesis))
        except (OSError, ValueError, smtplib.SMTPException) as error:
            with self.lock:
                self.counters["reports_failed"] += 1
            log_event("report_failed", fingerprint, error_type=type(error).__name__)
            return 503
        finally:
            with self.lock:
                self.inflight.discard(fingerprint)
        with self.lock:
            self.completed[fingerprint] = time.monotonic()
            self.counters["reports_sent"] += 1
        log_event("report_sent", fingerprint, model_used=bool(hypothesis))
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
