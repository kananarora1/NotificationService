from __future__ import annotations

import json
import math
import os
import re
import ssl
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from .agent import Config


MAX_LOG_BYTES = 32768


@dataclass(frozen=True)
class Metric:
    key: str
    label: str
    query: str
    optional: bool = False


METRICS = (
    Metric("app_up", "Application scrape up", 'min(up{job="notification-service"})'),
    Metric("fail_open_5m", "Rate-limit fail-open events in 5m", "sum(increase(ratelimit_fail_open_total[5m]))"),
    Metric("overdue_confirmation", "Delivery confirmations overdue by 30m", "max(notifications_overdue_confirmation)"),
    Metric("awaiting_confirmation", "Notifications awaiting confirmation", "max(notifications_awaiting_confirmation)"),
    Metric("database_poll_age_seconds", "Database snapshot age in seconds",
           'max(time() - metrics_poll_last_success_timestamp_seconds{source="database"})'),
    Metric("queue_poll_age_seconds", "Queue snapshot age in seconds",
           'max(time() - metrics_poll_last_success_timestamp_seconds{source="rabbitmq"})'),
    Metric("failed_attempt_5m", "Failed delivery attempts in 5m",
           "sum(increase(notifications_failed_attempt_total[5m]))", True),
    Metric("dead_5m", "Notifications marked dead in 5m",
           "sum(increase(notifications_dead_total[5m]))", True),
    Metric("stuck_sending", "Sending notifications past their lease", "max(notifications_stuck_sending)"),
    Metric("unpublished_pending", "Pending notifications not confirmed on broker",
           "max(notifications_unpublished_pending)"),
    Metric("parking_lot_depth", "Messages in the parking lot",
           'max(queue_depth{queue="notifications.parking-lot"})'),
)

LOG_PATTERNS = {
    "redis_errors_15m": re.compile(
        r"redis.{0,120}(timeout|timed out|refused|connect|unavailable)|"
        r"(timeout|timed out|refused|connect).{0,120}redis", re.IGNORECASE),
    "brevo_event_warnings_15m": re.compile(
        r"Ignoring malformed Brevo event|Ignoring Brevo event .* with invalid timestamp", re.IGNORECASE),
    "database_poll_errors_15m": re.compile(r"Failed to refresh the database gauges", re.IGNORECASE),
    "queue_poll_errors_15m": re.compile(r"Failed to refresh the queue depth gauges", re.IGNORECASE),
    "broker_publish_errors_15m": re.compile(
        r"Broker nacked notification|Broker could not route notification|Could not publish notification|"
        r"No broker confirm for notification|Confirm for notification .* failed", re.IGNORECASE),
}


@dataclass(frozen=True)
class Finding:
    key: str
    summary: str
    evidence_keys: tuple[str, ...]
    next_check: str


def read_text(url: str, *, timeout: float = 4, limit: int = 262144) -> str:
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=timeout) as response:
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
    request = Request(f"https://{host}:{port}{path}", headers={"Authorization": f"Bearer {token}"})
    with urlopen(request, timeout=4, context=context) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Kubernetes response too large")
    return data.decode("utf-8", errors="replace")


def collect_evidence(config: Config) -> dict:
    evidence = {
        "metrics": {metric.key: None for metric in METRICS},
        "logs": {key: 0 for key in LOG_PATTERNS},
        "pods_ready": None,
        "pods_total": None,
        "gaps": [],
    }
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {metric.key: pool.submit(prometheus_value, config, metric.query) for metric in METRICS}
        for metric in METRICS:
            try:
                evidence["metrics"][metric.key] = futures[metric.key].result()
                if evidence["metrics"][metric.key] is None and not metric.optional:
                    evidence["gaps"].append(f"{metric.key} metric absent")
            except (OSError, ValueError, KeyError, TypeError):
                evidence["gaps"].append(f"{metric.key} metric unavailable")

    label_selector = quote("app.kubernetes.io/name=notification-service", safe="")
    namespace = quote(config.namespace, safe="")
    path = f"/api/v1/namespaces/{namespace}/pods?labelSelector={label_selector}"
    try:
        pods = json.loads(kubernetes_get(config, path)).get("items", [])[:3]
    except (OSError, ValueError, KeyError, TypeError):
        evidence["gaps"].append("application pod data unavailable")
        return evidence

    evidence["pods_total"] = len(pods)
    evidence["pods_ready"] = 0
    for pod in pods:
        name = pod.get("metadata", {}).get("name", "")
        if not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", name):
            continue
        statuses = pod.get("status", {}).get("containerStatuses") or []
        app_status = next((item for item in statuses if item.get("name") == "notification-service"), {})
        evidence["pods_ready"] += bool(app_status.get("ready", False))
        base = f"/api/v1/namespaces/{namespace}/pods/{quote(name, safe='')}/log"
        for previous in (False, True) if app_status.get("restartCount", 0) else (False,):
            query = urlencode({"container": "notification-service", "sinceSeconds": 900,
                               "tailLines": 200, "limitBytes": MAX_LOG_BYTES, "previous": str(previous).lower()})
            try:
                log = kubernetes_get(config, base + "?" + query, limit=MAX_LOG_BYTES)
                for line in log.splitlines():
                    for key, pattern in LOG_PATTERNS.items():
                        evidence["logs"][key] += bool(pattern.search(line))
            except (OSError, ValueError):
                evidence["gaps"].append(f"{'previous' if previous else 'current'} log unavailable for {name}")
    return evidence


def derive_findings(evidence: dict) -> list[Finding]:
    metrics = evidence["metrics"]
    logs = evidence["logs"]
    findings = []
    app_down = metrics["app_up"] == 0
    pods_unready = (evidence["pods_total"] is not None and
                    (evidence["pods_total"] == 0 or evidence["pods_ready"] < evidence["pods_total"]))
    if app_down or pods_unready:
        keys = tuple(key for key, condition in (("app_up", app_down), ("pods_ready", pods_unready)) if condition)
        findings.append(Finding("application_unhealthy", "Application scrape or pod readiness is unhealthy.", keys,
                                "Inspect application pod events, restarts, readiness, and recent logs."))

    stale = tuple(key for key in ("database_poll_age_seconds", "queue_poll_age_seconds")
                  if metrics[key] is None or metrics[key] < 0 or metrics[key] > 60)
    if stale:
        findings.append(Finding("metrics_stale", "A database or queue metrics snapshot is stale or unavailable.", stale,
                                "Check the metrics poller and its database or RabbitMQ dependency."))
    if logs["database_poll_errors_15m"]:
        findings.append(Finding("database_poll_errors", "Application logs show database gauge refresh failures.",
                                ("database_poll_errors_15m",), "Check database connectivity and pool health."))
    if logs["queue_poll_errors_15m"]:
        findings.append(Finding("queue_poll_errors", "Application logs show queue gauge refresh failures.",
                                ("queue_poll_errors_15m",), "Check RabbitMQ connectivity and queue availability."))
    if metrics["fail_open_5m"] is not None and metrics["fail_open_5m"] >= 3:
        findings.append(Finding("rate_limit_bypassed", "Rate-limit checks failed open in the last five minutes.",
                                ("fail_open_5m",), "Inspect Redis health and the application-to-Redis path."))
    if logs["redis_errors_15m"]:
        findings.append(Finding("redis_app_errors", "Application logs contain Redis connection or timeout errors.",
                                ("redis_errors_15m",), "Check Redis connectivity; this alone does not prove Redis is down."))
    db_fresh = metrics["database_poll_age_seconds"] is not None and 0 <= metrics["database_poll_age_seconds"] <= 60
    if db_fresh and metrics["overdue_confirmation"] is not None and metrics["overdue_confirmation"] > 0:
        findings.append(Finding("confirmation_overdue", "Provider-accepted notifications lack a recorded delivery outcome after 30 minutes.",
                                ("overdue_confirmation", "database_poll_age_seconds"),
                                "Compare affected notification IDs with Brevo events and webhook processing."))
    if logs["brevo_event_warnings_15m"]:
        findings.append(Finding("brevo_event_warnings", "Application logs show malformed or invalid Brevo events.",
                                ("brevo_event_warnings_15m",),
                                "Inspect webhook validation; these logs are not linked to specific overdue notifications."))
    failed = tuple(key for key in ("failed_attempt_5m", "dead_5m")
                   if metrics[key] is not None and metrics[key] > 0)
    if failed:
        findings.append(Finding("delivery_failures", "Delivery failures were recorded in the last five minutes.", failed,
                                "Correlate notification IDs and provider responses before deciding whether to retry."))
    if logs["broker_publish_errors_15m"]:
        findings.append(Finding("broker_publish_errors", "Application logs show broker publishing or confirm errors.",
                                ("broker_publish_errors_15m",), "Inspect RabbitMQ connections, routing, and confirms."))
    queue_fresh = metrics["queue_poll_age_seconds"] is not None and 0 <= metrics["queue_poll_age_seconds"] <= 60
    if queue_fresh and metrics["parking_lot_depth"] is not None and metrics["parking_lot_depth"] > 0:
        findings.append(Finding("parked_messages", "Messages are waiting in the parking lot for manual review.",
                                ("parking_lot_depth", "queue_poll_age_seconds"),
                                "Inspect parked message IDs and the dead-letter path; do not replay blindly."))
    db_backlog = tuple(key for key in ("stuck_sending", "unpublished_pending")
                       if db_fresh and metrics[key] is not None and metrics[key] > 0)
    if db_backlog:
        findings.append(Finding("reconciliation_backlog", "Notifications need reconciliation.", db_backlog,
                                "Check lease recovery, broker confirms, and reconciler progress."))
    return findings


def prioritize_with_model(config: Config, alert_name: str, evidence: dict,
                          findings: list[Finding]) -> tuple[Finding, str] | None:
    if not config.model_url or not findings:
        return None
    choices = [finding.key for finding in findings]
    schema = {
        "type": "object",
        "properties": {
            "lead_id": {"type": "string", "enum": choices},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        },
        "required": ["lead_id", "confidence"],
        "additionalProperties": False,
    }
    values = {**evidence["metrics"], **evidence["logs"],
              "pods_ready": evidence["pods_ready"], "pods_total": evidence["pods_total"]}
    candidates = [{"id": item.key, "summary": item.summary,
                   "evidence": {key: values[key] for key in item.evidence_keys}} for item in findings]
    prompt = (
        "Choose the most useful diagnostic lead to investigate first for this alert. "
        "Candidates are observed symptoms, not proven root causes. Do not invent a cause. "
        + json.dumps({"alert": alert_name, "candidates": candidates})
    )
    payload = json.dumps({"model": config.model_name, "stream": False, "format": schema,
                          "options": {"temperature": 0, "num_predict": 50},
                          "messages": [{"role": "user", "content": prompt}]}).encode()
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if config.model_key_file:
        key = Path(config.model_key_file).read_text(encoding="utf-8").strip()
        headers["Authorization"] = f"Bearer {key}"
    with urlopen(Request(config.model_url, data=payload, headers=headers, method="POST"), timeout=55) as response:
        answer = json.loads(response.read(16384))
    result = json.loads(answer["message"]["content"])
    if (not isinstance(result, dict) or set(result) != {"lead_id", "confidence"}
            or result.get("confidence") not in ("low", "medium", "high")):
        raise ValueError("invalid model response")
    selected = next((item for item in findings if item.key == result.get("lead_id")), None)
    if selected is None:
        raise ValueError("unsupported model lead")
    return selected, result["confidence"]


def format_report(alert_name: str, evidence: dict, findings: list[Finding],
                  priority: tuple[Finding, str] | None) -> str:
    metrics = evidence["metrics"]
    logs = evidence["logs"]
    ready = evidence["pods_ready"]
    total = evidence["pods_total"]
    lines = [f"RelayNest incident analysis: {alert_name}", "", "Observed facts"]
    lines.extend(f"- {metric.label}: {metrics[metric.key] if metrics[metric.key] is not None else 'unavailable'}"
                 for metric in METRICS)
    lines.append(f"- Application pods ready: {ready}/{total}" if total is not None else
                 "- Application pods ready: unavailable")
    lines.extend(f"- {key.replace('_', ' ')}: {count} matching log lines in 15m" for key, count in logs.items())
    if evidence["gaps"]:
        lines.append("- Evidence gaps: " + "; ".join(evidence["gaps"]))
    lines.extend(["", "Diagnostic leads"])
    if findings:
        lines.extend(f"- {item.summary} Evidence: {', '.join(item.evidence_keys)}." for item in findings)
    else:
        lines.append("- Current evidence does not confirm an ongoing failure; the alert may have recovered.")
    if priority:
        lines.append(f"- AI-prioritized lead (unverified, self-rated {priority[1]} confidence): "
                     f"{priority[0].summary} Evidence: {', '.join(priority[0].evidence_keys)}.")
    lines.extend(["", "Next checks"])
    checks = [priority[0].next_check] if priority else []
    checks.extend(item.next_check for item in findings if item.next_check not in checks)
    lines.extend(f"- {check}" for check in checks[:3])
    if not checks:
        lines.append("- Compare the alert timeline with fresh metrics and application logs.")
    lines.extend(["", "Limits", "- Alert symptoms and log counts do not prove a root cause or that a notification was not delivered.",
                  "- Do not resend notifications or change delivery statuses based on this report."])
    return "\n".join(lines)
