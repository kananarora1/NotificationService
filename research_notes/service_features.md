# NotificationService — production feature research

## Recommendation

Make delivery truth and the observability foundation the next release. An LLM “observability agent” is valuable **after**, not before, the service has correlated traces/logs/metrics, redacted telemetry, SLO-based alerts, stateful delivery data, and runbooks. Otherwise it will turn incomplete and possibly sensitive logs into plausible but untrustworthy diagnoses.

## Reliability and security work that must precede expansion

1. Remove deployable development credentials and seeded keys; use secret-backed production configuration, private/authenticated management endpoints, tenant/object authorization, and `NOT NULL` / foreign-keyed notification tenant ownership.
2. Bind idempotency keys to a canonical request hash. Persist notification state and an `outbox_event` in the same Postgres transaction. A publisher claims outbox rows and only records them as published after RabbitMQ publisher confirm.
3. Use durable/persistent queues, manual acknowledgements, bounded prefetch, retry attempt limits/delays, failure classification, DLQ, and an audited replay path. RabbitMQ says confirms and consumer acknowledgements are essential to data safety; acknowledge only after the work has succeeded ([reliability](https://www.rabbitmq.com/docs/reliability), [confirms](https://www.rabbitmq.com/docs/confirms)). Its default DLX forwarding has no publisher confirm, so clustered dead-letter safety needs quorum at-least-once DLX or the Postgres outbox as authority ([DLX safety](https://www.rabbitmq.com/docs/next/dlx)).
4. Add tenant quotas for request/recipient/payload/scheduled volume/provider spend and make Redis failure behaviour explicit per environment. Notification sends are paid resources, and OWASP lists unrestricted resource consumption and abuse of sensitive business flows among API risks ([OWASP API Security Top 10](https://api-security.owasp.org/)).
5. Fix deterministic listener/scheduler shutdown, migration-upgrade tests, backup/restore checks, least-privilege DB/broker accounts, encrypted PII, and retention/deletion policies.

## Prioritised feature roadmap

### 1. Delivery status API and provider callbacks

**Customer value:** customers can distinguish accepted/queued/dispatched/provider-accepted/delivered/failed/expired/cancelled instead of treating HTTP 202 as delivery.

**Build:** current-state row plus immutable `notification_event` and `delivery_attempt` ledger. Store provider message ID, error category/code, timestamps and attempt number. Receive signed provider callback events, deduplicate by provider event ID, and accept only valid monotonic state transitions. Expose `GET /v1/notifications/{id}` and a tenant-scoped attempt history.

**Why first:** Twilio is a concrete example: status callbacks supply lifecycle changes including sent, delivered, failed and undelivered; it recommends callbacks over polling at scale ([lifecycle](https://www.twilio.com/docs/messaging/guides/outbound-message-status-in-status-callbacks), [status logging](https://www.twilio.com/docs/messaging/guides/outbound-message-logging)). An initial provider acceptance is not delivery.

**Dependencies:** attempt/state model, provider adapter, callback signature verification, outbox/inbox idempotency, tenant access checks.

### 2. Customer callback webhooks

**Customer value:** an integrator can react to delivery/failure in its own order, support or identity workflow without polling.

**Build:** `webhook_endpoint` and `webhook_delivery` tables; an outbox-driven webhook worker; versioned HMAC signature and timestamp; retry/backoff/DLQ; delivery log, secret rotation, test-event endpoint and admin-only registration. Validate targets against SSRF—HTTPS only, deny private/link-local IPs, no credential forwarding, strict redirects/timeouts/response sizes.

**Dependencies:** status events first; share outbox/attempt mechanics but keep webhook-delivery state separate from provider-dispatch state.

### 3. Versioned templates, localisation and preview

**Customer value:** customers reuse safe transactional content and change approved copy without redeploying their own service.

**Build:** tenant-scoped template with immutable versions, locale/channel content, a small typed variable schema, preview, render-time validation, length/channel validation, and stored template version on every notification. Escape by channel and do not use arbitrary template code.

**Dependencies:** roles/audit, content redaction/retention, status. Start with one channel and limited syntax.

### 4. Preferences, consent, quiet hours and opt-out

**Customer value:** reduces unwanted contact and cost, while supporting consent policies.

**Build:** recipient/channel/category/locale/time-zone preferences, consent source/time/version, suppression records, a signed unsubscribe route where supported, and checks both before enqueue and immediately before dispatch. Report `SUPPRESSED_PREFERENCE` as a separate terminal state.

**Dependencies:** PII lifecycle and tenant-controller decisions, templates/categories. Deliver before promotional or bulk work.

### 5. Scheduling, cancellation and expiry

**Customer value:** reminders and timed messages survive restarts; OTPs can expire.

**Build:** durable Postgres `scheduled_at` / `expires_at` (UTC and supplied IANA zone); clustered scheduler claims due rows with row locks/`SKIP LOCKED`, writes the outbox transactionally, and reconciliation repairs missed jobs. Cancellation must win only before an attempt is claimed. Cap horizon and tenant volume.

**Dependencies:** durable outbox, state machine, clock/zone test suite, scheduler-lag metrics. Do not use only in-memory local scheduling.

### 6. Provider abstraction, health-aware routing and controlled failover

**Customer value:** isolates provider outages/cost changes and permits channel growth without changing the public API.

**Build:** small `ProviderAdapter` (`submit`, `parseCallback`, `getStatus`) with capability metadata. Version tenant/channel routing policies by country, class, cost ceiling, health and compliance. Persist provider choice/idempotency token before submit; circuit-break provider calls. Never blindly retry an already accepted send through a second provider, because duplicates are usually worse than delay.

**Dependencies:** end-to-end status, callbacks, attempt ledger, quota and credentials. Start with one real provider and a fake adapter; add a second only after duplicate/failure behaviour is measured.

### 7. DLQ inspection and safe replay

**Customer value:** restores operationally failed jobs with accountability.

**Build:** an admin/tenant-role-gated failure view and a replay action that preserves original attempt, records operator/reason/time, sets `replay_of_attempt_id`, and resubmits through the outbox. Bulk replay requires dry-run, filters, rate limits and approval. A broker DLQ alone is not a product-facing console.

**Dependencies:** attempt ledger, audit roles and PII controls.

## Observability agent plan

### Instrument first

- Structured events with event name, severity, `trace_id`, `span_id`, notification ID, pseudonymised tenant/recipient ID, provider/channel, state transition, attempt, error class and duration.
- Propagated W3C trace context through HTTP → AMQP headers → worker/provider callback; traces for DB, outbox and provider calls.
- Bounded-label metrics: accepted/rejected; transitions/attempts; dispatch/provider latency; delivery ratio; queue depth and oldest age; retry/DLQ/outbox/scheduler lag; provider/circuit health; Redis fallback; quota drops.
- Redaction before telemetry export: no bodies, API keys, auth headers or raw recipient addresses. OpenTelemetry specifies trace/span/resource correlation in logs ([Logs](https://opentelemetry.io/docs/specs/otel/logs/), [data model](https://opentelemetry.io/docs/specs/otel/logs/data-model/)) and warns URL telemetry can contain sensitive data ([URL conventions](https://opentelemetry.io/docs/specs/otel/semconv/url/)).
- SLO alerts and linked runbooks. Google SRE says alerts should be timely, user-symptom based and actionable ([incident guidance](https://sre.google/resources/practices-and-processes/incident-management-guide/)); page only for immediate human action, otherwise create a ticket or dashboard signal ([practical alerting](https://sre.google/sre-book/practical-alerting/)).

### Agent v1: read-only evidence pack

Trigger on an already grouped alert or explicit developer request. Give it fixed, read-only and query-capped access to alert/SLO context, metric windows, trace lookup, redacted log searches, queue/outbox/DLQ summaries, deployment metadata and curated runbooks. Report: impact, time line, correlated changes, ranked hypotheses with supporting evidence links, next safe checks, confidence and unknowns. It must not assert root cause without deterministic evidence.

Guardrails: dedicated service account; no shell, write database, secrets, raw PII/message bodies, unrestricted log search, provider credentials or production mutation. Treat log text as untrusted/prompt-injectable. Redact before model access; audit every query/output; test seeded incidents and malicious logs; human approval for all action. The agent can recommend, never execute, replay/failover/configuration changes.

Agent v2, only after measured usefulness/redaction/cost/time-to-triage, may draft an incident ticket and suggest approved runbook steps. Free-form automated remediation should not be an extension of this agent.

## 10–14 week small-team sequencing

1. **Weeks 1–3:** core hardening, request-hash idempotency, outbox/confirms/manual-ack/retry/DLQ, attempt ledger, lifecycle/migration/backup tests. Exit: a crash cannot silently lose a committed request or duplicate a mismatched retry.
2. **Weeks 4–5:** provider adapter, verified provider callbacks, status API, SLO dashboards/runbooks. Exit: support can explain every notification state with evidence.
3. **Weeks 6–7:** OpenTelemetry/redaction/grouped alerts and agent v1/tabletop tests. Exit: triage becomes faster without sensitive access or production changes.
4. **Weeks 8–10:** customer callback webhooks and template v1. Exit: an integrator reliably receives delivery events and tenants safely maintain transactional content.
5. **Weeks 11–14:** scheduling/expiry/cancel and preferences; provider routing behind a feature flag. Exit: scheduled sends survive restart and preferences are applied at enqueue and dispatch.

## Architecture mapping

- **Postgres:** notification current state, immutable attempts/events, outbox/inbox, templates, preferences, webhook delivery, scheduler claims, audit history.
- **RabbitMQ:** persistent streams for provider dispatch, customer webhook delivery, retry and DLQ; publisher confirms, manual ack and bounded prefetch.
- **Redis:** quotas/rate limits, short-lived idempotency optimisation (not authority), shared health/circuit hints; define degradation when unavailable.
- **Spring:** transactional state/outbox writes; DB-claimed durable jobs around `@Scheduled`; timeouts/circuit breakers for providers; Micrometer/OpenTelemetry instrumentation.

## Explicitly defer

Campaign builders, visual workflow engines, arbitrary template scripting, multi-region active-active, and automatic cross-provider resend. They amplify consent, cost, duplicate-delivery and incident risk. Revisit bulk/broadcast only after preferences, quotas, templates, scheduling and delivery truth are proven.
