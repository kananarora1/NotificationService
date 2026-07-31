# Notification Service (v8)

An **asynchronous** notification service backed by **RabbitMQ**, with **Postgres**
as the source of truth. `POST /notify` saves the notification as `PENDING`,
publishes its id to a durable queue, and returns `202 Accepted` immediately. A
pool of `@RabbitListener` consumers drains the queue and performs delivery, with
**retries + backoff** and a **dead-letter queue** for permanent failures (v4),
**idempotency** for an at-most-once send (v5), **tenant identity via API keys**
(v6), and a **Redis-backed distributed rate limiter** (v7).

v8 adds **observability** — no new features, just visibility into what already
exists. The app exports Micrometer metrics (custom business counters/timers/gauges
plus the auto-provided HTTP/JVM/DB/Rabbit metrics) on a Prometheus endpoint, and the
stack ships an auto-provisioned **Prometheus + Grafana** with a ready-made dashboard.
See [Observability](#observability-v8).

## Stack

- Java 21, Spring Boot 3.3 (Spring Web, Spring Data JPA, Validation, AMQP, Data Redis, Actuator)
- PostgreSQL — source of truth (notifications + tenants)
- RabbitMQ — message broker (with retries + dead-letter queue)
- Redis — shared store for the distributed rate limiter
- Micrometer + Prometheus + Grafana — metrics and dashboards
- Maven

## Domain

A `Notification` has: `id` (UUID), `tenantId` (the API-key tenant that created it),
`recipient`, `channel` (`EMAIL`/`SMS`/`PUSH`), `message`, `status`, `attempts` (int),
a nullable `failureReason`, a nullable and **UNIQUE** `idempotencyKey`, `createdAt`,
and a nullable `sentAt`. A `Tenant` has: `id`, `name`, unique `apiKey`, and
`rateLimitPerMinute`.

`status` is one of:

| status    | meaning                                                              |
|-----------|---------------------------------------------------------------------|
| `PENDING` | accepted, awaiting (or undergoing) delivery — including retries      |
| `SENT`    | delivered successfully                                               |
| `DEAD`    | permanently failed: retries exhausted and the message dead-lettered  |
| `FAILED`  | legacy single-attempt failure; retained but no longer set in v4      |

Delivery is handled by `SimulatedProvider`: it sleeps ~800ms to mimic a slow
external API, logs `sent to {recipient} via {channel}`, and returns success. It
does **not** do any real delivery (no SMTP/SMS gateway). Two recipients drive the
failure paths so they are testable on demand:

- **`fail@example.com`** — always throws (permanent failure → ends `DEAD` after
  retries are exhausted).
- **`flaky@example.com`** — throws on the first two attempts for a given
  notification, then succeeds (transient failure → recovers to `SENT` via retry).

## Architecture (v4)

```
POST /notify ─► save PENDING ─► publish id ─► 202 {id, PENDING}
                                    │
                                    ▼
                  RabbitMQ durable queue "notifications.send"
                                    │   (persistent messages)
                 ┌──────────────────┼──────────────────┐
              consumer           consumer            consumer ...   (@RabbitListener,
                 │                                                   concurrency = 4)
        receive id ─► attempt++ ─► provider.send()
                 │                       │
            success                   failure
                 │                       │
            SENT + sentAt        retry with backoff (1s, 2s, 4s ... up to 4 attempts)
                                         │
                                  retries exhausted → reject (no requeue)
                                         │  via x-dead-letter-exchange
                                         ▼
                      RabbitMQ durable queue "notifications.dlq"
                                         │
                              DeadLetterListener ─► status DEAD + failureReason
```

- **Work queue** — `RabbitConfig` declares `notifications.send` (`durable`),
  configured with `x-dead-letter-exchange` → `notifications.dlx` so rejected
  messages are dead-lettered.
- **Dead-letter topology** — a durable direct exchange `notifications.dlx` and a
  durable queue `notifications.dlq` bound to it.
- **Producer** — `NotificationPublisher` publishes the id via `RabbitTemplate`
  (persistent delivery). `NotificationService.create()` saves the row, then publishes.
- **Consumer** — `NotificationListener` calls `attemptDelivery(id)`, which
  increments `attempts`, calls the provider, and marks `SENT` on success. On a
  provider exception it **rethrows** (status stays `PENDING`) so the retry
  interceptor backs off and retries.
- **Retry/backoff** — configured under `spring.rabbitmq.listener.simple.retry.*`
  (see below). When `max-attempts` is reached, the message is rejected **without
  requeue** and RabbitMQ routes it to the DLQ.
- **DLQ consumer** — `DeadLetterListener` listens on `notifications.dlq`, marks the
  row `DEAD`, records a `failureReason`, and logs the id and reason.

### Retry configuration (`application.yml`)

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        concurrency: 4
        prefetch: 1
        acknowledge-mode: auto
        default-requeue-rejected: false   # exhausted retries are dead-lettered, not requeued
        retry:
          enabled: true
          max-attempts: 4
          initial-interval: 1000   # 1s before the first retry
          multiplier: 2.0          # then 2s, 4s, ...
          max-interval: 10000      # capped at 10s
```

This is Spring AMQP's stateless retry: the listener thread backs off and re-invokes
the consumer in-process (no broker redelivery between attempts). After the 4th
attempt fails, Spring's default recoverer throws `AmqpRejectAndDontRequeueException`,
the message is rejected without requeue, and the queue's dead-letter exchange
routes it to `notifications.dlq`.

## Idempotency

v5 guarantees an **at-most-once send** by defending against two independent
duplicate sources.

### Caller-level: the `Idempotency-Key` header

Clients retry POSTs (timeouts, network blips, at-least-once upstreams). To make a
retry safe, send a stable **`Idempotency-Key`** header — typically derived from the
business event (e.g. `order-4711-receipt`):

- **Header present, key unseen** → create the row (storing the key), publish, return
  `202 Accepted`.
- **Header present, key seen** → return the **existing** `{ id, status }` with
  `200 OK`. No second row, no second publish.
- **Header absent** → always create (unchanged v4 behaviour), `202 Accepted`.

**Why a UNIQUE constraint, not an app-level check.** A naive
`findByKey()`-then-`insert` has a time-of-check/time-of-use (TOCTOU) gap: two
identical requests can both find "nothing" and both insert. The race-safe design
puts a **`UNIQUE` constraint on `idempotency_key`** and lets the database be the
single arbiter:

```
request A ─┐                       request B ─┐
           ▼                                  ▼
   findByKey() → empty               findByKey() → empty
           ▼                                  ▼
   INSERT (key) ── commits ──► wins   INSERT (key) ──► UNIQUE violation
                                              ▼
                                    catch DataIntegrityViolation,
                                    re-fetch by key → return A's row
```

The pre-check is just a fast path; **correctness comes from the constraint**. On
violation the loser catches `DataIntegrityViolationException`, re-fetches the
winner's row, and returns it — so concurrent duplicates collapse to exactly one
row. (`saveAndFlush` forces the INSERT immediately so the violation surfaces inside
the `try`, not later at commit.)

### Processing-level: redelivery-safe consumer

RabbitMQ is at-least-once: a message can be redelivered (consumer crash before ack,
connection drop, etc.). Before calling the provider, the consumer checks the row's
status:

- status `PENDING` → deliver (and on success mark `SENT`).
- status `SENT` or `DEAD` → **skip the provider call**, log
  `skipping already-processed {id}`, and ack.

So even if the same message is processed twice, the slow `provider.send()` runs at
most once. Postgres remains the source of truth for "has this already been sent".

## Tenants, API keys & rate limiting (v6)

### Identity: the `X-API-Key` header

`POST /notify` requires an **`X-API-Key`** header. The key is resolved to a
**tenant** (table `tenants`: `id`, `name`, unique `apiKey`, `rateLimitPerMinute`).
A missing or unknown key returns **`401 Unauthorized`** before the controller runs
(enforced by a `HandlerInterceptor`). The resolving tenant's id is stored on every
notification it creates (`tenant_id`). `GET /notify/{id}` is **not** gated.

Tenants are seeded at startup (idempotently, safe across instances):

| tenant   | apiKey         | rateLimitPerMinute |
|----------|----------------|--------------------|
| acme     | `key-acme`     | 60                 |
| globex   | `key-globex`   | 10                 |
| initech  | `key-initech`  | 5                  |

## Rate limiting: distributed (v7)

After identity, a second interceptor enforces the tenant's per-minute budget. In v7
the budget lives in **Redis** (not per-JVM memory), so it is shared across every app
instance.

```
POST /notify ─► [X-API-Key?] ──no/unknown──► 401
                     │ tenant
                     ▼
        [Redis EVAL token-bucket: allow?] ──no──► 429 + Retry-After
                     │ yes (atomically consumed 1)
                     ▼
              create / dedupe ─► 202 or 200
```

Each accepted `POST` consumes one token; an empty bucket returns **`429 Too Many
Requests`** with a **`Retry-After`** header (seconds until the next token). Buckets
are isolated per tenant — exhausting `globex` does not affect `acme`.

### Algorithm: token bucket in a Lua script

A **token bucket** (capacity = `rateLimitPerMinute`, refilling continuously at
`limit / 60s`), stored per tenant under key **`ratelimit:{tenantId}`** as a small
Redis hash (`tokens`, `ts`). The whole decision runs as one **Lua script via `EVAL`**
(`src/main/resources/ratelimit/token_bucket.lua`): read → refill by elapsed time →
check → decrement → write, all atomically.

Chosen over a fixed-window `INCR` counter because the token bucket **smooths bursts**
(a fixed window admits up to *2× limit* across a window boundary — the last requests
of one window plus the first of the next). The cost is a slightly more involved
script; both are atomic and correct, this one just has nicer burst behaviour.

### Why it must be atomic (not GET-then-SET)

The correctness of a distributed limiter hinges on the read-modify-write being a
**single atomic operation**. If Java did `GET count` → decide → `SET count`, two
instances could both read "1 token left", both decide "allowed", and both write —
admitting two requests against a budget of one. That is the exact **TOCTOU race**
this feature exists to prevent (it's the same class of bug as v5's idempotency
race, solved there by a UNIQUE constraint). Redis executes each `EVAL` script
atomically (single-threaded command execution), so the check-and-decrement cannot
interleave across instances.

### Redis-down policy: fail-open

If the Redis call throws (broker down, network partition), the limiter **fails
open** — it logs a warning and *allows* the request. For a notification system,
continuing to deliver is preferred over a hard outage caused by the rate limiter
itself. This is a deliberate choice; flip `RateLimiter.FAIL_OPEN` to `false` for
fail-closed (reject on Redis errors) if your risk tolerance differs.

### It now holds across instances

Run two instances against the same Postgres + RabbitMQ + Redis:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
# globex (limit 10) now admits 10/min TOTAL across both instances — not 10 each.
```

This is the property that failed in v6 and is pinned by the
`limitHoldsAcrossTwoInstancesSharingOneRedis` test.

## Observability (v8)

The app is instrumented with [Micrometer](https://micrometer.io/) and exposes
Actuator endpoints:

| endpoint | purpose |
|---|---|
| `GET /actuator/health` | liveness/readiness |
| `GET /actuator/metrics` | browse individual metrics |
| `GET /actuator/prometheus` | Prometheus scrape endpoint (text exposition) |

### Metrics

Auto-provided by Spring Boot (verified in the scrape): `http_server_requests_seconds`
(latency histogram + status counts), `jvm_*`, `hikaricp_connections*`, and the
process/system metrics.

Custom business metrics added in v8:

| metric | type | tags | meaning |
|---|---|---|---|
| `notifications.submitted` | counter | tenant, channel | notification accepted & created |
| `notifications.sent` | counter | tenant, channel | delivered successfully |
| `notifications.failed_attempt` | counter | tenant | a delivery attempt failed (fires on **every** attempt, incl. retries) |
| `notifications.dead` | counter | tenant | dead-lettered (retries exhausted) |
| `ratelimit.rejected` | counter | tenant | request rejected with 429 |
| `notifications.delivery.latency` | timer | — | duration of a `provider.send()` call |
| `notifications.end_to_end.latency` | timer | — | accepted (`createdAt`) → sent (`sentAt`) |
| `notifications.pending` | gauge | — | rows in PENDING status |
| `queue.depth` | gauge | queue | RabbitMQ messages in `notifications.send` / `.dlq` |

The two gauges are refreshed by a scheduled poller every 5s and cached, so a
Prometheus scrape never triggers a `COUNT(*)` on a large table or a broker
round-trip. Per-tenant metrics are tagged with the tenant **name** (e.g. `acme`).

### Grafana dashboard

`docker compose up -d` also starts **Prometheus** (scrapes the app every 5s) and
**Grafana**, auto-provisioned with the Prometheus datasource **and** a dashboard —
no manual clicking needed.

- **Grafana:** http://localhost:3000 — login **admin / admin**. Open the
  **Notification Service** dashboard.
- **Prometheus:** http://localhost:9090 (e.g. Status → Targets).

Prometheus scrapes the app on the host at `host.docker.internal:8080` and `:8081`
(the second is DOWN until you start a second instance — see the rate-limiter
section). Panels:

| panel | what it tells you |
|---|---|
| POST /notify request rate by status | traffic and the mix of 202/200/429/401/400 |
| POST /notify latency p50/p95/p99 | request-handling latency (should be ms — POST doesn't block on delivery) |
| Throughput: submitted vs sent | intake vs delivery rate; a persistent gap = growing backlog |
| Pending backlog | PENDING rows over time — watch it drain |
| Queue depth main & DLQ | RabbitMQ readiness; DLQ climbing = permanent failures |
| Failed attempts & dead-lettered | retry churn and give-ups |
| **429 rate by tenant** | the **noisy-neighbor** view — who is hitting their limit |
| Delivery latency (provider) heatmap | distribution of `provider.send()` time (~0.8s band) |
| End-to-end latency p50/p95/p99 | accepted → actually sent, incl. queue wait + retries |
| JVM heap / HikariCP active | runtime health and DB pool pressure |

### Drive load with k6

[`k6/load.js`](k6/load.js) simulates three tenants so the graphs show a readable
shape, ramping over ~4 minutes:

- **acme** (limit 60/min) — steady, well-behaved, stays under budget.
- **globex** (limit 10/min) — periodic bursts over budget → 429 spikes.
- **initech** (limit 5/min) — sustained flood far above budget → constant 429 (the
  noisy neighbor).

```bash
# install k6 (macOS): brew install k6
k6 run k6/load.js
# against a second instance: BASE_URL=http://localhost:8081 k6 run k6/load.js
```

Watch the **429 rate by tenant** panel light up for globex/initech while acme stays
green.

## 1. Start the stack (Postgres + RabbitMQ + Redis + Prometheus + Grafana)

```bash
docker compose up -d
```

This starts:

- **Postgres 16** on `localhost:5433` (host `5433` → container `5432`, to avoid
  colliding with a native Postgres on `5432`). DB/user/password are all
  `notifications`.
- **RabbitMQ 3.13** with the management plugin: AMQP on `localhost:5672`, and the
  management UI on **http://localhost:15672** (login **guest / guest**).
- **Redis 7** on `localhost:6379` — the shared store for the distributed rate
  limiter. Inspect a bucket with
  `docker exec notification-redis redis-cli hgetall "ratelimit:<tenantId>"`.
- **Prometheus** on **http://localhost:9090** — scrapes the app every 5s.
- **Grafana** on **http://localhost:3000** (**admin / admin**) — pre-loaded with the
  Notification Service dashboard.

> **Upgrading an existing broker:** v4 adds dead-letter arguments to the
> `notifications.send` queue. RabbitMQ refuses to redeclare an existing queue with
> different arguments, so if you ran v3 against a persistent broker, delete the old
> queue first: `docker exec notification-rabbitmq rabbitmqctl delete_queue notifications.send`.

## 2. Run the app

```bash
./mvnw spring-boot:run
```

The app listens on `http://localhost:8080`. The DB schema is created/updated
automatically (`spring.jpa.hibernate.ddl-auto=update`), and the queues/exchanges
are declared on startup.

## 3. Try the endpoints

### POST /notify

Body: `{ recipient, channel, message }`. **Requires** an `X-API-Key` header
(see [Tenants & rate limiting](#tenants-api-keys--rate-limiting-v6)). Saves as
`PENDING`, publishes to `notifications.send`, and returns `202 Accepted` with
`{ id, status: "PENDING" }` **immediately**.

- Missing / unknown `X-API-Key` → `401 Unauthorized`.
- Over the tenant's per-minute budget → `429 Too Many Requests` + `Retry-After`.
- Blank fields or an invalid channel → `400`.
- Optional **`Idempotency-Key`** header (see [Idempotency](#idempotency)): a repeat
  with the same key returns `200 OK` with the original `{ id, status }`.

```bash
# normal — succeeds on the first attempt
curl -i -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: key-acme' \
  -d '{"recipient":"alice@example.com","channel":"EMAIL","message":"Hello!"}'

# missing key -> 401
curl -i -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"alice@example.com","channel":"EMAIL","message":"no key"}'

# hammer globex (limit 10/min) to see 429 + Retry-After
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code} " -X POST http://localhost:8080/notify \
    -H 'Content-Type: application/json' -H 'X-API-Key: key-globex' \
    -d '{"recipient":"a@example.com","channel":"EMAIL","message":"burst"}'
done; echo   # => 202 ×10 then 429 ×2

# idempotent — run twice: first 202 (new), second 200 (same id)
curl -i -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: key-acme' \
  -H 'Idempotency-Key: order-4711-receipt' \
  -d '{"recipient":"alice@example.com","channel":"EMAIL","message":"Hello!"}'

# transient failure — fails twice then recovers to SENT
curl -s -X POST http://localhost:8080/notify -H 'Content-Type: application/json' \
  -H 'X-API-Key: key-acme' \
  -d '{"recipient":"flaky@example.com","channel":"EMAIL","message":"retry me"}'

# permanent failure — ends DEAD and lands in notifications.dlq
curl -s -X POST http://localhost:8080/notify -H 'Content-Type: application/json' \
  -H 'X-API-Key: key-acme' \
  -d '{"recipient":"fail@example.com","channel":"EMAIL","message":"doomed"}'
```

### GET /notify/{id}

Returns the current state, or `404` if the id is unknown. Poll it to watch
`PENDING → SENT` (or, for `fail@example.com`, `PENDING → DEAD` once retries are
exhausted). `attempts` and `failureReason` are included:

```bash
curl -s http://localhost:8080/notify/0a3d...
```

```json
{
  "id": "0a3d...",
  "recipient": "fail@example.com",
  "channel": "EMAIL",
  "message": "doomed",
  "status": "DEAD",
  "attempts": 4,
  "failureReason": "Dead-lettered from 'notifications.send' (reason=rejected): delivery retries exhausted",
  "createdAt": "2026-06-21T10:00:00Z",
  "sentAt": null
}
```

## 4. Observe the queues

Open the management UI at **http://localhost:15672** (guest / guest):

- **Queues → `notifications.send`** — message rate, ready/unacked counts, and the
  four consumers. POST a `fail@example.com` and watch its message stay **unacked**
  on a consumer for the duration of the backoff (≈1s + 2s + 4s) before it leaves.
- **Queues → `notifications.dlq`** — the dead-letter queue. If you stop the app
  before posting `fail@example.com`, the dead-lettered message accumulates here
  (Ready count > 0) instead of being consumed, proving it isn't dropped. With the
  app running, the DLQ consumer drains it and marks the row `DEAD`.

From the CLI:

```bash
docker exec notification-rabbitmq \
  rabbitmqctl list_queues name messages messages_ready messages_unacknowledged consumers
```

## 5. Replay a dead-lettered message

A message in `notifications.dlq` is just the notification id. To re-attempt
delivery, move it back onto the work queue. The simplest manual replay uses the
management UI's **shovel**-free "Move messages" plugin if installed; without it,
re-publish the id directly:

```bash
# 1. Read the dead id from the row (status DEAD) or the DLQ, e.g. "0a3d..."
# 2. Re-publish it to the work queue (default exchange, routing key = queue name):
docker exec notification-rabbitmq \
  rabbitmqadmin publish exchange=amq.default routing_key=notifications.send payload='0a3d...'
```

The work queue's consumer will pick it up and retry from scratch. (If the
underlying cause is permanent — e.g. `fail@example.com` — it will simply be
dead-lettered again.) For a real system you'd typically fix the root cause first,
then replay in bulk via the Shovel plugin or a small admin endpoint.

## Tests

```bash
./mvnw test
```

- `NotificationIntegrationTest` — `@SpringBootTest` + MockMvc, with **Postgres,
  RabbitMQ and Redis provided by [Testcontainers](https://testcontainers.com/)** and
  [Awaitility](https://github.com/awaitility/awaitility) for async waits. Covers: a
  normal recipient ends `SENT` with `attempts == 1`; `flaky@example.com` recovers to
  `SENT` with `attempts >= 3` (retry recovers a transient failure); `fail@example.com`
  ends `DEAD` with a `failureReason` and is dead-lettered (only the DLQ consumer sets
  `DEAD`, so reaching it proves the message landed in `notifications.dlq`); the
  backoff gap between attempts increases; and the unknown-id (`404`) / validation
  (`400`) paths still hold.
  Idempotency: two POSTs with the same `Idempotency-Key` produce one row, the same
  id, and a single provider call (second response is `200`); different keys produce
  two rows; **8 concurrent** duplicate POSTs collapse to exactly one row (the
  UNIQUE-constraint race); and invoking the consumer twice for an already-`SENT`
  notification does not call the provider again.
  Auth + rate limiting: missing/unknown `X-API-Key` → `401`, valid key proceeds and
  stores `tenant_id`; exceeding a tenant's per-minute budget → `429` (with a positive
  `Retry-After`) while a different tenant is unaffected (isolation); and the bucket
  refills over time — driven by a controllable `Clock` so the test is fast.
  **Distributed correctness (v7):** two `RateLimiter` instances sharing one Redis are
  fired `2×limit` interleaved requests and admit **exactly `limit`** in total (not
  double) — the property that failed with the v6 in-memory limiter.
  **Observability (v8):** `/actuator/prometheus` returns `200` and contains every
  custom metric name; posting increments `notifications.submitted` and (once
  delivered) `notifications.sent`; a rate-limited request increments
  `ratelimit.rejected` with the right tenant tag; a dead-letter increments
  `notifications.dead`. (`@AutoConfigureObservability` enables the real Prometheus
  registry in the test — Spring Boot uses a `SimpleMeterRegistry` otherwise.)
- `SimulatedProviderTest` — unit test for the stubbed provider.

> The integration test needs a running Docker daemon (Testcontainers starts the
> Postgres, RabbitMQ and Redis containers automatically — no need to `docker compose
> up` first). Because of the backoff, the dead-letter test waits ~10s.

### Note on Docker Engine 29+

Testcontainers' bundled docker-java client defaults to Docker API `v1.32`, which
Docker Engine 29 rejects (its minimum is `1.44`). The `maven-surefire-plugin`
config in `pom.xml` pins `api.version=1.44` (and sets `DOCKER_HOST` so the
client strategy that reads it is selected) to work around this. On older Docker
engines this is harmless.
