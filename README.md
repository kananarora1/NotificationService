# Notification Service (v8)

An **asynchronous** notification service backed by **RabbitMQ**, with **Postgres**
as the source of truth. `POST /notify` saves the notification as `PENDING`,
publishes its id to a durable queue, and returns `202 Accepted` immediately. A
pool of `@RabbitListener` consumers drains the queue and performs delivery, with
**retries + backoff** and a **dead-letter queue** for permanent failures (v4),
**idempotency** against duplicate requests and redeliveries (v5), **tenant identity
via API keys** (v6), and a **Redis-backed distributed rate limiter** (v7).

A **lease reconciler** (v10) recovers notifications whose consumer died mid-delivery,
and (v11) republishes any accepted notification whose message never reached the broker,
so neither a crash nor a RabbitMQ outage strands a send — it can only delay it. See
[Crash recovery](#crash-recovery-leases-and-the-reconciler-v10) and
[Publish failures](#publish-failures-confirms-and-republish-v11).

v8 adds **observability** — no new features, just visibility into what already
exists. The app exports Micrometer metrics (custom business counters/timers/gauges
plus the auto-provided HTTP/JVM/DB/Rabbit metrics) on a Prometheus endpoint, and the
stack ships an auto-provisioned **Prometheus + Grafana** with a ready-made dashboard.
See [Observability](#observability-v8).

## Stack

- Java 21, Spring Boot 3.3 (Spring Web, Spring Data JPA, Validation, AMQP, Data Redis, Actuator)
- PostgreSQL — source of truth (notifications + tenants), schema managed by Flyway
- Config & secrets from the environment (`.env.example`); API keys stored SHA-256 hashed
- RabbitMQ — message broker (with retries + dead-letter queue)
- Redis — shared store for the distributed rate limiter
- Micrometer + Prometheus + Grafana — metrics and dashboards
- Maven

## Domain

A `Notification` has: `id` (UUID), `tenantId` (the API-key tenant that created it),
`recipient`, `channel` (`EMAIL`/`SMS`/`PUSH`), `message`, `status`, `attempts` (int),
a nullable `failureReason`, a nullable and **UNIQUE** `idempotencyKey`, `createdAt`,
a nullable `sentAt`, a nullable `publishedAt` (when the broker confirmed its message),
and the delivery lease `claimToken` / `claimedAt` (both set only
while `SENDING`). A `Tenant` has: `id`, `name`, unique `apiKey`, and
`rateLimitPerMinute`.

`status` is one of:

| status    | meaning                                                              |
|-----------|---------------------------------------------------------------------|
| `PENDING` | accepted, awaiting delivery — including between retries              |
| `SENDING` | claimed by one consumer under a lease, provider call in flight       |
| `SENT`    | delivered successfully                                               |
| `DEAD`    | permanently failed: retries exhausted and the message dead-lettered  |

Delivery is handled by `SimulatedProvider`: it sleeps ~800ms to mimic a slow
external API, logs `sent to {recipient} via {channel}`, and returns success. It
does **not** do any real delivery (no SMTP/SMS gateway). Two recipients drive the
failure paths so they are testable on demand:

- **`fail@example.com`** — always throws (permanent failure → ends `DEAD` after
  retries are exhausted).
- **`flaky@example.com`** — throws on the first two attempts for a given
  notification, then succeeds (transient failure → recovers to `SENT` via retry).

`SimulatedProvider` is also **idempotent on the notification id**: a repeat send of an
id it has already delivered is acknowledged without delivering again (logged as
`already delivered {id}`). Every provider must behave this way — see
[the double-send contract](#the-double-send-contract). Its record of delivered ids lives
in the JVM, though, so unlike a real provider's it is forgotten on restart and not shared
between instances: it demonstrates the contract within one process, not across a crash.

`SimulatedProvider` is for development and tests only. It is inactive under the `prod`
profile, so starting with `SPRING_PROFILES_ACTIVE=prod` fails until a real
`NotificationProvider` bean exists — deliberately, rather than "delivering" into a log.

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
                                         │
                                  DEAD write still failing after 3 tries → reject
                                         │  via the DLQ's own x-dead-letter-exchange
                                         ▼
                 RabbitMQ durable queue "notifications.parking-lot"  (no consumer)
```

- **Work queue** — `RabbitConfig` declares `notifications.send` (`durable`),
  configured with `x-dead-letter-exchange` → `notifications.dlx` so rejected
  messages are dead-lettered.
- **Dead-letter topology** — a durable direct exchange `notifications.dlx` and a
  durable queue `notifications.dlq` bound to it. The DLQ dead-letters in turn, to the
  durable `notifications.parking-lot` on the same exchange, which nothing consumes.
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
  row `DEAD`, records a `failureReason`, and logs the id and reason. It has its own
  container factory — one consumer, 3 attempts — and parks what it cannot record (see
  [Dead-letters that cannot be recorded](#dead-letters-that-cannot-be-recorded-v13)).

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

Two independent duplicate sources are defended against: client retries (one row per
key) and broker redeliveries (one provider call per claim). Together with the
[lease reconciler](#crash-recovery-leases-and-the-reconciler-v10), delivery is
**at-least-once with an idempotent provider** — every accepted notification is sent,
and the provider absorbs the rare repeat. It is *not* at-most-once: a crash after the
provider accepted a notification but before `SENT` was recorded causes one retry.

### Caller-level: the `Idempotency-Key` header

Clients retry POSTs (timeouts, network blips, at-least-once upstreams). To make a
retry safe, send a stable **`Idempotency-Key`** header — typically derived from the
business event (e.g. `order-4711-receipt`):

- **Header present, key unseen** → create the row (storing the key), publish, return
  `202 Accepted`.
- **Header present, key seen** → return the **existing** `{ id, status }` with
  `200 OK`. No second row, no second publish. A `PENDING` answer here is always
  followed through, even if the original request's publish failed: a `PENDING` row with
  no confirmed message is republished by the reconciler (see
  [Publish failures](#publish-failures-confirms-and-republish-v11)).
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
connection drop, etc.), and a redelivery can race the original. Before calling the
provider, the consumer **claims** the row with one conditional UPDATE
(`PENDING -> SENDING ... WHERE status = 'PENDING'`):

- claim won → deliver, then mark `SENT`.
- claim lost (row is `SENDING` on another consumer, or already `SENT`/`DEAD`) →
  **skip the provider call**, log `skipping …`, and ack.

Of two concurrent copies of one message, exactly one reaches the provider. A failed
attempt releases the claim (`SENDING -> PENDING`) so the listener's retry can win it
again.

## Crash recovery: leases and the reconciler (v10)

**The problem.** A claim that is never finished — the consumer is `kill -9`ed, OOMs, or
hits a DB error between the claim and the `SENT` write — used to leave the row in
`SENDING` **forever**: the broker's redelivery lost the claim, skipped, and acked, and
nothing else ever looked at the row. Measured before this change: 30 notifications
posted, JVM killed 1s in, restarted — 16 rows were still `SENDING` 100s later with an
empty queue, and would have stayed that way.

**The lease.** The claim now also stamps `claim_token` (a fresh UUID per attempt) and
`claimed_at` (`now()` — the *database* clock, so every instance measures expiry against
the same clock). Every write after the claim is conditional on the token:

| write | guarded by | if it matches no row |
|---|---|---|
| `SENDING -> SENT` | `status = 'SENDING' AND claim_token = ?` | lease was reclaimed: log, write nothing, ack |
| `SENDING -> PENDING` (release after a failure) | same | lease was reclaimed: log, drop this copy (no retry) |
| `PENDING -> DEAD` (from the DLQ) | `status = 'PENDING'` | another delivery claimed or finished it: log, ignore |

So a consumer whose lease was taken away can never overwrite what happened next — in
particular, a late dead-letter can no longer turn a `SENT` row into `DEAD`.

**The reconciler.** `LeaseReconciler` runs every `sweep-interval` (30s). In one short
transaction it locks up to `batch-size` (100) `SENDING` rows whose lease is older than
`lease-timeout` (60s) with `SELECT … FOR UPDATE SKIP LOCKED` (so several instances
never fight over a row) and returns them to `PENDING` with `published_at` cleared — the
dead consumer's message is gone. The same sweep's republish pass then publishes them
with confirms (see [Publish failures](#publish-failures-confirms-and-republish-v11)), and
anything not confirmed is retried by the next sweep. Rows stranded before V5 have no
lease at all and are reclaimed on the first sweep. Each reclaim increments
`notifications.reclaimed` and logs a `WARN`.

The lease covers **one** provider call, not the retry cycle (the claim is released
between retries), so 60s only has to beat the slowest single send. Worst-case delay
for a crashed send is `lease-timeout + sweep-interval` ≈ 90s.

Measured after this change, same procedure (30 posted, `kill -9` at 1s, restart): the
16 redeliveries were skipped while the leases were live, the leases expired 60s after
the claim, the next sweep reclaimed all 16, and all 30 were `SENT` 77s after posting —
the 16 recovered rows with `attempts = 2`.

### The double-send contract

Reclaiming trades a possible **double-send** for a guaranteed eventual send. If the
provider had already accepted the notification before its consumer died (or a send ran
longer than the lease), the reclaimed retry calls the provider **again** for the same
id. That is deliberate. It is safe only because:

- **Every `NotificationProvider` must be idempotent on `notification.getId()`.** A
  repeat call for a delivered id must succeed without delivering again. For a real
  provider, pass the notification id as its idempotency key — if its API takes one; many
  email/SMS APIs do not, so check. Without one, exactly-once is impossible: a record kept
  by this app can only be written *after* the send returns, which is the same gap `SENT`
  leaves. Then either accept the rare duplicate, or narrow the gap with provider features
  (tag the message with the id and look it up before resending, or record `SENT` from
  delivery webhooks).
- **Provider calls must time out well below `lease-timeout`**, so a slow send fails and
  retries rather than being reclaimed underneath itself.

## Publish failures: confirms and republish (v11)

**The problem.** `POST /notify` commits the row, *then* publishes its id. If RabbitMQ was
down, the publish threw: the client got `500`, and the row sat `PENDING` with no message
behind it, forever. A retry with the same `Idempotency-Key` made it worse — the fast path
answered `200 {status: PENDING}` for a notification that would never be delivered. And
with no publisher confirms, even a publish that "succeeded" proved nothing.

**Confirms.** Every publish is now mandatory and waits for a publisher confirm
(`publisher-confirm-type: correlated`, `publisher-returns: true`,
`template.mandatory: true`). It counts as published only if the broker **acked** it and
**routed** it to a queue; a nack, an unroutable return, an unreachable broker or no
confirm within `notifications.publisher.confirm-timeout` (5s) all count as failed.
`spring.rabbitmq.connection-timeout` (5s) bounds the attempt against a broker that hangs
rather than refusing. On a confirmed publish the row's `published_at` is set, so a
`PENDING` row with `published_at` null is exactly one with no message known to be on the
broker.

**What `202` means.** `202 Accepted` means **durably accepted**, not "already on the
queue". Once the row is committed nothing fails the request: if the publish is not
confirmed, the row stays `PENDING` with `published_at` null, the API still answers `202`,
and the reconciler guarantees it is published later. (Answering `500` would invite a
retry that, without an `Idempotency-Key`, creates a second notification.)

**Republish.** The reconciler's sweep has a second pass: it finds `PENDING` rows with
`published_at` null older than `publish-grace` (15s — the grace keeps it off a row whose
`create()` is still publishing; it must exceed connection-timeout + confirm-timeout),
and republishes each with confirms, setting `published_at` only on a confirm. Each row is
locked (`FOR UPDATE SKIP LOCKED`), published and marked in its own short transaction, and
the first unconfirmed publish ends the pass — the rest wait for the next sweep instead of
each timing out. This pass also publishes the rows reclaimed from `SENDING`. Each
confirmed republish increments `notifications.republished`.

Republishing is safe even when a message for the row *is* already queued (its confirm
was lost, not the message): the claim lets one copy deliver and skips the other — the
[double-send contract](#the-double-send-contract) again.

**Measured.** Before the fix, a failed publish on create answered `500`, the row stayed
`PENDING` with no message, a same-key retry answered `200 PENDING`, and it was never
delivered. After it, with `docker stop notification-rabbitmq`: 5 POSTs (3 with an
`Idempotency-Key`) all answered `202` in 10–25ms, leaving 5 `PENDING` rows with
`published_at` null; same-key retries answered `200 PENDING`; `unpublished_pending` read
5 and the two sweeps during the outage each stopped at the first unconfirmed publish.
RabbitMQ was started again and the next sweep, 24s later, republished all 5 with confirms:
all `SENT`, each delivered once with `attempts = 1`, `republished_total` 5,
`unpublished_pending` back to 0. With the broker up, waiting for the confirm keeps a POST
at about 10ms.

Rows created before V6 have `published_at` null and are republished once on the first
sweep after deploy — harmless duplicates for rows already queued, and a rescue for any
stranded by a failed publish before the fix.

**Not covered here** (separate fixes): a `PENDING` row whose message was confirmed and
then lost on the consumer side still has `published_at` set, so the republish pass does
not touch it. The case that mattered — a dead-letter whose `DEAD` write keeps failing —
is fixed in v13, below. Replaying a `DEAD` notification still does nothing.

## Dead-letters that cannot be recorded (v13)

**The bug.** After its last failed attempt a row is `PENDING` with `attempts = 4`, its
message goes to `notifications.dlq`, and `DeadLetterListener` writes `DEAD`. If that
write failed, the DLQ listener retried it (on the work queue's container factory: 4
attempts, up to 32 consumers), then rejected the message — and `notifications.dlq` had
no dead-letter exchange, so RabbitMQ **discarded** it. The row stayed `PENDING` forever:
it has a confirmed message, so the republish pass skips it. `notifications.dead` never
incremented (the exception came first) and the DLQ drained to 0, so neither
`DeadLetterGrowing` nor `DeadLetterQueueNotDraining` could fire. The trigger is a
correlated outage: Postgres down fails every send, so the DLQ floods exactly when every
`DEAD` write fails too.

Reproduced first, by a test that makes every `DEAD` write for one `fail@example.com`
throw: `markDead` was called 4 times, the row was left `PENDING` with `attempts = 4`,
`notifications.dead` +0, DLQ depth 0, and the message was nowhere — it had been dropped.

**Nothing is discarded any more.** Two safety nets, one on each side:

- **Broker side: the parking lot.** `notifications.dlq` now dead-letters to the durable
  `notifications.parking-lot`. When the DLQ listener gives up, the message moves there
  instead of vanishing. Nothing consumes the parking lot; it waits for a person (see
  [Parked dead-letters](#parked-dead-letters)), its depth is the
  `queue.depth{queue="notifications.parking-lot"}` gauge, and `ParkedDeadLetters`
  (critical) fires when it is above 0.
- **Database side: the reconciler.** Each sweep marks `DEAD` the `PENDING` rows that
  have used up their retries (`attempts >= retry.max-attempts`) and whose last attempt
  began more than `dead-grace` (60s) ago — once Postgres is back, every row that failed
  its last delivery attempt but could not be recorded `DEAD`. The new `last_attempt_at` column (V8, stamped by the claim) dates the
  last attempt, since `claimed_at` is cleared when an attempt releases its claim. These
  count in `notifications.dead` like any other `DEAD` (so `DeadLetterGrowing` sees them)
  and in `notifications.dead_reconciled`. The pass runs after the reclaim pass, so a row
  reclaimed from `SENDING` on its last attempt is marked `DEAD` rather than retried again.

**The DLQ listener has its own container factory**: one consumer, prefetch 1, and a
short retry (`notifications.dead-letter.retry.*`: 3 attempts, 0.5s then 1s), instead of
the work queue's 16–32 consumers and 4 attempts. In a Postgres outage that is about one
write a second against the dead database rather than four per message across 16
consumers, and each message parks within ~1.5s. Whatever is still in the DLQ when
Postgres returns is recorded normally.

**Both `DEAD` writes are guarded on `status = 'PENDING'`**, so neither can overwrite a
`SENT` row, nor a `SENDING` one: a `SENDING` row has a delivery in flight, and if that
consumer has died the lease reclaim returns it to `PENDING` first. Whichever write gets
there first wins; the other matches no row, so a notification is counted `DEAD` once.
`notifications.dead` is incremented only after the write succeeds. A dead-letter for a
row that no longer exists is logged and acknowledged — there is nothing to record, and
nothing for a person to look at.

Rows stranded this way before V8 have `last_attempt_at` NULL, which the sweep reads as
`created_at`, so the first sweep after deploy marks them `DEAD`.

**Measured** on a throwaway stack, with a trigger making every `DEAD` write fail:

- **Before (v12):** 2 `fail@example.com` rows were left `PENDING` with `attempts = 4`.
  The DLQ, served by 16 consumers, tried each `DEAD` write 4 times, then both messages
  were gone from every queue, and `notifications_dead_total` did not exist.
- **After:** 3 rows. The single DLQ consumer tried each write 3 times, and all 3
  messages were parked within 5s (`queue_depth{queue="notifications.parking-lot"}` 3).
  With the trigger dropped, the reconciler marked all 3 `DEAD` 42s later (60s after
  their last attempt), and `notifications_dead_total` and
  `notifications_dead_reconciled_total` each reached 3.
- **Upgrade:** the 2 rows the old version had stranded were marked `DEAD` by the first
  sweeps after V8.
- **`docker pause` on Postgres** mid-delivery: every database call failed after the
  pool's 30s timeout, so the 2 messages reached the DLQ after ~90s and parked after
  ~4 minutes.
  - Both rows were `PENDING` with `attempts = 2`: the outage itself had spent their
    retries at the claim, so the reconciler rightly left them alone.
  - Replaying the 2 parked ids onto `notifications.send` gave each 4 more attempts, and
    the listener recorded both `DEAD`.

> **Upgrading an existing broker:** v13 adds dead-letter arguments to
> `notifications.dlq`, and RabbitMQ refuses to redeclare a queue with different
> arguments. Stop the app, check the DLQ is empty (`rabbitmqctl list_queues name
> messages`), delete it — `docker exec notification-rabbitmq rabbitmqctl delete_queue
> notifications.dlq` — and start the new version, which declares it again.
>
> Skipping this used to go unnoticed: Spring AMQP only logs the refused redeclare, so
> the app reported healthy while running on the old DLQ — no parking lot, the bug still
> there. On startup `QueueArgumentsCheck` now redeclares every queue itself and **refuses
> to start** if one exists with different arguments, naming the queue and these steps.
> A broker that is down at startup is not an error; the queues are declared on connect.

## Tenants, API keys & rate limiting (v6)

### Identity: the `X-API-Key` header

`POST /notify` requires an **`X-API-Key`** header. The key is resolved to a
**tenant** (table `tenants`: `id`, `name`, unique `api_key_hash`,
`rate_limit_per_minute`). A missing or unknown key returns **`401 Unauthorized`**
before the controller runs (enforced by a `HandlerInterceptor`). The resolving
tenant's id is stored on every notification it creates (`tenant_id`).
`GET /notify/{id}` is **not** gated.

**The key itself is never stored** — only its SHA-256 hash. On each request the
presented key is hashed and the tenant looked up by that hash, on a unique index. See
[API key storage](#api-key-storage-sha-256-not-bcrypt).

Tenants are seeded at startup (idempotently, safe across instances) from
`notifications.seeding.*`, each key overridable by environment variable:

| tenant   | key (local dev) | env var          | rateLimitPerMinute |
|----------|-----------------|------------------|--------------------|
| acme     | `key-acme`      | `ACME_API_KEY`   | 60                 |
| globex   | `key-globex`    | `GLOBEX_API_KEY` | 10                 |
| initech  | `key-initech`   | `INITECH_API_KEY`| 5                  |

These are development keys, published here deliberately. In a deployed environment set
`SEED_TENANTS_ENABLED=false` and provision tenants properly — see
[Adding a tenant](#adding-a-tenant).

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
| `notifications.dead` | counter | tenant | recorded `DEAD` (retries exhausted), by the DLQ listener or the reconciler; counted only once the write succeeds |
| `notifications.dead_reconciled` | counter | — | the subset of `notifications.dead` the reconciler wrote because the DLQ listener could not (registered at 0) |
| `ratelimit.rejected` | counter | tenant | request rejected with 429 |
| `notifications.delivery.latency` | timer | — | duration of a `provider.send()` call |
| `notifications.end_to_end.latency` | timer | — | accepted (`createdAt`) → sent (`sentAt`) |
| `notifications.pending` | gauge | — | rows in PENDING status |
| `notifications.sending` | gauge | — | rows in SENDING status (claimed, in flight) |
| `notifications.stuck_sending` | gauge | — | SENDING rows past their lease, awaiting the reconciler |
| `notifications.reclaimed` | counter | — | stranded SENDING rows returned to PENDING for republishing (registered at 0 on startup) |
| `notifications.republished` | counter | — | PENDING rows with no confirmed message that the reconciler republished, counted on the broker's confirm (includes reclaimed rows; registered at 0) |
| `notifications.unpublished_pending` | gauge | — | PENDING rows with no confirmed message, older than the publish grace |
| `queue.depth` | gauge | queue | RabbitMQ messages in `notifications.send` / `.dlq` / `.parking-lot` |

The gauges are refreshed by a scheduled poller every 5s and cached, so a
Prometheus scrape never triggers a `COUNT(*)` on a large table or a broker
round-trip. The database counts and the queue depths refresh on separate scheduled tasks
(each with its own scheduler thread), so the queue gauges stay live while Postgres is down
or hanging — exactly when the parking lot fills. Per-tenant metrics are tagged with the tenant **name** (e.g. `acme`).

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
cp .env.example .env    # first time only — then edit the passwords
docker compose up -d
```

`docker compose` reads `.env` from this directory automatically. Every port and
credential below is a `.env` variable with a local-development fallback, so a bare
`docker compose up -d` works without one.

This starts:

- **Postgres 16** on `localhost:${POSTGRES_PORT:-5433}` (host → container `5432`, to
  avoid colliding with a native Postgres on `5432`). Database `POSTGRES_DB`, user
  `DB_USERNAME`, password `DB_PASSWORD` — all defaulting to `notifications`.
- **RabbitMQ 3.13** with the management plugin: AMQP on `localhost:5672`, and the
  management UI on **http://localhost:15672** (login `RABBITMQ_USERNAME` /
  `RABBITMQ_PASSWORD`, default **guest / guest**).
- **Redis 7** on `localhost:6379` — the shared store for the distributed rate
  limiter. Inspect a bucket with
  `docker exec notification-redis redis-cli hgetall "ratelimit:<tenantId>"`.
- **Prometheus** on **http://localhost:9090** — scrapes the app every 5s.
- **Grafana** on **http://localhost:3000** (`GRAFANA_ADMIN_USER` /
  `GRAFANA_ADMIN_PASSWORD`, default **admin / admin**) — pre-loaded with the
  Notification Service dashboard.

> **Upgrading an existing broker:** v4 adds dead-letter arguments to the
> `notifications.send` queue. RabbitMQ refuses to redeclare an existing queue with
> different arguments, so if you ran v3 against a persistent broker, delete the old
> queue first: `docker exec notification-rabbitmq rabbitmqctl delete_queue notifications.send`.
> v13 does the same to `notifications.dlq`: with the app stopped and the DLQ empty,
> `docker exec notification-rabbitmq rabbitmqctl delete_queue notifications.dlq`.

## 2. Run the app

```bash
# load the secrets from .env into the environment, then start
set -a; source .env; set +a
./mvnw spring-boot:run
```

Without `source .env` the app still starts: every setting falls back to the
local-development default baked into `application.yml` (see
[Configuration & secrets](#configuration--secrets)).

The app listens on `http://localhost:8080` (`SERVER_PORT`). The schema is applied by
**Flyway** on startup and verified by Hibernate (`ddl-auto: validate` — it never
changes the schema); see [Database migrations](#database-migrations). The
queues/exchanges are declared on startup.

## 3. Try the endpoints

### POST /notify

Body: `{ recipient, channel, message }`. **Requires** an `X-API-Key` header
(see [Tenants & rate limiting](#tenants-api-keys--rate-limiting-v6)). Saves as
`PENDING`, publishes to `notifications.send`, and returns `202 Accepted` with
`{ id, status: "PENDING" }` **immediately**. `202` means durably accepted: if RabbitMQ
does not confirm the publish, the request still succeeds and the reconciler publishes it
later (see [Publish failures](#publish-failures-confirms-and-republish-v11)).

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

### Errors

Every error is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem response,
`Content-Type: application/problem+json`, with a `detail` that says what to change.
Field and parameter failures also list each one under `errors`:

```bash
curl -s -H 'X-API-Key: key-acme' 'localhost:8080/notifications?status=FAILED'
# {"type":"about:blank","title":"Bad Request","status":400,
#  "detail":"Invalid request: 'status' has invalid value 'FAILED'; expected one of PENDING, SENDING, SENT, DEAD.",
#  "instance":"/notifications",
#  "errors":[{"field":"status","message":"has invalid value 'FAILED'; expected one of PENDING, SENDING, SENT, DEAD"}]}
```

| status | when | `detail` / extra fields |
|---|---|---|
| `400` | invalid body field, query parameter, path id or `Idempotency-Key`; malformed JSON | names each field, the value sent and what is allowed; `errors[]` |
| `401` | `X-API-Key` missing, or not a known key | `Missing X-API-Key header.` / `Unknown API key.` |
| `404` | unknown notification id, or unknown endpoint | which id / which method and path |
| `405`, `415` | wrong method / `Content-Type` | Spring's standard wording |
| `429` | tenant over its per-minute budget | the limit and wait; `Retry-After` header and `retryAfterSeconds` |
| `500` | anything unexpected | generic — no internals — plus an `errorId` that matches the ERROR log line with the stack trace |

`recipient`, `message` and `Idempotency-Key` are limited to 255 characters (their column
size), so an over-long value is a `400` naming the field rather than a failed insert.

## 4. Observe the queues

Open the management UI at **http://localhost:15672** (guest / guest):

- **Queues → `notifications.send`** — message rate, ready/unacked counts, and the
  four consumers. POST a `fail@example.com` and watch its message stay **unacked**
  on a consumer for the duration of the backoff (≈1s + 2s + 4s) before it leaves.
- **Queues → `notifications.dlq`** — the dead-letter queue. If you stop the app
  before posting `fail@example.com`, the dead-lettered message accumulates here
  (Ready count > 0) instead of being consumed, proving it isn't dropped. With the
  app running, the DLQ consumer drains it and marks the row `DEAD`.
- **Queues → `notifications.parking-lot`** — dead-letters the DLQ consumer could not
  record as `DEAD`. Normally empty, and nothing drains it: see
  [Parked dead-letters](#parked-dead-letters).

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

### Parked dead-letters

A message in `notifications.parking-lot` means a notification's message used up its
retries and the DLQ consumer could not write `DEAD` (Postgres down, typically). Once
Postgres is healthy, look up each parked id:

- **`DEAD`**, `failure_reason` naming the reconciler — the row had reached its retry
  limit and the reconciler recorded it (within `dead-grace` plus a sweep). Accounted
  for; purge the message.
- **still `PENDING`** — the retries were spent on the outage itself: each attempt's
  claim failed against the database, so `attempts` never reached the limit and neither
  reconciler pass touches the row (its message was confirmed, then parked). It has not
  been delivered. Replay it: a `PENDING` row is picked up by a plain re-publish onto
  `notifications.send` (as in the replay above) and gets a full set of attempts.

```bash
# how many, and which ids (requeue=true leaves them parked)
docker exec notification-rabbitmq rabbitmqctl list_queues name messages
docker exec notification-rabbitmq \
  rabbitmqadmin get queue=notifications.parking-lot count=10 ackmode=ack_requeue_true
# check each row
docker exec notification-postgres psql -U notifications -d notifications \
  -c "select id, status, attempts, failure_reason from notifications where id in ('…')"
# replay each one still PENDING
docker exec notification-rabbitmq \
  rabbitmqadmin publish exchange=amq.default routing_key=notifications.send payload='…'
# once every parked id is accounted for, clear them
docker exec notification-rabbitmq rabbitmqctl purge_queue notifications.parking-lot
```

`ParkingLot.replayOne()` takes one parked message and sends its notification through
delivery again from scratch (`NotificationService.requeueForDelivery`: back to `PENDING`
with attempts and failure reason cleared, then published); a row already `SENT` or in
flight is left alone and the message is just acknowledged. No endpoint calls it yet.

## Operations

### Graceful shutdown

On `SIGTERM` (a `docker stop`, a Kubernetes pod eviction, `Ctrl-C`) the app drains
instead of dying mid-delivery:

```yaml
server:
  shutdown: graceful          # drain in-flight HTTP requests
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
notifications:
  listener:
    shutdown-timeout: 25s     # bound on the RabbitMQ listener drain
```

`server.shutdown: graceful` covers the **HTTP connector only** — Spring Boot knows
nothing about message listeners. The listener side is configured separately in
`ListenerShutdownConfig`, which raises Spring AMQP's `shutdownTimeout` from its 5s
default. With an 800ms-per-send provider and retry backoff, 5s is not enough to finish
what is in flight.

**What happens to in-flight messages on deploy** (measured, not assumed — see below):

| | behaviour |
|---|---|
| HTTP request already executing | runs to completion; Tomcat stops accepting new connections first |
| `@RabbitListener` invocation already executing | **runs to completion and acks** — never abandoned mid-send |
| Message prefetched but not yet started | **requeued untouched** (`attempts` still 0), picked up by the next instance |
| Message still in the queue | untouched, waits for the next consumer |

The middle two are the important ones. A notification the provider has already
accepted is never redelivered, so no duplicate send. A notification that had not
started yet goes back on the queue rather than being lost.

One thing that looks alarming in the logs but is correct:

```
Rejecting received message(s) because the listener container has been stopped
```

That is the prefetched-but-unstarted batch being handed back. It **requeues** — it does
not route to the DLQ, despite `default-requeue-rejected: false`, which governs listener
*failures* rather than container shutdown. Those messages come back as `ready`, not
`unacked`.

**Verified under load.** 60 notifications submitted against the 800ms provider, then
`SIGTERM` while 16 were mid-send:

| | at SIGTERM | after shutdown | after restart |
|---|---|---|---|
| `SENT` | 16 | 48 (all `attempts = 1`) | 60 (all `attempts = 1`) |
| `SENDING` (in flight) | 16 | 0 | 0 |
| queue `ready` | 28 | 12 | 0 |
| queue `unacked` | 28 | **0** | 0 |
| dead-lettered | 0 | **0** | 0 |

Every one of the 16 in-flight sends finished and acked. Nothing was left unacked,
nothing was dead-lettered, and every notification ended `SENT` with `attempts = 1` —
so nothing was delivered twice.

**Containerised deployments** must give Docker a `stop_grace_period` comfortably above
the app's own budget, or the JVM is `SIGKILL`ed mid-drain and the guarantees above are
void. The worst case is roughly two shutdown phases back to back (HTTP drain, then
listener drain), so with the 30s default use **90s**. `docker-compose.yml` carries a
ready-to-use `app` service block showing this; the app currently runs on the host, so
there is no app container in the compose file yet.

### Alert rules

[`observability/prometheus/alerts.yml`](observability/prometheus/alerts.yml) is mounted
into Prometheus and visible at **http://localhost:9090/alerts**. Every threshold is
commented in the file so it is easy to tune — they are sized for the demo scale, not
for production traffic.

Alertmanager is **not** wired up; the rules are fully usable from the Prometheus UI
without it. `prometheus.yml` has a commented `alerting:` block if you want to add one.

| alert | fires when | what a responder does |
|---|---|---|
| **DeadLetterGrowing** `critical` | any notification was dead-lettered in the last 10m (`for: 2m`) | These gave up and will **not** retry themselves. Read `failure_reason` on the `DEAD` rows, fix the cause, then replay onto `notifications.send`. |
| **ParkedDeadLetters** `critical` | `max(queue_depth{queue="notifications.parking-lot"}) > 0` for 1m | The DLQ consumer could not record dead-letters as `DEAD` — usually a Postgres outage. Check the logs, confirm the rows have since been marked `DEAD` (`notifications_dead_reconciled_total`), then replay or purge the parked messages ([Parked dead-letters](#parked-dead-letters)). |
| **DeadLetterQueueNotDraining** `critical` | `notifications.dlq` depth > 0 for 5m | `DeadLetterListener` is stuck or down, so failures are no longer being recorded as `DEAD`. Check the app logs and `rabbitmqctl list_queues name messages consumers`. |
| **PendingBacklogHigh** `warning` | `notifications_pending > 100` for 5m | Consumers are behind. Check `queue_depth` for the work queue, that instances are alive, and whether the provider slowed down. |
| **HighErrorRate** `warning` | failed attempts > 0.2/s for 5m | Retries may still recover these, but sustained failure leads to dead-letters. Check provider health and the recurring exception. |
| **StuckSendingNotRecovered** `critical` | `max(notifications_stuck_sending) > 0` for 3m | Rows are past their lease but the reconciler is not reclaiming them, so they will never send. Look for `LeaseReconciler` errors (a failing sweep logs at `ERROR` each interval). |
| **AcceptedNotificationsNotPublished** `critical` | `max(notifications_unpublished_pending) > 0` for 5m | Accepted notifications have had no confirmed message on the broker for 5 minutes — RabbitMQ is down, alarmed or unreachable. They are safe in Postgres and republish on their own once it confirms again; check `rabbitmqctl status` and the logs for `Could not publish`. |
| **SendingLeasesReclaimedContinuously** `warning` | reclaim rate > 0 for 15m | One crash causes one burst, which is normal. A sustained rate means consumers keep dying, or sends outlive the lease (duplicate sends absorbed only by provider idempotency). Check restarts/OOMs and delivery latency vs `lease-timeout`. |
| **TargetDown** `critical` | `up == 0` for 1m | An instance stopped being scraped. If it is the only one, the service is down. |
| **RateLimitRejectionsSpike** `warning` | a tenant's 429 rate > 1/s for 5m | Decide: abuse (a client retrying without honouring `Retry-After`) or a tenant that needs a bigger `rate_limit_per_minute`. |
| **HighEndToEndLatencyP99** `warning` | p99 submit→deliver > 5s for 10m | Compare against `notifications_delivery_latency_seconds`. If provider time is flat, the latency is queueing — check the backlog. |

Two things worth knowing about these rules:

- **`DeadLetterGrowing` counts `notifications_dead_total`, not DLQ depth.** The obvious
  rule — `queue_depth{queue="notifications.dlq"} > 0` — would essentially never fire,
  because `DeadLetterListener` consumes the DLQ and drains it to zero within
  milliseconds. Depth-based alerting on that queue detects a *broken consumer*, which is
  why it is a separate alert (`DeadLetterQueueNotDraining`) rather than the primary one.
- **It is also not written with `increase()`,** for a reason worth knowing before you
  write the next counter alert. A Micrometer counter series does not exist until its
  first increment, so when the first dead-letters after a restart all land within one
  5s scrape, the series is *born* at its full value. `increase()` then sees a flat line
  and reports 0 — this was observed silently missing three real dead-letters. The rule
  subtracts the value 10 minutes ago instead, with `or vector(0)` on both sides so an
  absent series reads as zero:

  ```promql
  (sum(notifications_dead_total) or vector(0))
    - (sum(notifications_dead_total offset 10m) or vector(0)) > 0
  ```
- **`TargetDown` fires out of the box.** `prometheus.yml` scrapes both `app-8080` and
  `app-8081`, and 8081 is not running unless you start a second instance by hand. Remove
  that target to silence it.

## Configuration & secrets

No credential is hardcoded. Every setting in `application.yml` is read from an
environment variable with a **local-development fallback**:

```yaml
spring:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5433/notifications}
    username: ${DB_USERNAME:notifications}
    password: ${DB_PASSWORD:notifications}
```

The fallbacks are the docker-compose values — non-secret, and there so a fresh clone
runs with `docker compose up -d && ./mvnw spring-boot:run`. **A deployed environment
must set the variables explicitly**; the environment always wins over the default.

[`.env.example`](.env.example) is the full list. Copy it and fill in real values:

```bash
cp .env.example .env
```

`.env` is gitignored and must never be committed. `docker compose` reads it from this
directory automatically; to run the app on the host with the same values:

```bash
set -a; source .env; set +a
./mvnw spring-boot:run
```

| variable | purpose | dev default |
|---|---|---|
| `DB_URL` | JDBC url | `jdbc:postgresql://localhost:5433/notifications` |
| `DB_USERNAME` / `DB_PASSWORD` | Postgres credentials (also used to init the container) | `notifications` |
| `POSTGRES_DB` / `POSTGRES_PORT` | container database name / host port | `notifications` / `5433` |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | broker address | `localhost` / `5672` |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | broker credentials | `guest` |
| `RABBITMQ_MANAGEMENT_PORT` | management UI port | `15672` |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | rate-limiter store (password empty locally) | `localhost` / `6379` / — |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` / `GRAFANA_PORT` | Grafana login and port | `admin` / `admin` / `3000` |
| `PROMETHEUS_PORT` | Prometheus port | `9090` |
| `SERVER_PORT` | app HTTP port | `8080` |
| `SEED_TENANTS_ENABLED` | seed the dev tenants on startup | `true` |
| `ACME_API_KEY` / `GLOBEX_API_KEY` / `INITECH_API_KEY` | seeded tenants' keys | `key-acme` / `key-globex` / `key-initech` |
| `ACME_RATE_LIMIT` / `GLOBEX_RATE_LIMIT` / `INITECH_RATE_LIMIT` | their per-minute budgets | `60` / `10` / `5` |

`ConfigSecretsTest` enforces this: it fails the build if a credential stops being an
environment placeholder, if a variable used by `application.yml` is missing from
`.env.example`, or if `.env` stops being gitignored.

### API key storage: SHA-256, not bcrypt

API keys are stored as a **SHA-256 hash** (lowercase hex, column `api_key_hash`,
unique-indexed). A client keeps sending the same `X-API-Key` it always did; only the
storage changed. On each request the interceptor hashes the presented key and looks
the tenant up by hash — one indexed lookup, the same cost as the old plaintext one.

**Why not bcrypt/argon2?** They are *password* hashes, deliberately slow (a tunable
work factor, ~100ms) to make brute-forcing low-entropy human-chosen passwords
impractical. Neither property helps here:

- **It is the hot path.** Every request authenticates. A 100ms KDF would dominate an
  endpoint whose p99 is a few milliseconds, and would cap throughput at roughly
  *threads ÷ 0.1s* requests per second. SHA-256 is well under a microsecond.
- **API keys are generated, not chosen.** 256 bits from `SecureRandom` — there is no
  dictionary to run and no feasible brute force, so the work factor buys nothing. The
  reason to hash at all is narrower: a leaked database dump must not contain working
  credentials.
- **No salt, deliberately.** Lookup is *by* the hash, on an index. A per-key salt would
  force a scan of every tenant row per request. Unsalted is safe precisely because the
  input is high-entropy — rainbow tables cover human passwords, not 256-bit random
  strings.

The trade-off worth stating: SHA-256 gives no protection against a *weak* key, so keys
must be generated, never chosen by a human.

### Adding a tenant

Generate a random key, give it to the client **once**, store only its hash:

```bash
# 1. Generate a 256-bit key (43 URL-safe base64 chars)
API_KEY=$(openssl rand -base64 32 | tr -d '=+/' | cut -c1-43)
echo "$API_KEY"          # hand this to the client now — it cannot be recovered later

# 2. Compute its SHA-256, exactly as the app does
API_KEY_HASH=$(printf '%s' "$API_KEY" | shasum -a 256 | cut -d' ' -f1)

# 3. Insert the tenant, hash only
docker exec notification-postgres psql -U "${DB_USERNAME:-notifications}" -d notifications \
  -c "insert into tenants (id, name, api_key_hash, rate_limit_per_minute)
      values (gen_random_uuid(), 'newco', '$API_KEY_HASH', 30);"
```

Postgres computes the identical digest itself, if you would rather not shell out:
`encode(sha256(convert_to('<key>','UTF8')),'hex')` — that is exactly what the `V3`
backfill used.

**Rotating or revoking** a key is an `update` or `delete` on that row. Since the
plaintext is unrecoverable, a lost key is re-issued, never looked up.

## Database migrations

The schema is managed by **[Flyway](https://flywaydb.org/)**, not by Hibernate.
Migrations live in `src/main/resources/db/migration` and run automatically on
startup, before the JPA `EntityManagerFactory` is built.

| file | what it does |
|---|---|
| `V1__baseline_schema.sql` | The full schema — `notifications`, `tenants`, their constraints and indexes. |
| `V2__tenant_scoped_list_indexes.sql` | Indexes for the tenant-scoped list queries. |
| `V3__hash_api_keys.sql` | Adds `api_key_hash` (unique) and backfills it from the plaintext keys. |
| `V4__drop_plaintext_api_key.sql` | Drops the plaintext `api_key` column. |
| `V5__sending_lease.sql` | Adds the SENDING lease: `claim_token`, `claimed_at`, partial index for the sweep. |
| `V6__published_at.sql` | Adds `published_at` and a partial index for unpublished `PENDING` rows. |
| `V7__drop_failed_status.sql` | Narrows the status `CHECK` to `PENDING`/`SENDING`/`SENT`/`DEAD`; aborts if any row is still `FAILED`. |

### `ddl-auto` is `validate`

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
```

**Hibernate never changes the schema.** It only checks, at startup, that the
entities match what Flyway built, and fails fast if they have drifted — for
example if you add a field to an entity and forget the migration. This replaces
the old `ddl-auto: update`, which silently mutated the database to match the
entities and left every environment's schema subtly different.

### Adding a migration

Create the next numbered file in `src/main/resources/db/migration`:

```
src/main/resources/db/migration/V3__add_notification_priority.sql
```

The naming is `V<version>__<description>.sql` — capital `V`, a **double**
underscore before the description. Then:

1. Write the DDL (`ALTER TABLE notifications ADD COLUMN priority varchar(255);`).
2. Make the matching change to the JPA entity.
3. Run `./mvnw test` — the tests build their schema from these same migrations, so
   a migration that disagrees with the entities fails there rather than in production.

**Destructive changes go in two migrations.** `V3`/`V4` are the worked example: `V3`
adds `api_key_hash`, backfills it and makes the old column nullable; `V4` drops the
plaintext column. Splitting them means the backfill can be verified while the original
data still exists, and an instance running the old code keeps working between the two
(the expand/contract pattern). You can stop at the expand half with
`--spring.flyway.target=3`.

Two rules worth internalising:

- **Never edit an already-applied migration.** Flyway checksums each file; changing
  one that has run makes validation fail on every environment that already applied
  it. Fix forward with a new version instead.
- **Never change the schema by editing `V1`.** It does not re-run on databases that
  already exist (see below), so the change would reach only brand-new environments.

### Why there is a baseline

The schema predates Flyway: it was originally created by `ddl-auto: update`, so
existing databases already had tables but no `flyway_schema_history`. The config
handles both cases:

```yaml
spring:
  flyway:
    baseline-on-migrate: true
    baseline-version: 1
```

- **An existing database** is stamped at version 1 on first run. `V1` is recorded as
  already applied and is *not* re-executed — so no `CREATE TABLE` runs against live
  data. Migrations from `V2` up are then applied normally.
- **An empty database** (a fresh `docker compose up`, or a Testcontainers database in
  the test suite) ignores baselining entirely and runs `V1` from scratch, building the
  whole schema with no manual SQL.

`V1` is a verbatim snapshot of the pre-Flyway schema, down to the Hibernate-generated
constraint names on `tenants`, so both paths end up with an identical schema.

To inspect what has been applied:

```bash
docker exec notification-postgres psql -U notifications -d notifications \
  -c "select version, description, type, success from flyway_schema_history order by installed_rank;"
```

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
  **Hashed API keys (v9):** the existing keys still authenticate and resolve the right
  tenant after the change; an unknown key is still `401`; the `tenants` table has **no**
  `api_key` column once `V4` has run; the stored value is the SHA-256 of the key and
  matches what Postgres's own `sha256()` produced in the `V3` backfill; and no column of
  any tenant row contains a plaintext key.
  **Crash recovery (v10):** a row claimed and then abandoned (the crash) is reclaimed
  by the scheduled reconciler, republished and `SENT` with exactly one provider call,
  and `notifications.reclaimed` increments; a stale lease holder's `SENT` and release
  writes match no row once its lease has been reclaimed; a consumer that delivers and
  then hangs past its lease drops its late result without touching the new holder's
  `SENT` (provider called twice, delivered once); a redelivery of a `SENT` row is still
  skipped; and a late dead-letter cannot overwrite `SENT`.
  **Publish failures (v11):** when the broker refuses the publish on create, the POST
  still answers `202`, the row is `PENDING` with no message, a same-key retry answers
  `200 PENDING`, and the reconciler republishes it to `SENT` with one provider call; a
  normal POST records `published_at`; republishing an id whose message is already queued
  leaves two copies on the queue and still delivers once. **Statuses:** the enum is
  exactly `PENDING`/`SENDING`/`SENT`/`DEAD`, the `CHECK` constraint allows exactly those,
  and writing `FAILED` is rejected.
  **Errors (v12):** every error is `application/problem+json`; missing and unknown API
  keys get distinct 401 details; bad query, path and body values name the field, the value
  and the allowed values; every failing body field is listed; over-long input is a 400
  that writes nothing; only the idempotency constraint is treated as an idempotency race
  (checked against real Postgres errors), and 8 concurrent same-key POSTs answer exactly
  one 202 and seven 200s; 429 carries `Retry-After`; an unexpected exception is a generic
  500 whose `errorId` matches the logged stack trace and whose body leaks nothing.
  **Unrecordable dead-letters (v13):** with every `DEAD` write for a `fail@example.com`
  failing, the DLQ listener tries 3 times and the message lands in
  `notifications.parking-lot` (before the fix: 4 tries, then discarded) while the row
  stays `PENDING`; once writes work and the dead grace has passed, the reconciler marks
  it `DEAD`, `notifications.dead` and `notifications.dead_reconciled` each +1, and the
  parked message stays. A message parked straight from the DLQ shows in the
  `queue.depth` gauge and the Prometheus scrape; a dead-letter for a missing row is
  acknowledged once, not parked; neither `DEAD` write touches a `SENT` or `SENDING` row,
  and the reconciler leaves an exhausted row alone inside the grace; `ParkingLot.replayOne()`
  sends a parked `DEAD` notification through delivery again (`SENT`, `attempts = 1`) and
  only acknowledges a parked `SENT` one; and startup refuses a queue the broker already
  has with different arguments, naming the `delete_queue` fix.
- `ConfigSecretsTest` — no Spring context, no containers. Reads the real
  `application.yml`, `.env.example` and `.gitignore` and fails if a credential stops
  being a `${ENV_VAR:default}` placeholder, if a variable the app reads is undocumented
  in `.env.example`, if `.env` stops being gitignored, or if a `.env` is ever tracked.
  It also exercises the placeholder itself: `ACME_API_KEY` set overrides the seeded key,
  absent falls back to the development default.
- `SimulatedProviderTest` — unit test for the stubbed provider: a repeat send of a
  delivered id is a no-op, a failed `flaky@` attempt is never recorded as delivered, and
  the bean is absent under the `prod` profile. Deliveries are asserted on the provider's
  log, so the production class carries no test-only accessors; the integration test's
  `RecordingProvider` wrapper does the call recording.

> The integration test needs a running Docker daemon (Testcontainers starts the
> Postgres, RabbitMQ and Redis containers automatically — no need to `docker compose
> up` first). Because of the backoff, the dead-letter test waits ~10s.

### Note on Docker Engine 29+

Testcontainers' bundled docker-java client defaults to Docker API `v1.32`, which
Docker Engine 29 rejects (its minimum is `1.44`). The `maven-surefire-plugin`
config in `pom.xml` pins `api.version=1.44` (and sets `DOCKER_HOST` so the
client strategy that reads it is selected) to work around this. On older Docker
engines this is harmless.
