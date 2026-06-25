# Notification Service (v4)

An **asynchronous** notification service backed by **RabbitMQ**, with **Postgres**
as the source of truth. `POST /notify` saves the notification as `PENDING`,
publishes its id to a durable queue, and returns `202 Accepted` immediately. A
pool of `@RabbitListener` consumers drains the queue and performs delivery.

v4 adds **retries with backoff** and a **dead-letter queue**: a transient provider
failure is retried a few times with increasing delay, and a message that still
fails after all attempts is **dead-lettered** (never silently dropped) and its
`Notification` row is marked `DEAD`. There is still no Redis, auth, rate limiting,
or caching yet.

## Stack

- Java 21, Spring Boot 3.3 (Spring Web, Spring Data JPA, Validation, AMQP)
- PostgreSQL — source of truth
- RabbitMQ — message broker (with retries + dead-letter queue)
- Maven

## Domain

A `Notification` has: `id` (UUID), `recipient`, `channel` (`EMAIL`/`SMS`/`PUSH`),
`message`, `status`, `attempts` (int), a nullable `failureReason`, `createdAt`,
and a nullable `sentAt`.

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

## 1. Start Postgres + RabbitMQ

```bash
docker compose up -d
```

This starts:

- **Postgres 16** on `localhost:5433` (host `5433` → container `5432`, to avoid
  colliding with a native Postgres on `5432`). DB/user/password are all
  `notifications`.
- **RabbitMQ 3.13** with the management plugin: AMQP on `localhost:5672`, and the
  management UI on **http://localhost:15672** (login **guest / guest**).

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

Body: `{ recipient, channel, message }`. Saves as `PENDING`, publishes to
`notifications.send`, and returns `202 Accepted` with `{ id, status: "PENDING" }`
**immediately**. Blank fields or an invalid channel return `400`.

```bash
# normal — succeeds on the first attempt
curl -i -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"alice@example.com","channel":"EMAIL","message":"Hello!"}'

# transient failure — fails twice then recovers to SENT
curl -s -X POST http://localhost:8080/notify -H 'Content-Type: application/json' \
  -d '{"recipient":"flaky@example.com","channel":"EMAIL","message":"retry me"}'

# permanent failure — ends DEAD and lands in notifications.dlq
curl -s -X POST http://localhost:8080/notify -H 'Content-Type: application/json' \
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

- `NotificationIntegrationTest` — `@SpringBootTest` + MockMvc, with **Postgres and
  RabbitMQ provided by [Testcontainers](https://testcontainers.com/)** (via
  `@ServiceConnection`) and [Awaitility](https://github.com/awaitility/awaitility)
  for async waits. Covers: a normal recipient ends `SENT` with `attempts == 1`;
  `flaky@example.com` recovers to `SENT` with `attempts >= 3` (retry recovers a
  transient failure); `fail@example.com` ends `DEAD` with a `failureReason` and is
  dead-lettered (only the DLQ consumer sets `DEAD`, so reaching it proves the
  message landed in `notifications.dlq`); the backoff gap between attempts
  increases; and the unknown-id (`404`) / validation (`400`) paths still hold.
- `SimulatedProviderTest` — unit test for the stubbed provider.

> The integration test needs a running Docker daemon (Testcontainers starts the
> Postgres and RabbitMQ containers automatically — no need to `docker compose up`
> first). Because of the backoff, the dead-letter test waits ~10s.

### Note on Docker Engine 29+

Testcontainers' bundled docker-java client defaults to Docker API `v1.32`, which
Docker Engine 29 rejects (its minimum is `1.44`). The `maven-surefire-plugin`
config in `pom.xml` pins `api.version=1.44` (and sets `DOCKER_HOST` so the
client strategy that reads it is selected) to work around this. On older Docker
engines this is harmless.
