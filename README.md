# Notification Service (v2)

An **asynchronous, in-process** notification service. A request thread no longer
waits on the (slow) provider: `POST /notify` saves the notification as `PENDING`,
drops its id onto an in-memory queue, and returns `202 Accepted` immediately.
A small pool of background worker threads drains the queue and performs delivery,
flipping each notification to `SENT` or `FAILED` over time.

There is still **no external broker** (no RabbitMQ/Kafka/Redis) — the queue is a
plain `java.util.concurrent.LinkedBlockingQueue<UUID>` living inside this single
JVM. There is also no auth, rate limiting, or caching yet.

> The queue is in memory only: ids waiting (or being processed) when the process
> stops are lost. On shutdown the worker pool is drained gracefully, but the
> backlog is not persisted.

## Stack

- Java 21, Spring Boot 3.3 (Spring Web, Spring Data JPA, Validation)
- PostgreSQL (via Docker Compose)
- Maven

## Domain

A `Notification` has: `id` (UUID), `recipient`, `channel` (`EMAIL`/`SMS`/`PUSH`),
`message`, `status` (`PENDING`/`SENT`/`FAILED`), `createdAt`, and a nullable
`sentAt`.

Delivery is handled by `SimulatedProvider`: it sleeps ~800ms to mimic a slow
external API, logs `sent to {recipient} via {channel}`, and returns success.
It does **not** do any real delivery (no SMTP/SMS gateway). As a test hook it
throws for the recipient `fail@example.com`, which drives a notification to
`FAILED`.

## Architecture (v2)

```
POST /notify ──► save PENDING ──► enqueue(id) ──► 202 {id, PENDING}
                                      │
                          LinkedBlockingQueue<UUID>   (in-memory, this process)
                                      │
                 ┌────────────────────┴────────────────────┐
              worker-1   worker-2   worker-3   worker-4   (ExecutorService, 4 threads)
                 │
        take(id) ─► load ─► provider.send() ─► SENT + sentAt   (or FAILED on throw)
```

- `NotificationQueue` wraps the `LinkedBlockingQueue<UUID>` (`enqueue` / `take`).
- `NotificationWorker` starts a 4-thread `ExecutorService` on application
  startup. Each thread loops: take an id, load the notification, call the
  provider, and record the terminal state. A failed send is caught, logged, and
  marked `FAILED` — it never kills the worker thread. The pool is shut down
  gracefully when the application context closes.

## 1. Start Postgres

```bash
docker compose up -d
```

This runs Postgres 16 only, published on `localhost:5433` (host port `5433` maps
to the container's `5432`, chosen to avoid colliding with a Postgres that may
already be running natively on `5432`). Database/user/password are all set to
`notifications` (see `docker-compose.yml`).

## 2. Run the app

```bash
./mvnw spring-boot:run
# or, if you have Maven installed:
mvn spring-boot:run
```

The app listens on `http://localhost:8080`. The schema is created/updated
automatically (`spring.jpa.hibernate.ddl-auto=update`).

## 3. Try the endpoints

### POST /notify

Body: `{ recipient, channel, message }`. Saves as `PENDING`, enqueues for async
delivery, and returns `202 Accepted` with `{ id, status: "PENDING" }`
**immediately** (it does not block on the ~800ms provider call). Blank fields or
an invalid channel return `400`.

```bash
curl -i -X POST http://localhost:8080/notify \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"alice@example.com","channel":"EMAIL","message":"Hello!"}'
```

Example response (`202 Accepted`):

```json
{ "id": "0a3d...", "status": "PENDING" }
```

### GET /notify/{id}

Returns the current state of a notification, or `404` if the id is unknown.
Because delivery is asynchronous, the `status` reflects the live state and
transitions `PENDING -> SENT` (or `PENDING -> FAILED`) as the worker processes
it — poll this endpoint to observe the result.

```bash
curl -i http://localhost:8080/notify/0a3d...   # use the id from the POST
```

Example response once delivered:

```json
{
  "id": "0a3d...",
  "recipient": "alice@example.com",
  "channel": "EMAIL",
  "message": "Hello!",
  "status": "SENT",
  "createdAt": "2026-06-21T10:00:00Z",
  "sentAt": "2026-06-21T10:00:00.800Z"
}
```

## Tests

```bash
./mvnw test
```

- `NotificationIntegrationTest` — `@SpringBootTest` + MockMvc against in-memory
  H2 (no Docker needed). Uses [Awaitility](https://github.com/awaitility/awaitility)
  for the async waits: POST returns `202`/`PENDING` well under the 800ms provider
  latency, polling reaches `SENT`, `fail@example.com` reaches `FAILED`, N
  concurrent posts all return fast `202`s and eventually settle in a terminal
  state, and the unknown-id (`404`) / validation (`400`) paths still hold.
- `SimulatedProviderTest` — unit test for the stubbed provider, including the
  failing-recipient path.

> Tests use H2 to stay self-contained. The app itself runs against Postgres.
```
