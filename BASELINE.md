# Baseline Submission

- **Student name:** Kanan Arora
- **Student ID:** 23bcs10180
- **Project name:** Notification Service
- **Repository:** https://github.com/kananarora1/NotificationService
- **Baseline commit:** [`d40f9bf9d73c0287399baddf2cb7ea15be825870`](https://github.com/kananarora1/NotificationService/commit/d40f9bf9d73c0287399baddf2cb7ea15be825870)

## Application

Notification Service is an asynchronous notification system backed by PostgreSQL,
RabbitMQ, and Redis. It accepts notification requests, persists them, publishes
notification IDs to a durable queue, and delivers them through a provider with
retries, idempotency, tenant authentication, rate limiting, and dead-letter
handling.

## DevOps features already present at the baseline

- Docker Compose services for PostgreSQL, RabbitMQ, Redis, Prometheus, and Grafana.
- Maven-based Java 21 build and automated application tests.
- Environment-based configuration and `.env.example` secret setup.
- Flyway database migrations.
- Micrometer and Prometheus application metrics.
- Grafana provisioning and a notification-service dashboard.
- Retry, dead-letter, idempotency, lease-recovery, and publish-recovery behavior.

The baseline commit is preserved in repository history so the final DevOps
improvements can be reviewed against this starting point.
