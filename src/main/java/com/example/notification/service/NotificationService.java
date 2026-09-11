package com.example.notification.service;

import com.example.notification.domain.Notification;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final NotificationProvider provider;
    private final NotificationPublisher publisher;
    private final NotificationMetrics metrics;

    public NotificationService(NotificationRepository repository,
                               NotificationProvider provider,
                               NotificationPublisher publisher,
                               NotificationMetrics metrics) {
        this.repository = repository;
        this.provider = provider;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    /**
     * Creates a notification, optionally deduplicated by an idempotency key.
     *
     * <p>With no key this always persists a new PENDING row and publishes it for
     * delivery ({@code created = true}).
     *
     * <p>With a key, at most one row exists per (tenant, key). The check-then-insert
     * is made race-safe by the UNIQUE constraint on
     * {@code (tenant_id, idempotency_key)} rather than by
     * the (TOCTOU-prone) pre-check alone: if two identical requests race, exactly
     * one INSERT succeeds and the other catches the constraint violation and returns
     * the winner's row ({@code created = false}). Duplicates are never published.
     */
    public CreateResult create(Notification notification, String idempotencyKey) {
        if (!StringUtils.hasText(idempotencyKey)) {
            repository.save(notification);
            publisher.publish(notification.getId());
            return CreateResult.created(notification);
        }

        // Fast path: an earlier request from THIS tenant with this key already
        // created the row. Scoped by tenant so one tenant never sees another's.
        UUID tenantId = notification.getTenantId();
        var existing = repository.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey);
        if (existing.isPresent()) {
            log.info("Idempotency-Key {} already maps to notification {}", idempotencyKey, existing.get().getId());
            return CreateResult.existing(existing.get());
        }

        notification.setIdempotencyKey(idempotencyKey);
        try {
            // saveAndFlush forces the INSERT now, so a UNIQUE violation surfaces here
            // (inside this try) rather than later at transaction commit.
            repository.saveAndFlush(notification);
        } catch (DataIntegrityViolationException e) {
            // Lost the race: a concurrent request inserted the same key first. The
            // winner's row is committed, so re-fetch and return it without publishing.
            Notification winner = repository.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency-Key " + idempotencyKey + " violated UNIQUE but no row found", e));
            log.info("Idempotency-Key {} race lost; returning existing notification {}",
                    idempotencyKey, winner.getId());
            return CreateResult.existing(winner);
        }

        publisher.publish(notification.getId());
        return CreateResult.created(notification);
    }

    /**
     * Attempts to deliver a single notification. Invoked by the RabbitMQ listener,
     * once per retry attempt. Increments the attempt counter, calls the provider,
     * and marks SENT on success.
     *
     * <p>On a provider failure the claim is released (back to PENDING, not FAILED) and
     * the exception is <b>rethrown</b> so the listener's retry interceptor can back off
     * and retry. When retries are finally exhausted the message is dead-lettered and
     * {@link #markDead(UUID, String)} flips it to DEAD.
     *
     * <p>Processing idempotency: delivery is gated by an atomic PENDING -> SENDING
     * claim, so a row that is already terminal (SENT/DEAD) or in flight on another
     * consumer is skipped without calling the provider. That makes a redelivery safe
     * even when it races the original delivery rather than following it.
     */
    public void attemptDelivery(UUID id) {
        // Atomic claim: PENDING -> SENDING in one conditional UPDATE, so of two
        // concurrent redeliveries exactly one proceeds to the provider. This is its
        // own short transaction and commits before the (slow) send below runs.
        if (repository.claimForSending(id) == 0) {
            Notification current = repository.findById(id).orElse(null);
            if (current == null) {
                // The id was published but the row is gone (e.g. wiped between tests).
                log.warn("No notification found for published id {}; skipping", id);
            } else {
                // SENT/DEAD, or another consumer holds the claim. Ack without resending.
                log.info("skipping already-processed {} (status {})", id, current.getStatus());
            }
            return;
        }

        // Read AFTER the claim so the in-memory copy carries SENDING; reading first
        // would make the merge below write the stale PENDING back over the claim.
        Notification notification = repository.findById(id).orElse(null);
        if (notification == null) {
            log.warn("No notification found for published id {}; skipping", id);
            return;
        }

        int attempt = notification.incrementAttempts();
        repository.save(notification);
        log.info("Delivery attempt {} for notification {}", attempt, id);

        String tenant = metrics.tenantName(notification.getTenantId());
        long startNanos = System.nanoTime();
        try {
            provider.send(notification); // throws on failure -> propagates for retry/dead-lettering
        } catch (RuntimeException e) {
            metrics.recordDeliveryLatency(Duration.ofNanos(System.nanoTime() - startNanos));
            metrics.failedAttempt(tenant);
            // Release the claim (SENDING -> PENDING) before rethrowing: the listener's
            // retry re-enters this method and must be able to win the claim again.
            // Without this every retry would skip and the message would never be
            // dead-lettered. A crash here instead leaves the row stuck in SENDING.
            repository.releaseClaim(id);
            throw e;
        }
        metrics.recordDeliveryLatency(Duration.ofNanos(System.nanoTime() - startNanos));

        Instant sentAt = Instant.now();
        notification.markSent(sentAt);
        repository.save(notification);

        metrics.sent(tenant, notification.getChannel().name());
        metrics.recordEndToEndLatency(Duration.between(notification.getCreatedAt(), sentAt));
    }

    /**
     * Marks a notification permanently failed after it has been dead-lettered.
     * Returns the recorded attempt count for logging.
     */
    public int markDead(UUID id, String reason) {
        Notification notification = repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
        notification.markDead(reason);
        repository.save(notification);
        metrics.dead(metrics.tenantName(notification.getTenantId()));
        return notification.getAttempts();
    }

    /**
     * Reads a notification on behalf of a tenant. A row belonging to a different
     * tenant is reported as <b>not found</b> rather than forbidden, so the endpoint
     * does not leak which ids exist.
     */
    public Notification get(UUID id, UUID tenantId) {
        Notification notification = repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
        if (!Objects.equals(notification.getTenantId(), tenantId)) {
            throw new NotificationNotFoundException(id);
        }
        return notification;
    }
}
