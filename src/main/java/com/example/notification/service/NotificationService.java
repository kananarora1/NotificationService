package com.example.notification.service;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.UUID;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final NotificationProvider provider;
    private final NotificationPublisher publisher;

    public NotificationService(NotificationRepository repository,
                               NotificationProvider provider,
                               NotificationPublisher publisher) {
        this.repository = repository;
        this.provider = provider;
        this.publisher = publisher;
    }

    /**
     * Creates a notification, optionally deduplicated by an idempotency key.
     *
     * <p>With no key this always persists a new PENDING row and publishes it for
     * delivery ({@code created = true}).
     *
     * <p>With a key, at most one row exists per key. The check-then-insert is made
     * race-safe by the UNIQUE constraint on {@code idempotency_key} rather than by
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

        // Fast path: an earlier request with this key already created the row.
        var existing = repository.findByIdempotencyKey(idempotencyKey);
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
            Notification winner = repository.findByIdempotencyKey(idempotencyKey)
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
     * <p>On a provider failure the exception is <b>rethrown</b> so the listener's
     * retry interceptor can back off and retry — the status is deliberately left
     * PENDING (not FAILED). When retries are finally exhausted the message is
     * dead-lettered and {@link #markDead(UUID, String)} flips it to DEAD.
     *
     * <p>Processing idempotency: a message that has already reached a terminal state
     * (SENT or DEAD) is skipped without calling the provider, so a redelivery sends
     * at most once. Only PENDING notifications are delivered.
     */
    public void attemptDelivery(UUID id) {
        Notification notification = repository.findById(id).orElse(null);
        if (notification == null) {
            // The id was published but the row is gone (e.g. wiped between tests).
            log.warn("No notification found for published id {}; skipping", id);
            return;
        }

        if (notification.getStatus() != NotificationStatus.PENDING) {
            // Already SENT or DEAD — a duplicate/redelivered message. Ack without resending.
            log.info("skipping already-processed {} (status {})", id, notification.getStatus());
            return;
        }

        int attempt = notification.incrementAttempts();
        repository.save(notification);
        log.info("Delivery attempt {} for notification {}", attempt, id);

        provider.send(notification); // throws on failure -> propagates for retry/dead-lettering
        notification.markSent(Instant.now());
        repository.save(notification);
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
        return notification.getAttempts();
    }

    public Notification get(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
    }
}
