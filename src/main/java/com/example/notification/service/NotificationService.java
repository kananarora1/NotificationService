package com.example.notification.service;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
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

    /** UNIQUE (tenant_id, idempotency_key): the constraint that decides an idempotency race. */
    static final String IDEMPOTENCY_KEY_CONSTRAINT = "uk_notifications_tenant_idempotency_key";

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
     * the winner's row ({@code created = false}). A key may only be retried with the
     * same recipient, channel and message; a different request is rejected with a
     * conflict rather than silently returning the wrong notification. Duplicates are
     * never published.
     *
     * <p>Accepted means <b>durably</b> accepted, not "already on the queue". Once the row
     * is committed nothing here fails the request: if the publish fails or the broker
     * does not confirm it (RabbitMQ down, say), the row stays PENDING with
     * {@code published_at} null and {@code LeaseReconciler} republishes it after its
     * grace period. Answering 500 instead would invite a retry that, without an
     * Idempotency-Key, creates a second notification for one that will be delivered.
     */
    public CreateResult create(Notification notification, String idempotencyKey) {
        if (!StringUtils.hasText(idempotencyKey)) {
            repository.save(notification);
            publish(notification.getId());
            return CreateResult.created(notification);
        }

        // Fast path: an earlier request from THIS tenant with this key already
        // created the row. Scoped by tenant so one tenant never sees another's.
        UUID tenantId = notification.getTenantId();
        var existing = repository.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey);
        if (existing.isPresent()) {
            return existingOrConflict(existing.get(), notification);
        }

        notification.setIdempotencyKey(idempotencyKey);
        try {
            // saveAndFlush forces the INSERT now, so a UNIQUE violation surfaces here
            // (inside this try) rather than later at transaction commit.
            repository.saveAndFlush(notification);
        } catch (DataIntegrityViolationException e) {
            if (!DataIntegrityViolations.isViolationOf(e, IDEMPOTENCY_KEY_CONSTRAINT)) {
                // Not the race: some other constraint failed, and re-fetching by key
                // would only report it as a missing row. Let it surface as itself.
                throw e;
            }
            // Lost the race: a concurrent request inserted the same key first. The
            // winner's row is committed, so re-fetch and return it without publishing.
            Notification winner = repository.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency-Key " + idempotencyKey + " violated UNIQUE but no row found", e));
            return existingOrConflict(winner, notification);
        }

        publish(notification.getId());
        return CreateResult.created(notification);
    }

    /**
     * Returns the prior result for a true retry, but never lets an accidental key reuse
     * turn one notification request into another. This is also used after an insert
     * race: the concurrent winner must describe the same request before it is returned.
     */
    private CreateResult existingOrConflict(Notification existing, Notification requested) {
        if (!sameRequest(existing, requested)) {
            throw new IdempotencyConflictException();
        }
        log.info("Returning existing notification {} for an idempotent retry", existing.getId());
        return CreateResult.existing(existing);
    }

    private static boolean sameRequest(Notification existing, Notification requested) {
        return Objects.equals(existing.getRecipient(), requested.getRecipient())
                && existing.getChannel() == requested.getChannel()
                && Objects.equals(existing.getMessage(), requested.getMessage());
    }

    /**
     * Publishes a committed row's id and records {@code published_at} once the broker
     * confirms it. Never throws: on any failure the row keeps {@code published_at} null,
     * which is exactly what the reconciler looks for.
     *
     * <p>The fast path above may hand a client this row as PENDING before it is published.
     * That is no longer a dead end: an unpublished PENDING row is always republished.
     */
    private void publish(UUID id) {
        if (!publisher.publish(id)) {
            return;
        }
        try {
            repository.markPublished(id);
        } catch (DataAccessException e) {
            // The message is on the broker but unrecorded; the reconciler will publish a
            // second copy, which is skipped at the claim.
            log.warn("Notification {} was published but published_at could not be recorded", id, e);
        }
    }

    /**
     * Attempts to deliver a single notification. Invoked by the RabbitMQ listener,
     * once per retry attempt. A callback-based provider stays SENDING after hand-off;
     * only its delivered webhook marks SENT. The simulator marks SENT immediately.
     *
     * <p>On a provider failure the claim is released (back to PENDING) and
     * the exception is <b>rethrown</b> so the listener's retry interceptor can back off
     * and retry. When retries are finally exhausted the message is dead-lettered and
     * {@link #markDead(UUID, String)} flips it to DEAD.
     *
     * <p>Processing idempotency: delivery is gated by an atomic PENDING -> SENDING
     * claim, so a row that is already terminal (SENT/DEAD) or in flight on another
     * consumer is skipped without calling the provider.
     *
     * <p>The claim is a <b>lease</b>, not a lock held forever. If this consumer dies
     * anywhere before the provider-acceptance write, {@code LeaseReconciler} returns
     * the row to PENDING once the lease expires and republishes it. That makes delivery
     * at-least-once: the provider may already have accepted the notification before the
     * crash, so it can be sent again — which is why {@link NotificationProvider} must be
     * idempotent on the notification id. In exchange, nothing is stranded in SENDING.
     *
     * <p>Every write after the claim is conditional on this attempt's lease token. If
     * one matches no row, the lease was reclaimed while this attempt was running (it
     * outlived the lease timeout): the row now belongs to the consumer that claimed it
     * next, so this attempt logs, writes nothing and acks.
     */
    public void attemptDelivery(UUID id) {
        // Atomic claim: PENDING -> SENDING in one conditional UPDATE, so of two
        // concurrent redeliveries exactly one proceeds to the provider. This is its
        // own short transaction and commits before the (slow) send below runs.
        UUID token = UUID.randomUUID();
        if (repository.claimForSending(id, token) == 0) {
            Notification current = repository.findById(id).orElse(null);
            if (current == null) {
                // The id was published but the row is gone (e.g. wiped between tests).
                log.warn("No notification found for published id {}; skipping", id);
            } else if (current.getStatus() == NotificationStatus.SENDING) {
                // In flight on another consumer — or stranded by one that died. Either
                // way it is not ours: the reconciler recovers it if the lease expires.
                log.info("skipping {}: already claimed (lease since {})", id, current.getClaimedAt());
            } else {
                // SENT/DEAD. Ack without resending.
                log.info("skipping already-processed {} (status {})", id, current.getStatus());
            }
            return;
        }

        // Read AFTER the claim, for the provider and the metrics only. Never saved back:
        // all writes from here on are guarded UPDATEs, so this copy cannot overwrite them.
        Notification notification = repository.findById(id).orElse(null);
        if (notification == null) {
            log.warn("No notification found for published id {}; skipping", id);
            return;
        }

        int attempt = notification.getAttempts(); // incremented by the claim itself
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
            // dead-lettered. A crash here instead leaves the row in SENDING until the
            // lease expires and the reconciler recovers it.
            if (repository.releaseClaim(id, token) == 0) {
                // The reconciler already reclaimed and republished it, so another copy of
                // the message owns delivery now. Retrying this one would only compete.
                log.warn("Lease on {} was reclaimed during failed attempt {}; dropping this copy",
                        id, attempt, e);
                return;
            }
            throw e;
        }
        metrics.recordDeliveryLatency(Duration.ofNanos(System.nanoTime() - startNanos));

        if (provider.requiresDeliveryConfirmation()) {
            if (repository.markProviderAccepted(id, token) == 0) {
                log.info("Provider hand-off for {} was already resolved or its lease changed", id);
            } else {
                log.info("Provider accepted {}; awaiting delivery webhook", id);
            }
            return;
        }

        Instant sentAt = Instant.now();
        if (repository.markSent(id, token, sentAt) == 0) {
            // The send outlived the lease and the row was reclaimed: the next holder will
            // send again (the provider dedupes on the id) and records SENT itself.
            log.warn("Lease on {} was reclaimed before attempt {} could record SENT; "
                    + "leaving the row to its new holder", id, attempt);
            return;
        }

        metrics.sent(tenant, notification.getChannel().name());
        metrics.recordEndToEndLatency(Duration.between(notification.getCreatedAt(), sentAt));
    }

    /**
     * Marks a notification permanently failed after it has been dead-lettered. Only a
     * PENDING row qualifies (see {@link NotificationRepository#markDead}): if another
     * delivery has since claimed or completed it, the late dead-letter is logged and
     * ignored rather than overwriting that outcome. A dead-letter for a row that no longer
     * exists has nothing to record and is ignored too.
     *
     * <p>A database failure propagates, so the dead-letter handler retries and then parks
     * the message (see {@code RabbitConfig}). {@code notifications.dead} is counted only
     * once DEAD is written.
     */
    public void markDead(UUID id, String reason) {
        if (repository.markDead(id, reason) == 0) {
            repository.findById(id).ifPresentOrElse(
                    current -> log.warn("Ignoring dead-letter for {}: it is {}, not PENDING", id, current.getStatus()),
                    () -> log.warn("Ignoring dead-letter for {}: no such notification", id));
            return;
        }
        Notification dead = repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
        metrics.dead(metrics.tenantName(dead.getTenantId()));
        log.warn("Notification {} dead-lettered after {} attempts: {}", id, dead.getAttempts(), reason);
    }

    /**
     * Sends a DEAD notification (or one stranded PENDING past its retries) through
     * delivery again from scratch: the row goes back to PENDING with its attempts and
     * failure reason cleared, and its id is published onto {@code notifications.send}.
     * An unconfirmed publish is left to the reconciler, as on create.
     *
     * @return false, changing nothing, if the row is missing, in flight (SENDING) or
     * already SENT
     */
    public boolean requeueForDelivery(UUID id) {
        if (repository.resetForRedelivery(id) == 0) {
            log.info("Not requeueing {}: {}", id, repository.findById(id)
                    .map(n -> "it is " + n.getStatus()).orElse("no such notification"));
            return false;
        }
        log.info("Requeued notification {} for delivery", id);
        publish(id);
        return true;
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
