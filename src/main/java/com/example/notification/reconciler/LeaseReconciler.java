package com.example.notification.reconciler;

import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

/**
 * Makes Postgres the source of truth for what still needs delivering: every sweep
 * finds rows the broker has no message for, and gives them one.
 *
 * <ol>
 *   <li><b>Expired SENDING leases.</b> A consumer that dies between claiming a row and
 *       writing SENT (kill -9, OOM, a DB error after the claim) leaves it SENDING, and
 *       the broker's redelivery skips it because it is already claimed. Rows whose lease
 *       has expired go back to PENDING with {@code published_at} cleared.</li>
 *   <li><b>Retry-exhausted PENDING rows.</b> After its last failed attempt a row is
 *       PENDING with {@code attempts} at the retry limit, and the dead-letter handler
 *       writes DEAD. If that write keeps failing (Postgres down) the handler parks the
 *       message, and nothing else would ever finish the row: it has a confirmed message,
 *       so step 3 skips it. Rows at the limit whose last attempt is older than the dead
 *       grace are marked DEAD here, once the database is reachable again.</li>
 *   <li><b>Unpublished PENDING rows.</b> A PENDING row with {@code published_at} null
 *       has no message known to be on the broker: its publish on create failed or was
 *       never confirmed, or step 1 just reclaimed it. Each is republished, and
 *       {@code published_at} is set only once the broker confirms it — an unconfirmed
 *       republish leaves the row for the next sweep. Runs after step 2, so a reclaimed
 *       row that was already on its last attempt is marked DEAD, not retried again.</li>
 * </ol>
 *
 * <p>Both steps can deliver a notification twice. A reclaimed row may have been
 * delivered just before its consumer died, and a republished one may already have a
 * message queued (the confirm was lost, not the message). Both are deliberate: the claim
 * skips a second copy of a row that is already SENDING or SENT, and the provider must be
 * idempotent on the id for the case where it already delivered — see
 * {@code NotificationProvider}.
 */
@Component
public class LeaseReconciler {

    private static final Logger log = LoggerFactory.getLogger(LeaseReconciler.class);

    private final NotificationRepository repository;
    private final NotificationPublisher publisher;
    private final NotificationMetrics metrics;
    private final TransactionTemplate transactionTemplate;
    private final ReconcilerProperties properties;
    /** The listener's retry limit: a row with this many attempts gets no more. */
    private final int maxAttempts;

    public LeaseReconciler(NotificationRepository repository,
                           NotificationPublisher publisher,
                           NotificationMetrics metrics,
                           TransactionTemplate transactionTemplate,
                           ReconcilerProperties properties,
                           RabbitProperties rabbitProperties) {
        this.repository = repository;
        this.publisher = publisher;
        this.metrics = metrics;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.maxAttempts = rabbitProperties.getListener().getSimple().getRetry().getMaxAttempts();
    }

    /**
     * One sweep. Fixed delay, so a slow sweep never overlaps the next one on this
     * instance; across instances, {@code SKIP LOCKED} keeps them off each other's rows.
     * Each pass runs even if an earlier one failed, and an exception is logged by the
     * scheduler and the next sweep runs as usual.
     */
    @Scheduled(fixedDelayString = "${notifications.reconciler.sweep-interval:PT30S}")
    public void scheduledSweep() {
        try {
            reclaimExpiredLeases();
        } finally {
            try {
                markExhaustedDead();
            } finally {
                republishUnpublished();
            }
        }
    }

    /** Returns up to one batch of expired SENDING leases to PENDING; returns how many. */
    public int reclaimExpiredLeases() {
        List<UUID> reclaimed = transactionTemplate.execute(tx -> {
            List<UUID> expired = repository.lockExpiredLeases(
                    properties.leaseTimeout().toMillis(), properties.batchSize());
            if (!expired.isEmpty()) {
                repository.reclaim(expired);
            }
            return expired;
        });
        if (reclaimed == null || reclaimed.isEmpty()) {
            return 0;
        }
        metrics.reclaimed(reclaimed.size());
        log.warn("Reclaimed {} notification(s) stranded in SENDING past the {} lease: {}",
                reclaimed.size(), properties.leaseTimeout(), reclaimed);
        return reclaimed.size();
    }

    /**
     * Marks DEAD up to one batch of PENDING rows that used up their retries but were
     * never recorded DEAD; returns how many. Each is counted in {@code notifications.dead}
     * like any other DEAD, and in {@code notifications.dead_reconciled}. The update is
     * guarded on PENDING, so if the dead-letter handler records a row first, this skips it.
     */
    public int markExhaustedDead() {
        List<UUID> dead = transactionTemplate.execute(tx -> {
            List<UUID> exhausted = repository.lockExhaustedPending(
                    maxAttempts, properties.deadGrace().toMillis(), properties.batchSize());
            if (!exhausted.isEmpty()) {
                repository.markExhaustedDead(exhausted,
                        "Delivery retries exhausted; marked DEAD by the reconciler "
                                + "because the dead-letter handler could not record it");
            }
            return exhausted;
        });
        if (dead == null || dead.isEmpty()) {
            return 0;
        }
        repository.findAllById(dead).forEach(n -> metrics.dead(metrics.tenantName(n.getTenantId())));
        metrics.deadReconciled(dead.size());
        log.warn("Marked {} notification(s) DEAD that had used up their {} attempts but were never recorded DEAD: {}",
                dead.size(), maxAttempts, dead);
        return dead.size();
    }

    /**
     * Republishes up to one batch of PENDING rows that have no confirmed message, older
     * than the publish grace; returns how many the broker confirmed.
     *
     * <p>Each row is locked, published and marked in its own short transaction, so a row
     * lock is held for one confirm round-trip only — a consumer that receives the message
     * blocks on its claim until that commit, and a whole batch held under one lock would
     * hold those consumers, and their pooled connections, for the entire batch. The
     * first unconfirmed publish ends the pass: the broker is not taking messages, and
     * the rest wait for the next sweep rather than each timing out in turn.
     */
    public int republishUnpublished() {
        List<UUID> candidates = repository.findUnpublishedPending(
                properties.publishGrace().toMillis(), properties.batchSize());
        int republished = 0;
        for (UUID id : candidates) {
            Boolean confirmed = transactionTemplate.execute(tx -> {
                if (repository.lockUnpublishedPending(id).isEmpty()) {
                    return null; // published, claimed or held by another instance since the scan
                }
                if (!publisher.publish(id)) {
                    return false;
                }
                repository.markPublished(id);
                return true;
            });
            if (Boolean.FALSE.equals(confirmed)) {
                log.warn("Republish of {} was not confirmed; leaving it and the rest of this batch for the next sweep",
                        id);
                break;
            }
            if (Boolean.TRUE.equals(confirmed)) {
                republished++;
            }
        }
        if (republished > 0) {
            metrics.republished(republished);
            log.warn("Republished {} PENDING notification(s) that had no confirmed message on the broker",
                    republished);
        }
        return republished;
    }
}
