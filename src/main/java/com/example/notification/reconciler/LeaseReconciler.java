package com.example.notification.reconciler;

import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

/**
 * Recovers notifications stranded in SENDING. A consumer that dies between claiming a
 * row and writing SENT (kill -9, OOM, a DB error after the claim) leaves the row
 * SENDING, and the broker's redelivery skips it because it is already claimed. This
 * sweep finds SENDING rows whose lease has expired, returns them to PENDING and
 * republishes their ids so delivery is retried.
 *
 * <p>Reclaiming trades a possible double-send for a guaranteed eventual send: the dead
 * consumer may have delivered before it died. The provider must therefore be idempotent
 * on the notification id — see {@code NotificationProvider}.
 *
 * <p>The republish happens <b>inside</b> the sweep's transaction, before it commits.
 * That ordering makes a failed sweep heal itself:
 * <ul>
 *   <li>A consumer that receives a republished id before the commit blocks on the row
 *       lock in its claim UPDATE, then sees the committed PENDING and wins the claim.</li>
 *   <li>If the sweep rolls back (a publish or the commit fails), the rows are still
 *       SENDING with an expired lease: any message already published is skipped, and the
 *       next sweep tries again.</li>
 * </ul>
 * Committing first and publishing after would instead risk a PENDING row with no message
 * behind it, which nothing would ever pick up.
 */
@Component
public class LeaseReconciler {

    private static final Logger log = LoggerFactory.getLogger(LeaseReconciler.class);

    private final NotificationRepository repository;
    private final NotificationPublisher publisher;
    private final NotificationMetrics metrics;
    private final TransactionTemplate transactionTemplate;
    private final ReconcilerProperties properties;

    public LeaseReconciler(NotificationRepository repository,
                           NotificationPublisher publisher,
                           NotificationMetrics metrics,
                           TransactionTemplate transactionTemplate,
                           ReconcilerProperties properties) {
        this.repository = repository;
        this.publisher = publisher;
        this.metrics = metrics;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
    }

    /**
     * Runs one sweep. Fixed delay, so a slow sweep never overlaps the next one on this
     * instance; across instances, {@code SKIP LOCKED} keeps them off each other's rows.
     * An exception is logged by the scheduler and the next sweep runs as usual.
     */
    @Scheduled(fixedDelayString = "${notifications.reconciler.sweep-interval:PT30S}")
    public void scheduledSweep() {
        sweep();
    }

    /** Reclaims and republishes up to one batch of expired leases; returns how many. */
    public int sweep() {
        List<UUID> reclaimed = transactionTemplate.execute(tx -> {
            List<UUID> expired = repository.lockExpiredLeases(
                    properties.leaseTimeout().toMillis(), properties.batchSize());
            if (!expired.isEmpty()) {
                repository.reclaim(expired);
                expired.forEach(publisher::publish); // before commit — see the class doc
            }
            return expired;
        });
        if (reclaimed == null || reclaimed.isEmpty()) {
            return 0;
        }
        metrics.reclaimed(reclaimed.size());
        log.warn("Reclaimed {} notification(s) stranded in SENDING past the {} lease; republished {}",
                reclaimed.size(), properties.leaseTimeout(), reclaimed);
        return reclaimed.size();
    }
}
