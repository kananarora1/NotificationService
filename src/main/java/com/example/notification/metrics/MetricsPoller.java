package com.example.notification.metrics;

import com.example.notification.domain.NotificationStatus;
import com.example.notification.messaging.RabbitConfig;
import com.example.notification.reconciler.ReconcilerProperties;
import com.example.notification.repository.NotificationRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Backs the "expensive to compute" gauges with values refreshed on a fixed schedule
 * rather than on every Prometheus scrape:
 * <ul>
 *   <li>{@code notifications.pending} — count of PENDING rows (a COUNT query we do not
 *       want to run per scrape on a large table).</li>
 *   <li>{@code notifications.sending} — count of SENDING rows: claimed and in flight.</li>
 *   <li>{@code notifications.stuck_sending} — SENDING rows past their lease, i.e. what
 *       the reconciler should be recovering. It drains every sweep, so a value that
 *       stays above zero means the reconciler itself is not working.</li>
 *   <li>{@code notifications.unpublished_pending} — PENDING rows with no confirmed
 *       message, past the publish grace. The reconciler republishes these every sweep, so
 *       a value that stays above zero means the broker is not confirming publishes.</li>
 *   <li>{@code queue.depth} — RabbitMQ message counts for the work queue and the DLQ
 *       (a broker round-trip we cache).</li>
 * </ul>
 * The gauges read cheap {@link AtomicLong}s; a scheduled task updates them every 5s.
 */
@Component
public class MetricsPoller {

    private static final Logger log = LoggerFactory.getLogger(MetricsPoller.class);

    private final NotificationRepository repository;
    private final AmqpAdmin amqpAdmin;
    private final ReconcilerProperties reconcilerProperties;

    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong sending = new AtomicLong();
    private final AtomicLong stuckSending = new AtomicLong();
    private final AtomicLong unpublishedPending = new AtomicLong();
    private final AtomicLong sendQueueDepth = new AtomicLong();
    private final AtomicLong dlqDepth = new AtomicLong();

    public MetricsPoller(MeterRegistry registry,
                         NotificationRepository repository,
                         AmqpAdmin amqpAdmin,
                         ReconcilerProperties reconcilerProperties) {
        this.repository = repository;
        this.amqpAdmin = amqpAdmin;
        this.reconcilerProperties = reconcilerProperties;

        Gauge.builder("notifications.pending", pending, AtomicLong::get)
                .description("Notifications currently in PENDING status")
                .register(registry);
        Gauge.builder("notifications.sending", sending, AtomicLong::get)
                .description("Notifications currently in SENDING status (claimed, in flight)")
                .register(registry);
        Gauge.builder("notifications.stuck_sending", stuckSending, AtomicLong::get)
                .description("SENDING notifications whose lease has expired, awaiting the reconciler")
                .register(registry);
        Gauge.builder("notifications.unpublished_pending", unpublishedPending, AtomicLong::get)
                .description("PENDING notifications with no confirmed message past the publish grace")
                .register(registry);
        Gauge.builder("queue.depth", sendQueueDepth, AtomicLong::get)
                .description("Messages ready in the queue")
                .tag("queue", RabbitConfig.SEND_QUEUE)
                .register(registry);
        Gauge.builder("queue.depth", dlqDepth, AtomicLong::get)
                .description("Messages ready in the queue")
                .tag("queue", RabbitConfig.DEAD_LETTER_QUEUE)
                .register(registry);
    }

    @Scheduled(fixedRate = 5000)
    public void poll() {
        try {
            pending.set(repository.countByStatus(NotificationStatus.PENDING));
            sending.set(repository.countByStatus(NotificationStatus.SENDING));
            stuckSending.set(repository.countExpiredLeases(reconcilerProperties.leaseTimeout().toMillis()));
            unpublishedPending.set(repository.countUnpublishedPending(reconcilerProperties.publishGrace().toMillis()));
            sendQueueDepth.set(queueDepth(RabbitConfig.SEND_QUEUE));
            dlqDepth.set(queueDepth(RabbitConfig.DEAD_LETTER_QUEUE));
        } catch (RuntimeException e) {
            // Never let a metrics refresh failure disrupt the app; keep the last values.
            log.warn("Failed to refresh metric gauges", e);
        }
    }

    private long queueDepth(String queue) {
        // getQueueProperties returns null if the queue doesn't exist yet.
        Properties props = amqpAdmin.getQueueProperties(queue);
        if (props == null) {
            return 0L;
        }
        Object count = props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        return count instanceof Number number ? number.longValue() : 0L;
    }
}
