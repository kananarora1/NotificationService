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

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Backs the "expensive to compute" gauges with values refreshed on a fixed schedule
 * rather than on every Prometheus scrape:
 * <ul>
 *   <li>{@code notifications.pending} — count of PENDING rows (a COUNT query we do not
 *       want to run per scrape on a large table).</li>
 *   <li>{@code notifications.sending} — claimed or awaiting provider confirmation.</li>
 *   <li>{@code notifications.overdue_confirmation} — Brevo-accepted rows older than 30 minutes.</li>
 *   <li>{@code notifications.stuck_sending} — SENDING rows past their lease, i.e. what
 *       the reconciler should be recovering. It drains every sweep, so a value that
 *       stays above zero means the reconciler itself is not working.</li>
 *   <li>{@code notifications.unpublished_pending} — PENDING rows with no confirmed
 *       message, past the publish grace. The reconciler republishes these every sweep, so
 *       a value that stays above zero means the broker is not confirming publishes.</li>
 *   <li>{@code queue.depth} — RabbitMQ message counts for the work queue, the DLQ and the
 *       parking lot (a broker round-trip we cache). Nothing consumes the parking lot, so
 *       any depth there is a dead-letter waiting for a person.</li>
 * </ul>
 * The gauges read cheap {@link AtomicLong}s, updated every 5s by two scheduled tasks: one
 * for the database counts, one for the queue depths. They are separate because a Postgres
 * outage is exactly when the parking lot fills, and while Postgres hangs every query
 * takes the pool's 30s connection timeout to fail — the depths must not wait behind them.
 */
@Component
public class MetricsPoller {

    private static final Logger log = LoggerFactory.getLogger(MetricsPoller.class);
    private static final Duration CONFIRMATION_TIMEOUT = Duration.ofMinutes(30);

    private final NotificationRepository repository;
    private final AmqpAdmin amqpAdmin;
    private final ReconcilerProperties reconcilerProperties;

    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong sending = new AtomicLong();
    private final AtomicLong stuckSending = new AtomicLong();
    private final AtomicLong awaitingConfirmation = new AtomicLong();
    private final AtomicLong overdueConfirmation = new AtomicLong();
    private final AtomicLong unpublishedPending = new AtomicLong();
    private final AtomicLong sendQueueDepth = new AtomicLong();
    private final AtomicLong dlqDepth = new AtomicLong();
    private final AtomicLong parkingLotDepth = new AtomicLong();
    private final AtomicLong databaseLastSuccess = new AtomicLong();
    private final AtomicLong queueLastSuccess = new AtomicLong();

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
        Gauge.builder("notifications.awaiting_confirmation", awaitingConfirmation, AtomicLong::get)
                .description("Provider-accepted notifications waiting for a delivery webhook")
                .register(registry);
        Gauge.builder("notifications.overdue_confirmation", overdueConfirmation, AtomicLong::get)
                .description("Provider-accepted notifications waiting over 30 minutes for a delivery webhook")
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
        Gauge.builder("queue.depth", parkingLotDepth, AtomicLong::get)
                .description("Messages ready in the queue")
                .tag("queue", RabbitConfig.PARKING_LOT_QUEUE)
                .register(registry);
        Gauge.builder("metrics.poll.last_success_timestamp_seconds", databaseLastSuccess, AtomicLong::get)
                .description("Unix timestamp of the last complete database metrics refresh")
                .tag("source", "database")
                .register(registry);
        Gauge.builder("metrics.poll.last_success_timestamp_seconds", queueLastSuccess, AtomicLong::get)
                .description("Unix timestamp of the last complete RabbitMQ metrics refresh")
                .tag("source", "rabbitmq")
                .register(registry);
    }

    @Scheduled(fixedRate = 5000)
    public void pollDatabase() {
        try {
            long pendingCount = repository.countByStatus(NotificationStatus.PENDING);
            long sendingCount = repository.countByStatus(NotificationStatus.SENDING);
            long stuckCount = repository.countExpiredLeases(reconcilerProperties.leaseTimeout().toMillis());
            long awaitingCount = repository.countAwaitingConfirmation();
            long overdueCount = repository.countOverdueConfirmations(CONFIRMATION_TIMEOUT.toMillis());
            long unpublishedCount = repository.countUnpublishedPending(reconcilerProperties.publishGrace().toMillis());
            pending.set(pendingCount);
            sending.set(sendingCount);
            stuckSending.set(stuckCount);
            awaitingConfirmation.set(awaitingCount);
            overdueConfirmation.set(overdueCount);
            unpublishedPending.set(unpublishedCount);
            databaseLastSuccess.set(Instant.now().getEpochSecond());
        } catch (RuntimeException e) {
            log.warn("Failed to refresh the database gauges", e);
        }
    }

    @Scheduled(fixedRate = 5000)
    public void pollQueues() {
        try {
            long sendCount = queueDepth(RabbitConfig.SEND_QUEUE);
            long deadLetterCount = queueDepth(RabbitConfig.DEAD_LETTER_QUEUE);
            long parkingCount = queueDepth(RabbitConfig.PARKING_LOT_QUEUE);
            sendQueueDepth.set(sendCount);
            dlqDepth.set(deadLetterCount);
            parkingLotDepth.set(parkingCount);
            queueLastSuccess.set(Instant.now().getEpochSecond());
        } catch (RuntimeException e) {
            log.warn("Failed to refresh the queue depth gauges", e);
        }
    }

    private long queueDepth(String queue) {
        Properties props = amqpAdmin.getQueueProperties(queue);
        if (props == null) {
            throw new IllegalStateException("Queue unavailable: " + queue);
        }
        Object count = props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        if (!(count instanceof Number number)) {
            throw new IllegalStateException("Queue depth unavailable: " + queue);
        }
        return number.longValue();
    }
}
