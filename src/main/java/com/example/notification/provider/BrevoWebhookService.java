package com.example.notification.provider;

import com.example.notification.domain.DeliveryStatus;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.DateTimeException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Records relevant Brevo delivery callbacks for notifications this service sent. */
@Service
public class BrevoWebhookService {

    private static final Logger log = LoggerFactory.getLogger(BrevoWebhookService.class);
    private static final String CORRELATION_PREFIX = "notification-id:";
    private static final int MAX_REASON_LENGTH = 255;
    private static final EnumSet<DeliveryStatus> PERMANENT_FAILURES = EnumSet.of(
            DeliveryStatus.HARD_BOUNCED, DeliveryStatus.BLOCKED, DeliveryStatus.INVALID_EMAIL,
            DeliveryStatus.ERROR, DeliveryStatus.UNSUBSCRIBED);
    private static final Map<String, DeliveryStatus> DELIVERY_STATUSES = Map.of(
            "delivered", DeliveryStatus.DELIVERED,
            "deferred", DeliveryStatus.DEFERRED,
            "soft_bounce", DeliveryStatus.SOFT_BOUNCED,
            "hard_bounce", DeliveryStatus.HARD_BOUNCED,
            "blocked", DeliveryStatus.BLOCKED,
            "spam", DeliveryStatus.SPAM,
            "invalid_email", DeliveryStatus.INVALID_EMAIL,
            "error", DeliveryStatus.ERROR,
            "unsubscribed", DeliveryStatus.UNSUBSCRIBED);

    private final NotificationRepository notifications;
    private final NotificationMetrics metrics;

    public BrevoWebhookService(NotificationRepository notifications, NotificationMetrics metrics) {
        this.notifications = notifications;
        this.metrics = metrics;
    }

    /**
     * Stores a relevant callback once and updates the visible delivery outcome only when it is
     * newer than the event currently recorded for the notification. Unknown and unrelated events
     * are acknowledged without error so Brevo does not retry them indefinitely.
     */
    @Transactional
    public void record(BrevoWebhookPayload payload) {
        Optional<DeliveryStatus> deliveryStatus = deliveryStatus(payload.event());
        Optional<UUID> notificationId = notificationId(payload.customHeader());
        if (deliveryStatus.isEmpty() || notificationId.isEmpty()) {
            return;
        }
        if (payload.id() == null || payload.id() < 0 || payload.eventTimestampSeconds() == null) {
            log.warn("Ignoring malformed Brevo event for notification {}", notificationId.get());
            return;
        }
        // A retention job may have removed an old notification before Brevo finishes retrying
        // its callback. Acknowledge that event instead of causing provider retries forever.
        if (!notifications.existsById(notificationId.get())) {
            log.info("Ignoring Brevo event {} for missing notification {}", payload.id(), notificationId.get());
            return;
        }

        final Instant occurredAt;
        try {
            occurredAt = Instant.ofEpochSecond(payload.eventTimestampSeconds());
        } catch (DateTimeException exception) {
            log.warn("Ignoring Brevo event {} with invalid timestamp for notification {}",
                    payload.id(), notificationId.get());
            return;
        }

        if (notifications.recordBrevoEvent(payload.id(), notificationId.get(), payload.event(), occurredAt,
                truncate(payload.reason())) == 0) {
            return;
        }
        DeliveryStatus outcome = deliveryStatus.get();
        if (notifications.updateDeliveryStatus(notificationId.get(), outcome.name(), occurredAt) == 0) {
            return;
        }
        if (outcome == DeliveryStatus.DELIVERED
                && notifications.markProviderDelivered(notificationId.get(), occurredAt) == 1) {
            notifications.findById(notificationId.get()).ifPresent(notification -> {
                metrics.sent(metrics.tenantName(notification.getTenantId()), notification.getChannel().name());
                Duration elapsed = Duration.between(notification.getCreatedAt(), occurredAt);
                if (!elapsed.isNegative()) {
                    metrics.recordEndToEndLatency(elapsed);
                }
            });
        } else if (PERMANENT_FAILURES.contains(outcome)
                && notifications.markProviderDead(notificationId.get(), outcome.name(), occurredAt,
                truncate(payload.reason()) != null ? truncate(payload.reason()) : "Brevo reported " + outcome) == 1) {
            notifications.findById(notificationId.get()).ifPresent(notification ->
                    metrics.dead(metrics.tenantName(notification.getTenantId())));
        }
        log.info("Recorded Brevo {} event {} for notification {}", payload.event(), payload.id(), notificationId.get());
    }

    private static Optional<DeliveryStatus> deliveryStatus(String event) {
        if (event == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(DELIVERY_STATUSES.get(event.toLowerCase(Locale.ROOT)));
    }

    private static Optional<UUID> notificationId(String customHeader) {
        if (customHeader == null || !customHeader.startsWith(CORRELATION_PREFIX)) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(customHeader.substring(CORRELATION_PREFIX.length())));
        } catch (IllegalArgumentException exception) {
            return Optional.empty();
        }
    }

    private static String truncate(String text) {
        if (text == null) {
            return null;
        }
        return text.length() <= MAX_REASON_LENGTH ? text : text.substring(0, MAX_REASON_LENGTH);
    }
}
