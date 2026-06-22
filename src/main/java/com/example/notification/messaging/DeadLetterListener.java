package com.example.notification.messaging;

import com.example.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Consumes messages that exhausted their retries and were dead-lettered to
 * {@code notifications.dlq}. It records the permanent failure by flipping the
 * notification's status to DEAD and storing a {@code failureReason}, so a
 * dead-lettered notification is never silently dropped.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final NotificationService service;

    public DeadLetterListener(NotificationService service) {
        this.service = service;
    }

    @RabbitListener(queues = RabbitConfig.DEAD_LETTER_QUEUE)
    public void onDeadLetter(String notificationId,
                             @Header(name = "x-death", required = false) List<Map<String, ?>> xDeath) {
        UUID id = UUID.fromString(notificationId);
        String reason = describe(xDeath);
        int attempts = service.markDead(id, reason);
        log.warn("Notification {} dead-lettered after {} attempts: {}", id, attempts, reason);
    }

    /** Builds a short human-readable reason from RabbitMQ's x-death header, if present. */
    private static String describe(List<Map<String, ?>> xDeath) {
        if (xDeath == null || xDeath.isEmpty()) {
            return "Dead-lettered: delivery retries exhausted";
        }
        Map<String, ?> first = xDeath.get(0);
        Object reason = first.get("reason");
        Object queue = first.get("queue");
        return String.format("Dead-lettered from '%s' (reason=%s): delivery retries exhausted",
                queue, reason);
    }
}
