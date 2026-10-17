package com.example.notification.messaging;

import com.example.notification.service.NotificationService;
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
 *
 * <p>Runs on its own container factory: one consumer and a short retry (see
 * {@link RabbitConfig#deadLetterContainerFactory}). If the DEAD write still fails, the
 * message is rejected and the broker moves it to {@code notifications.parking-lot};
 * {@code LeaseReconciler} marks the row DEAD once the database is back.
 */
@Component
public class DeadLetterListener {

    private final NotificationService service;

    public DeadLetterListener(NotificationService service) {
        this.service = service;
    }

    @RabbitListener(queues = RabbitConfig.DEAD_LETTER_QUEUE,
            containerFactory = RabbitConfig.DEAD_LETTER_CONTAINER_FACTORY)
    public void onDeadLetter(String notificationId,
                             @Header(name = "x-death", required = false) List<Map<String, ?>> xDeath) {
        service.markDead(UUID.fromString(notificationId), describe(xDeath));
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
