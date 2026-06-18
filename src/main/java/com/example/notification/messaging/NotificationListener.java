package com.example.notification.messaging;

import com.example.notification.service.NotificationService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumes notification ids from {@code notifications.send} and delivers them.
 *
 * <p>Concurrency, prefetch (fair dispatch), auto acknowledgement and retry/backoff
 * are configured under {@code spring.rabbitmq.listener.simple.*}. When
 * {@link NotificationService#attemptDelivery(UUID)} throws (a provider failure),
 * the retry interceptor backs off and re-invokes this method. Once
 * {@code retry.max-attempts} is reached the message is rejected without requeue and
 * RabbitMQ dead-letters it to {@code notifications.dlq}.
 */
@Component
public class NotificationListener {

    private final NotificationService service;

    public NotificationListener(NotificationService service) {
        this.service = service;
    }

    @RabbitListener(queues = RabbitConfig.SEND_QUEUE)
    public void onNotification(String notificationId) {
        service.attemptDelivery(UUID.fromString(notificationId));
    }
}
