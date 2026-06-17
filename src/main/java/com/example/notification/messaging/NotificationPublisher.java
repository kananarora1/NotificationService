package com.example.notification.messaging;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Publishes notification ids onto the {@code notifications.send} queue for
 * asynchronous delivery. Messages are sent with persistent delivery mode
 * (Spring's default), so a durable queue retains them across a broker restart.
 */
@Component
public class NotificationPublisher {

    private final RabbitTemplate rabbitTemplate;

    public NotificationPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publish(UUID notificationId) {
        // Default exchange + routing key == queue name routes straight to the queue.
        rabbitTemplate.convertAndSend(RabbitConfig.SEND_QUEUE, notificationId.toString());
    }
}
