package com.example.notification.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes notification ids onto the {@code notifications.send} queue for
 * asynchronous delivery. Messages are sent with persistent delivery mode
 * (Spring's default), so a durable queue retains them across a broker restart.
 *
 * <p>Every publish waits for a publisher confirm, and is mandatory: a send that returns
 * without an exception proves nothing on its own. Only a broker ack for a message that
 * was routed to a queue counts as published — anything else is reported as a failure
 * and the caller leaves the row for {@code LeaseReconciler} to republish.
 */
@Component
public class NotificationPublisher {

    private static final Logger log = LoggerFactory.getLogger(NotificationPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final Duration confirmTimeout;

    public NotificationPublisher(RabbitTemplate rabbitTemplate,
                                 @Value("${notifications.publisher.confirm-timeout:5s}") Duration confirmTimeout) {
        this.rabbitTemplate = rabbitTemplate;
        this.confirmTimeout = confirmTimeout;
    }

    /**
     * Publishes the id and waits up to {@code confirm-timeout} for the broker's confirm.
     *
     * @return true only if the broker acked the message and routed it to a queue; false if
     * the broker was unreachable, nacked it, could not route it, or did not confirm in
     * time. After a timeout the message may in fact be queued — republishing it is safe,
     * because delivery is idempotent on the id. Never throws.
     */
    public boolean publish(UUID notificationId) {
        CorrelationData correlation = new CorrelationData(notificationId.toString());
        try {
            // Default exchange ("") + routing key == queue name routes straight to the queue.
            rabbitTemplate.convertAndSend("", RabbitConfig.SEND_QUEUE, notificationId.toString(), correlation);
            CorrelationData.Confirm confirm =
                    correlation.getFuture().get(confirmTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.isAck()) {
                log.warn("Broker nacked notification {}: {}", notificationId, confirm.getReason());
                return false;
            }
            // Set before the ack completes the future, for a mandatory message no queue took.
            ReturnedMessage returned = correlation.getReturned();
            if (returned != null) {
                log.warn("Broker could not route notification {}: {} {}",
                        notificationId, returned.getReplyCode(), returned.getReplyText());
                return false;
            }
            return true;
        } catch (AmqpException e) {
            log.warn("Could not publish notification {}: {}", notificationId, e.getMessage());
        } catch (TimeoutException e) {
            log.warn("No broker confirm for notification {} within {}", notificationId, confirmTimeout);
        } catch (ExecutionException e) {
            log.warn("Confirm for notification {} failed: {}", notificationId, e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
