package com.example.notification.messaging;

import com.example.notification.service.NotificationService;
import com.rabbitmq.client.GetResponse;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * Replays messages out of {@code notifications.parking-lot}, the holding queue for
 * dead-letters whose DEAD write failed (see {@link RabbitConfig}). Nothing calls this
 * automatically: parking means a person decides what happens next.
 */
@Component
public class ParkingLot {

    private final RabbitTemplate rabbitTemplate;
    private final NotificationService service;

    public ParkingLot(RabbitTemplate rabbitTemplate, NotificationService service) {
        this.rabbitTemplate = rabbitTemplate;
        this.service = service;
    }

    /**
     * One replayed message.
     *
     * @param requeued true if the notification was sent through delivery again; false if
     *                 there was nothing to replay (already SENT, in flight, or no such row)
     */
    public record Replayed(UUID id, boolean requeued) {
    }

    /**
     * Takes the oldest parked message and sends its notification through delivery again
     * from scratch ({@link NotificationService#requeueForDelivery}). The message is acked
     * only after that returns; if it throws (Postgres still down, say), the message goes
     * back to the parking lot and the exception propagates.
     *
     * @return empty if the parking lot is empty
     */
    public Optional<Replayed> replayOne() {
        return rabbitTemplate.execute(channel -> {
            GetResponse response = channel.basicGet(RabbitConfig.PARKING_LOT_QUEUE, false);
            if (response == null) {
                return Optional.empty();
            }
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            try {
                UUID id = UUID.fromString(new String(response.getBody(), StandardCharsets.UTF_8));
                boolean requeued = service.requeueForDelivery(id);
                channel.basicAck(deliveryTag, false);
                return Optional.of(new Replayed(id, requeued));
            } catch (RuntimeException e) {
                channel.basicNack(deliveryTag, false, true);
                throw e;
            }
        });
    }
}
