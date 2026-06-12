package com.example.notification.api;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Full read model returned from GET /notify/{id}. Keeps the JPA entity off the wire.
 */
public record NotificationView(
        UUID id,
        String recipient,
        Channel channel,
        String message,
        NotificationStatus status,
        Instant createdAt,
        Instant sentAt
) {

    public static NotificationView from(Notification n) {
        return new NotificationView(
                n.getId(),
                n.getRecipient(),
                n.getChannel(),
                n.getMessage(),
                n.getStatus(),
                n.getCreatedAt(),
                n.getSentAt()
        );
    }
}
