package com.example.notification.api;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Compact row for the {@code GET /notifications} list (demo UI). Carries the tenant
 * <em>name</em> rather than its id so the table is readable.
 */
public record NotificationSummary(
        UUID id,
        String tenant,
        String recipient,
        Channel channel,
        NotificationStatus status,
        int attempts,
        Instant createdAt,
        Instant sentAt
) {

    public static NotificationSummary of(Notification n, String tenant) {
        return new NotificationSummary(
                n.getId(),
                tenant,
                n.getRecipient(),
                n.getChannel(),
                n.getStatus(),
                n.getAttempts(),
                n.getCreatedAt(),
                n.getSentAt()
        );
    }
}
