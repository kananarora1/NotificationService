package com.example.notification.service;

import com.example.notification.domain.Notification;

/**
 * Outcome of {@link NotificationService#create}. {@code created} is true when a new
 * notification was persisted and published (→ 202 Accepted), and false when an
 * existing one was returned for a duplicate idempotency key (→ 200 OK).
 */
public record CreateResult(Notification notification, boolean created) {

    static CreateResult created(Notification notification) {
        return new CreateResult(notification, true);
    }

    static CreateResult existing(Notification notification) {
        return new CreateResult(notification, false);
    }
}
