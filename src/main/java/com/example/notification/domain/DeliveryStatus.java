package com.example.notification.domain;

/**
 * The latest provider-reported outcome after a notification has been accepted for delivery.
 * This is separate from {@link NotificationStatus}, which models the service's own queue and
 * retry lifecycle.
 */
public enum DeliveryStatus {
    ACCEPTED,
    DELIVERED,
    DEFERRED,
    SOFT_BOUNCED,
    HARD_BOUNCED,
    BLOCKED,
    SPAM,
    INVALID_EMAIL,
    ERROR,
    UNSUBSCRIBED
}
