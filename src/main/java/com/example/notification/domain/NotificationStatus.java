package com.example.notification.domain;

public enum NotificationStatus {
    PENDING,
    SENT,
    /** A single delivery attempt failed. Retained for completeness; v4 retries transient failures. */
    FAILED,
    /** Delivery permanently failed: retries were exhausted and the message was dead-lettered. */
    DEAD
}
