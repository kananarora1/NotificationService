package com.example.notification.domain;

public enum NotificationStatus {
    PENDING,
    /**
     * Claimed by a consumer and currently being delivered. Set by the atomic
     * PENDING -> SENDING conditional UPDATE, which is what stops two concurrent
     * redeliveries of the same id from both calling the provider. Released back to
     * PENDING if the attempt fails, so the listener's next retry can re-claim it.
     */
    SENDING,
    SENT,
    /** A single delivery attempt failed. Retained for completeness; v4 retries transient failures. */
    FAILED,
    /** Delivery permanently failed: retries were exhausted and the message was dead-lettered. */
    DEAD
}
