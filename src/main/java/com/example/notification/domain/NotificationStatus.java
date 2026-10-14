package com.example.notification.domain;

public enum NotificationStatus {
    PENDING,
    /**
     * Claimed by a consumer and currently being delivered. Set by the atomic
     * PENDING -> SENDING conditional UPDATE, which is what stops two concurrent
     * redeliveries of the same id from both calling the provider. Released back to
     * PENDING if the attempt fails, so the listener's next retry can re-claim it.
     *
     * <p>The claim is a lease ({@code claim_token}, {@code claimed_at}): if the holder
     * dies before finishing, {@code LeaseReconciler} returns the row to PENDING once the
     * lease expires and republishes it.
     */
    SENDING,
    SENT,
    /** Delivery permanently failed: retries were exhausted and the message was dead-lettered. */
    DEAD
}
