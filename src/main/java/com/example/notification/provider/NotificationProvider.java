package com.example.notification.provider;

import com.example.notification.domain.Notification;

public interface NotificationProvider {

    /**
     * Sends the notification to a provider. Implementations may block while
     * talking to an external system and must throw if the hand-off fails.
     *
     * <p><b>Contract: implementations MUST be idempotent on {@code notification.getId()}.</b>
     * Delivery is at-least-once. A consumer can die after this call has delivered but
     * before SENT is recorded, or a call can outlive its lease; either way the
     * reconciler retries, and this method is called again for an id it already
     * delivered. That repeat must be a successful no-op. For a real provider, pass the
     * notification id as the provider's idempotency key, if its API accepts one — many
     * email/SMS APIs do not, so check. Without one, exactly-once is not achievable: a
     * record kept on this side can only be written after the send returns, which is the
     * same gap SENT already leaves. The options are then to accept the rare duplicate, or
     * narrow the gap with provider features (tag the message with the id and look it up
     * before resending, or let delivery webhooks record SENT).
     *
     * <p>Calls must also be bounded by a timeout well below
     * {@code notifications.reconciler.lease-timeout}, so a slow send fails rather than
     * having its lease reclaimed underneath it.
     */
    void send(Notification notification);

    /** Whether a later provider callback must confirm delivery. */
    default boolean requiresDeliveryConfirmation() {
        return false;
    }
}
