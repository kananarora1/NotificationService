package com.example.notification.provider;

import com.example.notification.domain.Notification;

public interface NotificationProvider {

    /**
     * Delivers the notification to its recipient. Implementations may block while
     * talking to an external system and must throw if delivery fails.
     *
     * <p><b>Contract: implementations MUST be idempotent on {@code notification.getId()}.</b>
     * Delivery is at-least-once. A consumer can die after this call has delivered but
     * before SENT is recorded, or a call can outlive its lease; either way the
     * reconciler retries, and this method is called again for an id it already
     * delivered. That repeat must be a successful no-op. For a real provider, pass the
     * notification id as the provider's idempotency key (most email/SMS APIs accept
     * one); for a provider without one, record delivered ids on your side before
     * acknowledging.
     *
     * <p>Calls must also be bounded by a timeout well below
     * {@code notifications.reconciler.lease-timeout}, so a slow send fails rather than
     * having its lease reclaimed underneath it.
     */
    void send(Notification notification);
}
