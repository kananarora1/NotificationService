package com.example.notification.reconciler;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Settings for {@link LeaseReconciler}, bound from {@code notifications.reconciler.*}.
 * (The sweep interval is read directly by the {@code @Scheduled} annotation.)
 *
 * @param leaseTimeout how long a SENDING claim may stand before it is presumed dead and
 *                     reclaimed. A lease covers ONE attempt — the claim is released
 *                     between retries — so this must comfortably exceed the slowest
 *                     single provider call, not the whole retry cycle. Too short and a
 *                     slow-but-alive send is reclaimed and sent twice; too long and a
 *                     crashed send waits that much longer to be retried.
 * @param batchSize    the most rows one sweep reclaims, so a large backlog is worked off
 *                     over several short transactions rather than one long one
 */
@ConfigurationProperties(prefix = "notifications.reconciler")
public record ReconcilerProperties(
        @DefaultValue("60s") Duration leaseTimeout,
        @DefaultValue("100") int batchSize) {
}
