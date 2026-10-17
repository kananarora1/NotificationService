package com.example.notification.provider;

import com.example.notification.domain.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stubbed provider that mimics a slow external delivery API. It does not perform
 * any real delivery (no SMTP, no SMS gateway) — it just sleeps and logs.
 *
 * <p>Development and test only: inactive under the {@code prod} profile, so a production
 * deployment fails at startup until a real {@link NotificationProvider} is configured,
 * rather than silently "delivering" into a log.
 *
 * <p>Two recipients drive the failure paths so they are testable on demand:
 * <ul>
 *   <li>{@code fail@example.com} — always throws (permanent failure → ends DEAD after
 *       retries are exhausted).</li>
 *   <li>{@code flaky@example.com} — throws until the notification's third attempt
 *       (transient failure → recovers to SENT via retry). Driven by the attempt count the
 *       delivery claim records, so the simulator keeps no per-notification state for it.</li>
 * </ul>
 *
 * <p>Idempotent on the notification id, as {@link NotificationProvider} requires: once an
 * id has been delivered, a repeat send of it is acknowledged without delivering again —
 * how a real provider treats a repeated idempotency key. Unlike a real provider's
 * server-side record, though, this one lives in this JVM: a restart forgets it, and each
 * instance has its own. So it shows the contract within one process; it does not prove it
 * across a crash.
 */
@Component
@Profile("!prod & !brevo")
public class SimulatedProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(SimulatedProvider.class);

    private static final long SIMULATED_LATENCY_MILLIS = 800L;

    private static final String FAILING_RECIPIENT = "fail@example.com";
    private static final String FLAKY_RECIPIENT = "flaky@example.com";
    private static final int FLAKY_FAILURES = 2;

    /** Ids delivered by this JVM: the stand-in for a provider's idempotency-key store. */
    private final Set<UUID> delivered = ConcurrentHashMap.newKeySet();

    @Override
    public void send(Notification notification) {
        try {
            Thread.sleep(SIMULATED_LATENCY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending notification", e);
        }

        String recipient = notification.getRecipient();

        if (FAILING_RECIPIENT.equalsIgnoreCase(recipient)) {
            throw new IllegalStateException("Simulated permanent failure for " + FAILING_RECIPIENT);
        }

        // getAttempts() is 1 on the first attempt: the claim increments it before the send.
        if (FLAKY_RECIPIENT.equalsIgnoreCase(recipient) && notification.getAttempts() <= FLAKY_FAILURES) {
            throw new IllegalStateException(
                    "Simulated transient failure for " + FLAKY_RECIPIENT + " (attempt " + notification.getAttempts() + ")");
        }

        // Recorded only once delivery succeeds, so a failed attempt is never mistaken for a
        // delivered one. add() is atomic: of two concurrent sends of one id, one delivers.
        if (!delivered.add(notification.getId())) {
            log.info("already delivered {}; acknowledging the repeat send without delivering", notification.getId());
            return;
        }
        log.info("sent to {} via {}", recipient, notification.getChannel());
    }
}
