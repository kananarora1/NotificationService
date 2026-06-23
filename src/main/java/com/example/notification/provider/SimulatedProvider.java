package com.example.notification.provider;

import com.example.notification.domain.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stubbed provider that mimics a slow external delivery API. It does not perform
 * any real delivery (no SMTP, no SMS gateway) — it just sleeps and logs.
 *
 * <p>Two recipients drive the failure paths so they are testable on demand:
 * <ul>
 *   <li>{@code fail@example.com} — always throws (permanent failure → ends DEAD after
 *       retries are exhausted).</li>
 *   <li>{@code flaky@example.com} — throws on the first two attempts for a given
 *       notification, then succeeds (transient failure → recovers to SENT via retry).</li>
 * </ul>
 */
@Component
public class SimulatedProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(SimulatedProvider.class);

    private static final long SIMULATED_LATENCY_MILLIS = 800L;

    private static final String FAILING_RECIPIENT = "fail@example.com";
    private static final String FLAKY_RECIPIENT = "flaky@example.com";
    private static final int FLAKY_FAILURES = 2;

    /** Per-notification attempt counter used to make {@code flaky@example.com} deterministic. */
    private final ConcurrentHashMap<UUID, AtomicInteger> flakyAttempts = new ConcurrentHashMap<>();

    /** Wall-clock timestamps of each send() invocation per notification (for backoff assertions). */
    private final ConcurrentHashMap<UUID, List<Long>> invocationTimes = new ConcurrentHashMap<>();

    @Override
    public void send(Notification notification) {
        recordInvocation(notification.getId());

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

        if (FLAKY_RECIPIENT.equalsIgnoreCase(recipient)) {
            int attempt = flakyAttempts
                    .computeIfAbsent(notification.getId(), k -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt <= FLAKY_FAILURES) {
                throw new IllegalStateException(
                        "Simulated transient failure for " + FLAKY_RECIPIENT + " (attempt " + attempt + ")");
            }
        }

        log.info("sent to {} via {}", recipient, notification.getChannel());
    }

    private void recordInvocation(UUID id) {
        invocationTimes
                .computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(System.currentTimeMillis());
    }

    /** Returns a snapshot of the send() invocation timestamps for a notification. */
    public List<Long> invocationTimes(UUID id) {
        List<Long> times = invocationTimes.get(id);
        if (times == null) {
            return List.of();
        }
        synchronized (times) {
            return List.copyOf(times);
        }
    }
}
