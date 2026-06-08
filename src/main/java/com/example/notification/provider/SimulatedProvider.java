package com.example.notification.provider;

import com.example.notification.domain.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stubbed provider that mimics a slow external delivery API. It does not perform
 * any real delivery (no SMTP, no SMS gateway) — it just sleeps and logs.
 */
@Component
public class SimulatedProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(SimulatedProvider.class);

    private static final long SIMULATED_LATENCY_MILLIS = 800L;

    /** Recipient that always fails delivery — handy for exercising the FAILED path. */
    private static final String FAILING_RECIPIENT = "fail@example.com";

    @Override
    public void send(Notification notification) {
        try {
            Thread.sleep(SIMULATED_LATENCY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending notification", e);
        }

        if (FAILING_RECIPIENT.equalsIgnoreCase(notification.getRecipient())) {
            throw new IllegalStateException("Simulated provider failure for " + FAILING_RECIPIENT);
        }

        log.info("sent to {} via {}", notification.getRecipient(), notification.getChannel());
    }
}
