package com.example.notification.service;

import com.example.notification.domain.Notification;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final NotificationProvider provider;
    private final NotificationPublisher publisher;

    public NotificationService(NotificationRepository repository,
                               NotificationProvider provider,
                               NotificationPublisher publisher) {
        this.repository = repository;
        this.provider = provider;
        this.publisher = publisher;
    }

    /**
     * Persists the notification as PENDING and publishes its id to RabbitMQ for
     * asynchronous delivery. Returns immediately — the (slow) provider call happens
     * later on a listener thread, not in the request thread.
     */
    public Notification create(Notification notification) {
        repository.save(notification);
        publisher.publish(notification.getId());
        return notification;
    }

    /**
     * Attempts to deliver a single notification. Invoked by the RabbitMQ listener,
     * once per retry attempt. Increments the attempt counter, calls the provider,
     * and marks SENT on success.
     *
     * <p>On a provider failure the exception is <b>rethrown</b> so the listener's
     * retry interceptor can back off and retry — the status is deliberately left
     * PENDING (not FAILED). When retries are finally exhausted the message is
     * dead-lettered and {@link #markDead(UUID, String)} flips it to DEAD.
     */
    public void attemptDelivery(UUID id) {
        Notification notification = repository.findById(id).orElse(null);
        if (notification == null) {
            // The id was published but the row is gone (e.g. wiped between tests).
            log.warn("No notification found for published id {}; skipping", id);
            return;
        }

        int attempt = notification.incrementAttempts();
        repository.save(notification);
        log.info("Delivery attempt {} for notification {}", attempt, id);

        provider.send(notification); // throws on failure -> propagates for retry/dead-lettering
        notification.markSent(Instant.now());
        repository.save(notification);
    }

    /**
     * Marks a notification permanently failed after it has been dead-lettered.
     * Returns the recorded attempt count for logging.
     */
    public int markDead(UUID id, String reason) {
        Notification notification = repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
        notification.markDead(reason);
        repository.save(notification);
        return notification.getAttempts();
    }

    public Notification get(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
    }
}
