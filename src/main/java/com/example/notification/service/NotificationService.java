package com.example.notification.service;

import com.example.notification.domain.Notification;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.queue.NotificationQueue;
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
    private final NotificationQueue queue;

    public NotificationService(NotificationRepository repository,
                               NotificationProvider provider,
                               NotificationQueue queue) {
        this.repository = repository;
        this.provider = provider;
        this.queue = queue;
    }

    /**
     * Persists the notification as PENDING and enqueues its id for asynchronous
     * delivery. Returns immediately — the (slow) provider call happens later on a
     * worker thread, not in the request thread.
     */
    public Notification create(Notification notification) {
        repository.save(notification);
        queue.enqueue(notification.getId());
        return notification;
    }

    /**
     * Delivers a single notification. Invoked by the worker threads, never by the
     * request thread. Loads the row, calls the provider, and records the terminal
     * state: SENT (+ sentAt) on success, FAILED on provider error. A provider
     * failure is swallowed here (after marking FAILED) so a single bad send never
     * propagates out to kill a worker.
     */
    public void deliver(UUID id) {
        Notification notification = repository.findById(id).orElse(null);
        if (notification == null) {
            // The id was enqueued but the row is gone (e.g. wiped between tests).
            log.warn("No notification found for queued id {}; skipping", id);
            return;
        }

        try {
            provider.send(notification);
            notification.markSent(Instant.now());
            repository.save(notification);
        } catch (RuntimeException e) {
            notification.markFailed();
            repository.save(notification);
            log.warn("Delivery failed for notification {}", id, e);
        }
    }

    public Notification get(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotificationNotFoundException(id));
    }
}
