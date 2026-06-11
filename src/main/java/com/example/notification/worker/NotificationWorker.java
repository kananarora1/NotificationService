package com.example.notification.worker;

import com.example.notification.queue.NotificationQueue;
import com.example.notification.service.NotificationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background delivery engine. On startup it launches a fixed pool of worker
 * threads, each of which loops forever taking notification ids off the in-memory
 * {@link NotificationQueue} and delivering them via {@link NotificationService}.
 *
 * <p>The loop is resilient: a delivery that throws is caught and logged, and the
 * worker keeps going — one poison notification can never take a thread down. The
 * pool is shut down gracefully when the application context closes.
 */
@Component
public class NotificationWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    private static final int WORKER_COUNT = 4;
    private static final long SHUTDOWN_GRACE_SECONDS = 10L;

    private final NotificationQueue queue;
    private final NotificationService service;

    private ExecutorService executor;
    private volatile boolean running;

    public NotificationWorker(NotificationQueue queue, NotificationService service) {
        this.queue = queue;
        this.service = service;
    }

    @PostConstruct
    public void start() {
        running = true;
        executor = Executors.newFixedThreadPool(WORKER_COUNT, namedThreadFactory());
        for (int i = 0; i < WORKER_COUNT; i++) {
            executor.submit(this::workLoop);
        }
        log.info("Started {} notification worker threads", WORKER_COUNT);
    }

    private void workLoop() {
        while (running) {
            UUID id;
            try {
                id = queue.take();
            } catch (InterruptedException e) {
                // Interrupted while blocked on the queue — this is our shutdown signal.
                Thread.currentThread().interrupt();
                break;
            }

            try {
                service.deliver(id);
            } catch (Throwable t) {
                // Defensive net: deliver() already handles provider failures, but we
                // must never let anything escape and kill the worker thread.
                log.error("Unexpected error delivering notification {}", id, t);
            }
        }
    }

    @PreDestroy
    public void stop() {
        log.info("Shutting down notification workers");
        running = false;
        if (executor == null) {
            return;
        }
        executor.shutdownNow(); // interrupt threads blocked on queue.take()
        try {
            if (!executor.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Workers did not terminate within {}s", SHUTDOWN_GRACE_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory namedThreadFactory() {
        AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            Thread t = new Thread(runnable, "notification-worker-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
