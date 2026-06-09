package com.example.notification.queue;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * In-process, in-memory work queue holding the ids of notifications awaiting
 * delivery. This is deliberately a plain {@link java.util.concurrent.BlockingQueue}
 * inside the single application process — there is no external broker (no
 * RabbitMQ/Kafka/Redis). The queue does not survive a restart.
 */
@Component
public class NotificationQueue {

    private final BlockingQueue<UUID> queue = new LinkedBlockingQueue<>();

    /** Hands a notification id to the workers. Non-blocking and unbounded. */
    public void enqueue(UUID notificationId) {
        queue.add(notificationId);
    }

    /** Blocks until an id is available, then removes and returns it. */
    public UUID take() throws InterruptedException {
        return queue.take();
    }

    /** Current number of ids waiting to be processed. Useful for tests/metrics. */
    public int size() {
        return queue.size();
    }
}
