package com.example.notification.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Fails startup if RabbitMQ already has one of this app's queues with different
 * arguments — a queue left by an earlier version.
 *
 * <p>RabbitMQ refuses to redeclare such a queue, and {@code RabbitAdmin} only logs that
 * and carries on, so the app would report healthy while running on the old queue. For
 * {@code notifications.dlq} that means no route to the parking lot: a dead-letter whose
 * DEAD write fails would be dropped again, with nothing but a log line to show for it.
 * The fix is operational — delete the old queue once it is empty — so the app refuses to
 * start until it is done.
 *
 * <p>A broker that cannot be reached is not an error here: the app must start while
 * RabbitMQ is down (accepted notifications are republished once it is back), and
 * {@code RabbitAdmin} declares the queues when it connects.
 */
@Component
public class QueueArgumentsCheck implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(QueueArgumentsCheck.class);

    private final AmqpAdmin amqpAdmin;
    private final List<Queue> queues;

    public QueueArgumentsCheck(AmqpAdmin amqpAdmin, List<Queue> queues) {
        this.amqpAdmin = amqpAdmin;
        this.queues = queues;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (Queue queue : queues) {
            try {
                amqpAdmin.declareQueue(queue);
            } catch (AmqpException e) {
                if (RabbitUtils.isMismatchedQueueArgs(e)) {
                    throw new IllegalStateException(("RabbitMQ already has queue '%s' with different arguments, "
                            + "left by an earlier version. Stop every instance, check the queue is empty "
                            + "(rabbitmqctl list_queues name messages), delete it (rabbitmqctl delete_queue %s) "
                            + "and start again. See 'Upgrading an existing broker' in the README.")
                            .formatted(queue.getName(), queue.getName()), e);
                }
                log.warn("Could not verify RabbitMQ queue '{}' at startup ({}); it is declared on connect",
                        queue.getName(), e.getMessage());
                return;
            }
        }
    }
}
