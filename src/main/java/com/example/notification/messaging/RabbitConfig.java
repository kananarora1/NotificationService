package com.example.notification.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ topology.
 *
 * <p>The durable work queue {@code notifications.send} carries notification ids from
 * the producer (POST /notify) to the consumer ({@link NotificationListener}). The
 * listener retries transient failures (see {@code spring.rabbitmq.listener.simple.retry.*});
 * once retries are exhausted the message is rejected without requeue and RabbitMQ
 * routes it — via the queue's dead-letter exchange — to the durable
 * {@code notifications.dlq}, where {@link DeadLetterListener} records it.
 */
@Configuration
public class RabbitConfig {

    /** Name of the durable work queue. */
    public static final String SEND_QUEUE = "notifications.send";

    /** Dead-letter exchange and queue for messages that exhaust their retries. */
    public static final String DEAD_LETTER_EXCHANGE = "notifications.dlx";
    public static final String DEAD_LETTER_QUEUE = "notifications.dlq";
    public static final String DEAD_LETTER_ROUTING_KEY = "notifications.dlq";

    /**
     * Durable work queue, configured to dead-letter rejected messages to the DLX.
     *
     * <p>Note: changing these arguments on a broker that already has the queue from
     * an earlier version requires deleting the old queue first — RabbitMQ refuses to
     * redeclare an existing queue with different arguments.
     */
    @Bean
    public Queue notificationsSendQueue() {
        return QueueBuilder.durable(SEND_QUEUE)
                .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DEAD_LETTER_ROUTING_KEY)
                .build();
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DEAD_LETTER_EXCHANGE, true, false);
    }

    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DEAD_LETTER_ROUTING_KEY);
    }
}
