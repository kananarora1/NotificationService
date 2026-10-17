package com.example.notification.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
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
 *
 * <p>If recording it fails too (Postgres down), {@code notifications.dlq} dead-letters
 * the message once more, to the durable {@code notifications.parking-lot}. Nothing
 * consumes the parking lot: a message there reached the end of the line and waits for a
 * person to inspect and replay it. So no message is ever discarded.
 */
@Configuration
public class RabbitConfig {

    /** Name of the durable work queue. */
    public static final String SEND_QUEUE = "notifications.send";

    /** Dead-letter exchange and queue for messages that exhaust their retries. */
    public static final String DEAD_LETTER_EXCHANGE = "notifications.dlx";
    public static final String DEAD_LETTER_QUEUE = "notifications.dlq";
    public static final String DEAD_LETTER_ROUTING_KEY = "notifications.dlq";

    /** Holding queue, on the same exchange, for dead-letters the DLQ handler could not record. */
    public static final String PARKING_LOT_QUEUE = "notifications.parking-lot";
    public static final String PARKING_LOT_ROUTING_KEY = "notifications.parking-lot";

    /** Bean name of the DLQ listener's own container factory. */
    public static final String DEAD_LETTER_CONTAINER_FACTORY = "deadLetterContainerFactory";

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

    /**
     * The DLQ, itself dead-lettering to the parking lot: a message its handler rejects is
     * moved there by the broker instead of being dropped. Same redeclare caveat as the
     * work queue — a broker with a pre-v13 {@code notifications.dlq} must delete it first.
     */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE)
                .withArgument("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", PARKING_LOT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DEAD_LETTER_ROUTING_KEY);
    }

    /** No dead-letter exchange of its own and no consumer: it is the end of the line. */
    @Bean
    public Queue parkingLotQueue() {
        return QueueBuilder.durable(PARKING_LOT_QUEUE).build();
    }

    @Bean
    public Binding parkingLotBinding() {
        return BindingBuilder.bind(parkingLotQueue())
                .to(deadLetterExchange())
                .with(PARKING_LOT_ROUTING_KEY);
    }

    /**
     * The DLQ listener's own container factory, rather than the work queue's 16-32
     * consumers and 4 attempts. The DLQ is empty in normal operation, and it fills fastest
     * in exactly the case where its handler cannot succeed: a Postgres outage fails every
     * send, so every in-flight notification is dead-lettered at once and every DEAD write
     * fails. One consumer with a short retry then writes to the dead database about once
     * a second, instead of 4 times per message across 16 consumers, and parks each message
     * within seconds. Whatever is still in the DLQ when Postgres returns is recorded
     * normally.
     *
     * <p>Built from Boot's configurer, so it shares the connection, acknowledge mode and
     * {@code default-requeue-rejected: false} — a rejection must dead-letter, which is what
     * moves the message to the parking lot.
     */
    @Bean(DEAD_LETTER_CONTAINER_FACTORY)
    public SimpleRabbitListenerContainerFactory deadLetterContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory,
            ContainerCustomizer<SimpleMessageListenerContainer> gracefulListenerShutdown,
            DeadLetterProperties properties) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setPrefetchCount(1);
        factory.setDefaultRequeueRejected(false);
        DeadLetterProperties.Retry retry = properties.retry();
        factory.setAdviceChain(RetryInterceptorBuilder.stateless()
                .maxAttempts(retry.maxAttempts())
                .backOffOptions(retry.initialInterval().toMillis(), retry.multiplier(),
                        retry.maxInterval().toMillis())
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build());
        // Boot applies the customizer only to the factory it builds itself.
        factory.setContainerCustomizer(gracefulListenerShutdown);
        return factory;
    }
}
