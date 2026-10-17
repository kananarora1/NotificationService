package com.example.notification.messaging;

import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Makes the RabbitMQ listener containers drain rather than die on shutdown.
 *
 * <p>Spring Boot's {@code server.shutdown: graceful} covers the HTTP connector only —
 * it knows nothing about message listeners. Without the settings below, a SIGTERM
 * cancels the consumers and gives in-flight {@code @RabbitListener} invocations just
 * 5 seconds (Spring AMQP's default {@code shutdownTimeout}) to finish. With an
 * 800ms-per-send provider and a prefetch of 10, that is not enough: the container
 * would abandon messages mid-delivery, leaving them unacked for the broker to
 * redeliver — a duplicate send for anything the provider had already accepted.
 *
 * <p>What actually happens on SIGTERM (measured, see the README's Operations section):
 * <ul>
 *   <li>Consumers are cancelled, so the broker sends no <em>new</em> messages.</li>
 *   <li>Every {@code @RabbitListener} invocation already <em>executing</em> runs to
 *       completion and acks. That is the guarantee that matters: a notification the
 *       provider has already accepted is never redelivered and sent twice.</li>
 *   <li>Messages that were prefetched but not yet started are <em>requeued</em> — the
 *       container logs "Rejecting received message(s) because the listener container
 *       has been stopped". They return to the queue as ready, untouched
 *       ({@code attempts} still 0), and the next instance to start picks them up.
 *       Note this rejection requeues; it does <em>not</em> route to the DLQ, despite
 *       {@code default-requeue-rejected: false}, which governs listener failures
 *       rather than container shutdown.</li>
 *   <li>{@code shutdownTimeout} bounds the wait for the executing invocations. It is
 *       deliberately set below {@code spring.lifecycle.timeout-per-shutdown-phase} so
 *       the container reports its own timeout rather than being cut off mid-phase by
 *       the context close.</li>
 * </ul>
 *
 * <p>So the drain is not "finish everything prefetched" — it is "finish what is in
 * flight, hand the rest back". No message is lost, none is delivered twice.
 */
@Configuration
public class ListenerShutdownConfig {

    private final Duration shutdownTimeout;

    public ListenerShutdownConfig(
            @Value("${notifications.listener.shutdown-timeout:25s}")
            @DurationUnit(ChronoUnit.SECONDS) Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    /**
     * Applied by Spring Boot to the auto-configured
     * {@code SimpleRabbitListenerContainerFactory} (the work queue), and by
     * {@link RabbitConfig#deadLetterContainerFactory} to the DLQ's own factory, so every
     * {@code @RabbitListener} drains the same way.
     */
    @Bean
    public ContainerCustomizer<SimpleMessageListenerContainer> gracefulListenerShutdown() {
        return container -> {
            container.setShutdownTimeout(shutdownTimeout.toMillis());
            // Explicit, though it is the default. forceStop=true would abandon the
            // in-flight invocation too, which is the case that causes a duplicate send:
            // the provider has already accepted the notification but the message is
            // never acked, so it comes back on the next boot.
            container.setForceStop(false);
        };
    }
}
