package com.example.notification.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Settings for the {@code notifications.dlq} listener, bound from
 * {@code notifications.dead-letter.*}. Deliberately separate from the work queue's
 * {@code spring.rabbitmq.listener.simple.*}: see {@link RabbitConfig#deadLetterContainerFactory}.
 *
 * @param retry how often, and how patiently, the handler retries a failed DEAD write
 *              before the message is parked in {@code notifications.parking-lot}
 */
@ConfigurationProperties(prefix = "notifications.dead-letter")
public record DeadLetterProperties(@DefaultValue Retry retry) {

    /**
     * @param maxAttempts     attempts in total, the first included
     * @param initialInterval wait before the first retry
     * @param multiplier      growth of the wait between retries
     * @param maxInterval     cap on the wait between retries
     */
    public record Retry(
            @DefaultValue("3") int maxAttempts,
            @DefaultValue("500ms") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("2s") Duration maxInterval) {
    }
}
