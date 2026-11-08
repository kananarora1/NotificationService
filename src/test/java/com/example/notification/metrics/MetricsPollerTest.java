package com.example.notification.metrics;

import com.example.notification.domain.NotificationStatus;
import com.example.notification.messaging.RabbitConfig;
import com.example.notification.reconciler.ReconcilerProperties;
import com.example.notification.repository.NotificationRepository;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetricsPollerTest {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final NotificationRepository repository = mock(NotificationRepository.class);
    private final AmqpAdmin amqpAdmin = mock(AmqpAdmin.class);
    private final ReconcilerProperties reconcilerProperties = new ReconcilerProperties(
            Duration.ofSeconds(60), 100, Duration.ofSeconds(15), Duration.ofSeconds(60));
    private final MetricsPoller poller = new MetricsPoller(registry, repository, amqpAdmin, reconcilerProperties);

    @Test
    void databaseFailureKeepsThePreviousCompleteSnapshotAndTimestamp() {
        when(repository.countByStatus(NotificationStatus.PENDING)).thenReturn(2L, 9L);
        when(repository.countAwaitingConfirmation()).thenReturn(0L)
                .thenThrow(new IllegalStateException("database unavailable"));

        assertThat(lastSuccess("database")).isZero();
        assertThat(registry.scrape()).contains("metrics_poll_last_success_timestamp_seconds{source=\"database\"} 0.0");
        poller.pollDatabase();
        double lastSuccess = lastSuccess("database");
        assertThat(lastSuccess).isPositive();
        assertThat(registry.scrape()).contains("metrics_poll_last_success_timestamp_seconds{source=\"database\"}");
        assertThat(registry.find("notifications.pending").gauge().value()).isEqualTo(2);

        poller.pollDatabase();
        assertThat(lastSuccess("database")).isEqualTo(lastSuccess);
        assertThat(registry.find("notifications.pending").gauge().value()).isEqualTo(2);
    }

    @Test
    void missingQueueKeepsThePreviousCompleteSnapshotAndTimestamp() {
        when(amqpAdmin.getQueueProperties(RabbitConfig.SEND_QUEUE))
                .thenReturn(queueProperties(5), queueProperties(20));
        when(amqpAdmin.getQueueProperties(RabbitConfig.DEAD_LETTER_QUEUE))
                .thenReturn(queueProperties(0));
        when(amqpAdmin.getQueueProperties(RabbitConfig.PARKING_LOT_QUEUE))
                .thenReturn(queueProperties(1), (Properties) null);

        assertThat(lastSuccess("rabbitmq")).isZero();
        poller.pollQueues();
        double lastSuccess = lastSuccess("rabbitmq");
        assertThat(lastSuccess).isPositive();
        assertThat(queueDepth(RabbitConfig.SEND_QUEUE)).isEqualTo(5);

        poller.pollQueues();
        assertThat(lastSuccess("rabbitmq")).isEqualTo(lastSuccess);
        assertThat(queueDepth(RabbitConfig.SEND_QUEUE)).isEqualTo(5);
    }

    private double lastSuccess(String source) {
        return registry.find("metrics.poll.last_success_timestamp_seconds")
                .tag("source", source).gauge().value();
    }

    private double queueDepth(String queue) {
        return registry.find("queue.depth").tag("queue", queue).gauge().value();
    }

    private Properties queueProperties(int count) {
        Properties properties = new Properties();
        properties.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, count);
        return properties;
    }
}
