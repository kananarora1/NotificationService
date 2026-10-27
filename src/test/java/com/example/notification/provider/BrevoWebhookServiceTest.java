package com.example.notification.provider;

import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrevoWebhookServiceTest {

    @Mock
    private NotificationRepository notifications;

    @Mock
    private NotificationMetrics metrics;

    @Test
    void recordsRecognizedDeliveryEventOnceAndUpdatesLatestOutcome() {
        UUID notificationId = UUID.randomUUID();
        BrevoWebhookPayload payload = payload(notificationId, 701L, "delivered", 1_767_225_600L);
        when(notifications.existsById(notificationId)).thenReturn(true);
        when(notifications.recordBrevoEvent(701L, notificationId, "delivered",
                Instant.parse("2026-01-01T00:00:00Z"), null)).thenReturn(1);
        when(notifications.updateDeliveryStatus(notificationId, "DELIVERED",
                Instant.parse("2026-01-01T00:00:00Z"))).thenReturn(1);

        new BrevoWebhookService(notifications, metrics).record(payload);

        verify(notifications).markProviderDelivered(notificationId,
                Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void ignoresCallbackRetryAfterEventHasAlreadyBeenRecorded() {
        UUID notificationId = UUID.randomUUID();
        BrevoWebhookPayload payload = payload(notificationId, 702L, "hard_bounce", 1_767_225_601L);
        when(notifications.existsById(notificationId)).thenReturn(true);
        when(notifications.recordBrevoEvent(702L, notificationId, "hard_bounce",
                Instant.parse("2026-01-01T00:00:01Z"), null)).thenReturn(0);

        new BrevoWebhookService(notifications, metrics).record(payload);

        verify(notifications, never()).updateDeliveryStatus(notificationId, "HARD_BOUNCED",
                Instant.parse("2026-01-01T00:00:01Z"));
    }

    @Test
    void permanentFailureMarksNotificationDead() {
        UUID notificationId = UUID.randomUUID();
        Instant occurredAt = Instant.parse("2026-01-01T00:00:02Z");
        when(notifications.existsById(notificationId)).thenReturn(true);
        when(notifications.recordBrevoEvent(705L, notificationId, "hard_bounce",
                occurredAt, "mailbox unavailable")).thenReturn(1);
        when(notifications.updateDeliveryStatus(notificationId, "HARD_BOUNCED", occurredAt)).thenReturn(1);

        new BrevoWebhookService(notifications, metrics).record(new BrevoWebhookPayload(
                "hard_bounce", 705L, "notification-id:" + notificationId,
                1_767_225_602L, "mailbox unavailable"));

        verify(notifications).markProviderDead(notificationId, "HARD_BOUNCED", occurredAt,
                "mailbox unavailable");
    }

    @Test
    void acknowledgesCallbacksForMissingNotificationsWithoutPersistingThem() {
        UUID notificationId = UUID.randomUUID();
        when(notifications.existsById(notificationId)).thenReturn(false);

        new BrevoWebhookService(notifications, metrics).record(payload(notificationId, 703L, "delivered", 1_767_225_600L));

        verify(notifications, never()).recordBrevoEvent(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any());
    }

    @Test
    void ignoresEventsWithoutTheExactRelayNestCorrelationHeader() {
        new BrevoWebhookService(notifications, metrics).record(new BrevoWebhookPayload(
                "delivered", 704L, "different-header:abc", 1_767_225_600L, null));

        verify(notifications, never()).existsById(org.mockito.ArgumentMatchers.any());
    }

    private static BrevoWebhookPayload payload(UUID notificationId, long eventId, String event, long timestamp) {
        return new BrevoWebhookPayload(event, eventId, "notification-id:" + notificationId, timestamp, null);
    }
}
