package com.example.notification.messaging;

import com.example.notification.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class NotificationLogCorrelationTest {

    @Test
    void deliveryListenerSetsAndClearsNotificationIdEvenOnFailure() {
        UUID id = UUID.randomUUID();
        NotificationService service = mock(NotificationService.class);
        doAnswer(invocation -> {
            assertThat(MDC.get("notificationId")).isEqualTo(id.toString());
            throw new IllegalStateException("delivery failed");
        }).when(service).attemptDelivery(id);

        assertThatThrownBy(() -> new NotificationListener(service).onNotification(id.toString()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(MDC.get("notificationId")).isNull();
    }

    @Test
    void deadLetterListenerSetsAndClearsNotificationId() {
        UUID id = UUID.randomUUID();
        NotificationService service = mock(NotificationService.class);
        doAnswer(invocation -> {
            assertThat(MDC.get("notificationId")).isEqualTo(id.toString());
            return null;
        }).when(service).markDead(org.mockito.ArgumentMatchers.eq(id), anyString());

        new DeadLetterListener(service).onDeadLetter(id.toString(), null);

        assertThat(MDC.get("notificationId")).isNull();
    }
}
