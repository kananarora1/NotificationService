package com.example.notification.service;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.repository.NotificationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceConfirmationTest {

    @Mock NotificationRepository repository;
    @Mock NotificationProvider provider;
    @Mock NotificationPublisher publisher;
    @Mock NotificationMetrics metrics;

    @Test
    void brevoAcceptanceWaitsForWebhookInsteadOfMarkingSent() {
        Notification notification = new Notification("recipient@example.com", Channel.EMAIL, "Hello");
        UUID id = notification.getId();
        when(repository.claimForSending(eq(id), any(UUID.class))).thenReturn(1);
        when(repository.findById(id)).thenReturn(Optional.of(notification));
        when(provider.requiresDeliveryConfirmation()).thenReturn(true);
        when(repository.markProviderAccepted(eq(id), any(UUID.class))).thenReturn(1);

        new NotificationService(repository, provider, publisher, metrics).attemptDelivery(id);

        verify(provider).send(notification);
        verify(repository).markProviderAccepted(eq(id), any(UUID.class));
        verify(repository, never()).markSent(eq(id), any(UUID.class), any(Instant.class));
        verify(metrics, never()).sent(any(), any());
    }
}
