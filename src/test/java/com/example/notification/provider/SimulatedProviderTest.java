package com.example.notification.provider;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimulatedProviderTest {

    private final SimulatedProvider provider = new SimulatedProvider();

    @Test
    void sendCompletesWithoutThrowing() {
        Notification notification = new Notification("alice@example.com", Channel.EMAIL, "hello");

        assertThatCode(() -> provider.send(notification)).doesNotThrowAnyException();
    }

    @Test
    void sendThrowsForTheFailingRecipient() {
        Notification notification = new Notification("fail@example.com", Channel.EMAIL, "boom");

        assertThatThrownBy(() -> provider.send(notification))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void repeatSendOfTheSameIdIsAcknowledgedWithoutDeliveringAgain() {
        Notification notification = new Notification("once@example.com", Channel.EMAIL, "hi");

        provider.send(notification);
        assertThatCode(() -> provider.send(notification)).doesNotThrowAnyException();

        assertThat(provider.invocationTimes(notification.getId())).hasSize(2);
        assertThat(provider.wasDelivered(notification.getId())).isTrue();
        assertThat(provider.duplicatesIgnored(notification.getId())).isEqualTo(1);
    }

    @Test
    void failedSendIsNotRecordedAsDelivered() {
        Notification notification = new Notification("fail@example.com", Channel.EMAIL, "boom");

        assertThatThrownBy(() -> provider.send(notification)).isInstanceOf(RuntimeException.class);

        assertThat(provider.wasDelivered(notification.getId())).isFalse();
    }

    @Test
    void sendBlocksForTheSimulatedLatency() {
        Notification notification = new Notification("+15551234567", Channel.SMS, "hi");

        long start = System.nanoTime();
        provider.send(notification);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Provider sleeps ~800ms to mimic a slow external API.
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(700);
    }
}
