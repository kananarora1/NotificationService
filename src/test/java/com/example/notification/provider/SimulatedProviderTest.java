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
    void sendBlocksForTheSimulatedLatency() {
        Notification notification = new Notification("+15551234567", Channel.SMS, "hi");

        long start = System.nanoTime();
        provider.send(notification);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Provider sleeps ~800ms to mimic a slow external API.
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(700);
    }
}
