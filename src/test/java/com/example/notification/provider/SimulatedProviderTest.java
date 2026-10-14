package com.example.notification.provider;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimulatedProviderTest {

    private final SimulatedProvider provider = new SimulatedProvider();

    /** The simulator's only output: each "sent to …" line is one delivery. */
    private final ListAppender<ILoggingEvent> log = new ListAppender<>();

    @BeforeEach
    void captureLog() {
        log.start();
        ((Logger) LoggerFactory.getLogger(SimulatedProvider.class)).addAppender(log);
    }

    @AfterEach
    void releaseLog() {
        ((Logger) LoggerFactory.getLogger(SimulatedProvider.class)).detachAppender(log);
    }

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

        assertThat(messages()).filteredOn(m -> m.startsWith("sent to once@example.com")).hasSize(1);
        assertThat(messages()).filteredOn(m -> m.startsWith("already delivered " + notification.getId())).hasSize(1);
    }

    @Test
    void flakyRecipientFailsUntilItsThirdAttemptAndAFailureIsNotRecordedAsDelivered() {
        Notification notification = new Notification("flaky@example.com", Channel.EMAIL, "hi");

        // attempts is set by the delivery claim in production; 1 = first attempt.
        ReflectionTestUtils.setField(notification, "attempts", 2);
        assertThatThrownBy(() -> provider.send(notification)).isInstanceOf(RuntimeException.class);

        // Had the failed attempt been recorded as delivered, this would be a silent no-op.
        ReflectionTestUtils.setField(notification, "attempts", 3);
        provider.send(notification);

        assertThat(messages()).filteredOn(m -> m.startsWith("sent to flaky@example.com")).hasSize(1);
        assertThat(messages()).noneMatch(m -> m.startsWith("already delivered"));
    }

    @Test
    void isNotRegisteredUnderTheProdProfile() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("prod");
            context.register(SimulatedProvider.class);
            context.refresh();

            assertThat(context.getBeansOfType(NotificationProvider.class)).isEmpty();
        }
        try (var context = new AnnotationConfigApplicationContext(SimulatedProvider.class)) {
            assertThat(context.getBeansOfType(NotificationProvider.class)).hasSize(1);
        }
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

    private List<String> messages() {
        synchronized (log) {
            return log.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }
    }
}
