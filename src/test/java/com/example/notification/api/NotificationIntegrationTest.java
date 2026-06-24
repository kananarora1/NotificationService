package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.provider.SimulatedProvider;
import com.example.notification.repository.NotificationRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class NotificationIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"));

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitmq =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private SimulatedProvider provider;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void normalRecipientSucceedsOnFirstAttempt() throws Exception {
        UUID id = postNotify("alice@example.com");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getAttempts()).isEqualTo(1);
            assertThat(stored.getSentAt()).isNotNull();
        });
    }

    @Test
    void flakyRecipientRecoversToSentViaRetry() throws Exception {
        // Fails the first two attempts, then succeeds — proves retry recovers a transient failure.
        UUID id = postNotify("flaky@example.com");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getAttempts()).isGreaterThanOrEqualTo(3);
            assertThat(stored.getFailureReason()).isNull();
        });
    }

    @Test
    void permanentFailureEndsDeadAndIsDeadLettered() throws Exception {
        // Always fails -> retries are exhausted -> message is dead-lettered to notifications.dlq,
        // where the DLQ consumer marks the row DEAD with a failureReason. Reaching DEAD (set
        // *only* by the DLQ consumer) proves the message landed in the DLQ instead of vanishing.
        UUID id = postNotify("fail@example.com");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.DEAD);
            assertThat(stored.getAttempts()).isEqualTo(4); // retry.max-attempts
            assertThat(stored.getFailureReason()).isNotNull();
            assertThat(stored.getSentAt()).isNull();
        });
    }

    @Test
    void backoffDelayIncreasesBetweenAttempts() throws Exception {
        UUID id = postNotify("fail@example.com");

        // Wait for all four attempts to have been recorded by the provider.
        await().atMost(Duration.ofSeconds(30))
                .until(() -> provider.invocationTimes(id).size() >= 4);

        List<Long> times = provider.invocationTimes(id);
        long gap1 = times.get(1) - times.get(0);
        long gap2 = times.get(2) - times.get(1);
        long gap3 = times.get(3) - times.get(2);

        // initial-interval 1s, multiplier 2.0 => gaps grow (~1s, ~2s, ~4s on top of the send time).
        // Assert loosely: each successive gap is larger than the previous one.
        assertThat(gap2).as("2nd gap > 1st gap").isGreaterThan(gap1);
        assertThat(gap3).as("3rd gap > 2nd gap").isGreaterThan(gap2);
    }

    @Test
    void getReflectsAttemptsAndStatus() throws Exception {
        UUID id = postNotify("bob@example.com");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mockMvc.perform(get("/notify/{id}", id.toString()))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SENT"))
                        .andExpect(jsonPath("$.attempts").value(1))
                        .andExpect(jsonPath("$.sentAt").exists()));
    }

    @Test
    void getUnknownIdReturns404() throws Exception {
        mockMvc.perform(get("/notify/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    void blankFieldReturns400() throws Exception {
        String body = """
                {"recipient":"","channel":"EMAIL","message":"hello"}
                """;

        mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void invalidChannelReturns400() throws Exception {
        String body = """
                {"recipient":"alice@example.com","channel":"CARRIER_PIGEON","message":"hello"}
                """;

        mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    private UUID postNotify(String recipient) throws Exception {
        String body = String.format(
                "{\"recipient\":\"%s\",\"channel\":\"EMAIL\",\"message\":\"hello\"}", recipient);

        MvcResult result = mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();

        JsonNode json = objectMapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(json.get("id").asText());
    }
}
