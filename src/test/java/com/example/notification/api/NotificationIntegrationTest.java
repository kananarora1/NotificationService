package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.repository.NotificationRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class NotificationIntegrationTest {

    private static final Set<NotificationStatus> TERMINAL =
            EnumSet.of(NotificationStatus.SENT, NotificationStatus.FAILED);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void postReturnsAcceptedAndPendingWithoutBlocking() throws Exception {
        String body = """
                {"recipient":"alice@example.com","channel":"EMAIL","message":"hello"}
                """;

        long start = System.nanoTime();
        MvcResult result = mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // The provider sleeps ~800ms; accepting the request must not wait for it.
        assertThat(elapsedMillis)
                .as("POST must return well under the 800ms provider latency")
                .isLessThan(500);

        UUID id = idOf(result);

        // The worker eventually delivers it: PENDING -> SENT with sentAt populated.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getSentAt()).isNotNull();
            assertThat(stored.getSentAt()).isAfterOrEqualTo(stored.getCreatedAt());
        });
    }

    @Test
    void getReflectsAsyncTransitionToSent() throws Exception {
        String body = """
                {"recipient":"+15551234567","channel":"SMS","message":"hi there"}
                """;

        MvcResult result = mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andReturn();
        String id = idOf(result).toString();

        // Immediately after POST the read model is PENDING...
        mockMvc.perform(get("/notify/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.recipient").value("+15551234567"))
                .andExpect(jsonPath("$.channel").value("SMS"))
                .andExpect(jsonPath("$.message").value("hi there"));

        // ...and within a few seconds GET reports SENT with a sentAt timestamp.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                mockMvc.perform(get("/notify/{id}", id))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SENT"))
                        .andExpect(jsonPath("$.sentAt").exists()));
    }

    @Test
    void providerFailureTransitionsToFailed() throws Exception {
        String body = """
                {"recipient":"fail@example.com","channel":"EMAIL","message":"boom"}
                """;

        MvcResult result = mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        UUID id = idOf(result);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.FAILED);
            assertThat(stored.getSentAt()).isNull();
        });
    }

    @Test
    void manyRequestsReturnFastAndAllReachTerminalState() throws Exception {
        int n = 12;
        List<UUID> ids = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            String body = String.format(
                    "{\"recipient\":\"user%d@example.com\",\"channel\":\"EMAIL\",\"message\":\"m%d\"}",
                    i, i);

            long start = System.nanoTime();
            MvcResult result = mockMvc.perform(post("/notify")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.status").value("PENDING"))
                    .andReturn();
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertThat(elapsedMillis)
                    .as("each POST must be fast and not block on delivery")
                    .isLessThan(500);
            ids.add(idOf(result));
        }

        // With 4 workers and ~800ms/send, all 12 settle in roughly 3 batches.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            for (UUID id : ids) {
                Notification stored = repository.findById(id).orElseThrow();
                assertThat(stored.getStatus()).isIn(TERMINAL);
            }
        });
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

    @Test
    void notificationStartsPending() {
        Notification fresh = new Notification("bob@example.com",
                com.example.notification.domain.Channel.PUSH, "ping");
        assertThat(fresh.getStatus()).isEqualTo(NotificationStatus.PENDING);
        assertThat(fresh.getSentAt()).isNull();
    }

    private UUID idOf(MvcResult result) throws Exception {
        JsonNode json = objectMapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(json.get("id").asText());
    }
}
