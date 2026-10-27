package com.example.notification.provider;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrevoEmailProviderTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsEmailWithBrevoAuthenticationAndNotificationIdempotencyKey() throws Exception {
        AtomicReference<String> apiKey = new AtomicReference<>();
        AtomicReference<JsonNode> payload = new AtomicReference<>();
        startServer(exchange -> {
            apiKey.set(exchange.getRequestHeaders().getFirst("api-key"));
            payload.set(objectMapper.readTree(exchange.getRequestBody()));
            respond(exchange, 201, "{\"messageId\":\"<brevo@example.test>\"}");
        });
        Notification notification = new Notification("recipient@example.com", Channel.EMAIL, "Hello from RelayNest");

        provider().send(notification);

        assertThat(provider().requiresDeliveryConfirmation()).isTrue();

        assertThat(apiKey.get()).isEqualTo("test-api-key");
        assertThat(payload.get().at("/sender/email").asText()).isEqualTo("notifications@relaynest.tech");
        assertThat(payload.get().at("/sender/name").asText()).isEqualTo("RelayNest Notifications");
        assertThat(payload.get().at("/to/0/email").asText()).isEqualTo("recipient@example.com");
        assertThat(payload.get().at("/textContent").asText()).isEqualTo("Hello from RelayNest");
        assertThat(payload.get().at("/headers/idempotencyKey").asText()).isEqualTo(notification.getId().toString());
        assertThat(payload.get().at("/replyTo/email").asText()).isEqualTo("support@relaynest.tech");
    }

    @Test
    void failsWhenBrevoRejectsTheMessage() throws Exception {
        startServer(exchange -> respond(exchange, 401, "{\"code\":\"unauthorized\"}"));

        assertThatThrownBy(() -> provider().send(new Notification("recipient@example.com", Channel.EMAIL, "Hello")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HTTP 401");
    }

    @Test
    void rejectsNonEmailChannelsBeforeMakingANetworkRequest() {
        assertThatThrownBy(() -> provider().send(new Notification("+15551234567", Channel.SMS, "Hello")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supports EMAIL");
    }

    private BrevoEmailProvider provider() {
        String apiUrl = server == null
                ? "http://localhost:1/v3/smtp/email"
                : "http://localhost:" + server.getAddress().getPort() + "/v3/smtp/email";
        BrevoProperties properties = new BrevoProperties(
                apiUrl,
                "test-api-key",
                "notifications@relaynest.tech",
                "RelayNest Notifications",
                "support@relaynest.tech",
                "RelayNest notification",
                "relaynest",
                "webhook-secret");
        return new BrevoEmailProvider(properties, objectMapper, HttpClient.newHttpClient());
    }

    private void startServer(ExchangeHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v3/smtp/email", exchange -> handler.handle(exchange));
        server.start();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
