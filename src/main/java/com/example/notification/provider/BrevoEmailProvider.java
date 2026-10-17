package com.example.notification.provider;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Sends EMAIL notifications through Brevo's transactional-email API in production. */
@Component
@Profile("prod | brevo")
public class BrevoEmailProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(BrevoEmailProvider.class);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final BrevoProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Autowired
    public BrevoEmailProvider(BrevoProperties properties, ObjectMapper objectMapper) {
        this(properties, objectMapper, HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
    }

    BrevoEmailProvider(BrevoProperties properties, ObjectMapper objectMapper, HttpClient httpClient) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
    }

    @Override
    public void send(Notification notification) {
        if (notification.getChannel() != Channel.EMAIL) {
            throw new IllegalArgumentException("BrevoEmailProvider only supports EMAIL notifications");
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(properties.apiUrl()))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("api-key", properties.apiKey())
                .POST(HttpRequest.BodyPublishers.ofString(requestBody(notification)))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Brevo rejected notification " + notification.getId()
                        + " with HTTP " + response.statusCode());
            }
            log.info("Brevo accepted EMAIL notification {}", notification.getId());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending notification " + notification.getId(), exception);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not reach Brevo for notification " + notification.getId(), exception);
        }
    }

    private String requestBody(Notification notification) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sender", Map.of("email", properties.senderEmail(), "name", properties.senderName()));
        body.put("to", List.of(Map.of("email", notification.getRecipient())));
        body.put("subject", properties.subject());
        body.put("textContent", notification.getMessage());
        body.put("tags", List.of("notification-service"));
        body.put("headers", Map.of(
                "idempotencyKey", notification.getId().toString(),
                "X-Mailin-custom", "notification-id:" + notification.getId()));
        if (StringUtils.hasText(properties.replyToEmail())) {
            body.put("replyTo", Map.of("email", properties.replyToEmail()));
        }

        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize notification " + notification.getId(), exception);
        }
    }
}
