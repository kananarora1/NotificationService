package com.example.notification.provider;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for Brevo's transactional-email API. */
@ConfigurationProperties(prefix = "notifications.brevo")
public record BrevoProperties(
        String apiUrl,
        String apiKey,
        String senderEmail,
        String senderName,
        String replyToEmail,
        String subject,
        String webhookUsername,
        String webhookPassword
) {
}
