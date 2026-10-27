package com.example.notification.provider;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** The fields needed to correlate and record a Brevo transactional-email callback. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrevoWebhookPayload(
        String event,
        Long id,
        @JsonProperty("X-Mailin-custom") String customHeader,
        @JsonProperty("ts_event") Long eventTimestampSeconds,
        String reason
) {
}
