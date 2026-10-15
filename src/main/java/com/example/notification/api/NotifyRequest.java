package com.example.notification.api;

import com.example.notification.domain.Channel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Sizes match the varchar(255) columns, so an over-long value is a 400, not a failed INSERT. */
public record NotifyRequest(
        @NotBlank @Size(max = 255) String recipient,
        @NotNull Channel channel,
        @NotBlank @Size(max = 255) String message
) {
}
