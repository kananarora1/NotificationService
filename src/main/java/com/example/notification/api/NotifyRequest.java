package com.example.notification.api;

import com.example.notification.domain.Channel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record NotifyRequest(
        @NotBlank String recipient,
        @NotNull Channel channel,
        @NotBlank String message
) {
}
