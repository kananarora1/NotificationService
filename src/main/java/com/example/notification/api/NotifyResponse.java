package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;

import java.util.UUID;

/**
 * Minimal response returned from POST /notify.
 */
public record NotifyResponse(UUID id, NotificationStatus status) {

    public static NotifyResponse from(Notification notification) {
        return new NotifyResponse(notification.getId(), notification.getStatus());
    }
}
