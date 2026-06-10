package com.example.notification.service;

import java.util.UUID;

/**
 * Raised when a notification cannot be found by id. Surfaced as HTTP 404.
 */
public class NotificationNotFoundException extends RuntimeException {

    public NotificationNotFoundException(UUID id) {
        super("Notification not found: " + id);
    }
}
