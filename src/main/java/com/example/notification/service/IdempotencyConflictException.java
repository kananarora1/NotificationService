package com.example.notification.service;

/**
 * Raised when a client reuses an idempotency key for a notification with different
 * delivery details. Reusing a key is valid only for a retry of the original request.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("Idempotency-Key was already used with a different notification request.");
    }
}
