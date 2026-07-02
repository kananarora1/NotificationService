package com.example.notification.ratelimit;

/**
 * Result of attempting to consume a token. When {@code allowed} is false,
 * {@code retryAfterSeconds} is a hint (for the HTTP {@code Retry-After} header) of
 * how long until at least one token is available again.
 */
public record RateLimitDecision(boolean allowed, long retryAfterSeconds) {

    static RateLimitDecision allow() {
        return new RateLimitDecision(true, 0);
    }

    static RateLimitDecision deny(long retryAfterSeconds) {
        return new RateLimitDecision(false, retryAfterSeconds);
    }
}
