package com.example.notification.ratelimit;

/**
 * A classic token bucket. Capacity equals the per-minute limit, and tokens refill
 * continuously at {@code limit / 60_000} tokens per millisecond. The bucket starts
 * full. All state is in process; time is supplied by the caller (so a test clock
 * can drive refills deterministically).
 */
class TokenBucket {

    private final double capacity;
    private final double refillPerMilli;

    private double tokens;
    private long lastRefillMillis;

    TokenBucket(int ratePerMinute, long nowMillis) {
        this.capacity = ratePerMinute;
        this.refillPerMilli = ratePerMinute / 60_000.0;
        this.tokens = ratePerMinute; // start full
        this.lastRefillMillis = nowMillis;
    }

    synchronized RateLimitDecision tryConsume(long nowMillis) {
        refill(nowMillis);
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return RateLimitDecision.allow();
        }
        return RateLimitDecision.deny(retryAfterSeconds());
    }

    private void refill(long nowMillis) {
        if (nowMillis > lastRefillMillis) {
            double refilled = (nowMillis - lastRefillMillis) * refillPerMilli;
            tokens = Math.min(capacity, tokens + refilled);
            lastRefillMillis = nowMillis;
        }
    }

    /** Seconds until the bucket holds at least one token (rounded up, minimum 1). */
    private long retryAfterSeconds() {
        double tokensNeeded = 1.0 - tokens;
        double millis = tokensNeeded / refillPerMilli;
        return Math.max(1L, (long) Math.ceil(millis / 1000.0));
    }
}
