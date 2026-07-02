package com.example.notification.ratelimit;

import com.example.notification.tenant.Tenant;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-tenant token-bucket rate limiter.
 *
 * <p><b>Deliberately in-memory.</b> Buckets live in a {@link ConcurrentHashMap} keyed
 * by tenant id, in this JVM only — there is no Redis or shared store. That makes the
 * limit <em>per instance</em>: running N app instances admits up to N× the configured
 * rate. This is a known limitation, to be addressed in v7.
 */
@Component
public class RateLimiter {

    private final Clock clock;
    private final ConcurrentHashMap<UUID, TokenBucket> buckets = new ConcurrentHashMap<>();

    public RateLimiter(Clock clock) {
        this.clock = clock;
    }

    public RateLimitDecision tryConsume(Tenant tenant) {
        TokenBucket bucket = buckets.computeIfAbsent(
                tenant.getId(),
                id -> new TokenBucket(tenant.getRateLimitPerMinute(), clock.millis()));
        return bucket.tryConsume(clock.millis());
    }
}
