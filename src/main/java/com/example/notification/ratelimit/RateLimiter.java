package com.example.notification.ratelimit;

import com.example.notification.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * Distributed per-tenant rate limiter backed by Redis.
 *
 * <p>The check-and-decrement is performed by an atomic Lua script ({@code EVAL}), so
 * the limit holds correctly across multiple app instances sharing one Redis — the
 * property the v6 in-memory limiter could not provide. Java never does GET-then-SET
 * (that would reintroduce the TOCTOU race); all decisions happen inside Redis.
 *
 * <p><b>Redis-down policy: fail-open.</b> If the Redis call fails, we log and
 * <em>allow</em> the request. For a notification system, delivering is preferable to
 * blocking on a rate-limiter outage. Flip {@link #FAIL_OPEN} to change this.
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    /** When Redis is unreachable: true = allow (fail-open), false = deny (fail-closed). */
    private static final boolean FAIL_OPEN = true;

    private static final long TTL_MILLIS = Duration.ofMinutes(2).toMillis();

    private final StringRedisTemplate redis;
    private final RedisScript<List> tokenBucketScript;
    private final Clock clock;

    public RateLimiter(StringRedisTemplate redis,
                       RedisScript<List> tokenBucketScript,
                       Clock clock) {
        this.redis = redis;
        this.tokenBucketScript = tokenBucketScript;
        this.clock = clock;
    }

    public RateLimitDecision tryConsume(Tenant tenant) {
        int capacity = tenant.getRateLimitPerMinute();
        double refillPerMs = capacity / 60_000.0;
        String key = "ratelimit:" + tenant.getId();

        try {
            @SuppressWarnings("unchecked")
            List<Long> result = (List<Long>) redis.execute(
                    tokenBucketScript,
                    List.of(key),
                    Integer.toString(capacity),
                    Double.toString(refillPerMs),
                    Long.toString(clock.millis()),
                    "1",
                    Long.toString(TTL_MILLIS));

            boolean allowed = result != null && result.get(0) != null && result.get(0) == 1L;
            if (allowed) {
                return RateLimitDecision.allow();
            }
            long retryAfterMs = result != null && result.get(1) != null ? result.get(1) : 1000L;
            long retryAfterSeconds = Math.max(1L, (long) Math.ceil(retryAfterMs / 1000.0));
            return RateLimitDecision.deny(retryAfterSeconds);
        } catch (RuntimeException e) {
            if (FAIL_OPEN) {
                log.warn("Rate-limiter Redis call failed; failing OPEN for tenant {}", tenant.getId(), e);
                return RateLimitDecision.allow();
            }
            log.warn("Rate-limiter Redis call failed; failing CLOSED for tenant {}", tenant.getId(), e);
            return RateLimitDecision.deny(1);
        }
    }
}
