package com.example.notification.ratelimit;

/**
 * The tenant's token bucket is empty. Thrown by {@link RateLimitInterceptor} and turned
 * into a 429 problem response, with {@code Retry-After}, by {@code ApiExceptionHandler}.
 */
public class RateLimitExceededException extends RuntimeException {

    private final int limitPerMinute;
    private final long retryAfterSeconds;

    public RateLimitExceededException(int limitPerMinute, long retryAfterSeconds) {
        super("Rate limit of " + limitPerMinute + " requests per minute exceeded");
        this.limitPerMinute = limitPerMinute;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int getLimitPerMinute() {
        return limitPerMinute;
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
