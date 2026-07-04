package com.example.notification.ratelimit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Supplies the {@link Clock} used by the {@link RateLimiter} for token refill.
 *
 * <p>Kept separate from {@code WebConfig} on purpose: {@code WebConfig} depends on
 * the rate-limit interceptor (→ limiter → clock), so defining the clock bean on
 * {@code WebConfig} itself would form a dependency cycle. Tests override this with a
 * controllable clock via {@code @Primary}.
 */
@Configuration
public class RateLimitConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }
}
