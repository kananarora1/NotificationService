package com.example.notification.ratelimit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.util.List;

/**
 * Beans for the Redis-backed rate limiter: the system {@link Clock} (supplies "now"
 * to the token-bucket script) and the loaded Lua {@link RedisScript}.
 *
 * <p>Kept separate from {@code WebConfig} on purpose: {@code WebConfig} depends on
 * the rate-limit interceptor (→ limiter → clock), so defining the clock bean on
 * {@code WebConfig} itself would form a dependency cycle. Tests override the clock
 * with a controllable one via {@code @Primary}.
 */
@Configuration
public class RateLimitConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }

    @Bean
    public RedisScript<List> tokenBucketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("ratelimit/token_bucket.lua"));
        script.setResultType(List.class);
        return script;
    }
}
