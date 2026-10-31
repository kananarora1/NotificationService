package com.example.notification.ratelimit;

import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RateLimiterTest {

    @Test
    void redisFailureAllowsRequestAndRecordsFailOpen() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        RedisScript<List> script = mock(RedisScript.class);
        when(redis.execute(eq(script), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("Redis unavailable"));

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NotificationMetrics metrics = new NotificationMetrics(registry, mock(TenantRepository.class));
        RateLimiter limiter = new RateLimiter(redis, script, Clock.systemUTC(), metrics);

        assertThat(registry.get("ratelimit.fail_open").counter().count()).isZero();
        assertThat(limiter.tryConsume(new Tenant("acme", "test-key", 60)).allowed()).isTrue();
        assertThat(registry.get("ratelimit.fail_open").counter().count()).isEqualTo(1);
    }
}
