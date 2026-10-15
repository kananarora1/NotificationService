package com.example.notification.ratelimit;

import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.tenant.Tenant;
import com.example.notification.web.ApiAuthenticationException;
import com.example.notification.web.ApiKeyAuthInterceptor;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Enforces the per-tenant rate limit on POST /notify. Runs after
 * {@link ApiKeyAuthInterceptor} (which has already resolved the tenant), tries to
 * consume one token, and on an empty bucket throws {@link RateLimitExceededException},
 * which becomes 429 Too Many Requests plus a {@code Retry-After} header.
 */
@Component
public class RateLimitInterceptor implements HandlerInterceptor {

    private final RateLimiter rateLimiter;
    private final NotificationMetrics metrics;

    public RateLimitInterceptor(RateLimiter rateLimiter, NotificationMetrics metrics) {
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }

        Tenant tenant = (Tenant) request.getAttribute(ApiKeyAuthInterceptor.TENANT_ATTRIBUTE);
        if (tenant == null) {
            // Auth interceptor should always have set this for POST; fail closed.
            throw new ApiAuthenticationException("Missing " + ApiKeyAuthInterceptor.API_KEY_HEADER + " header.");
        }

        RateLimitDecision decision = rateLimiter.tryConsume(tenant);
        if (!decision.allowed()) {
            metrics.rateLimitRejected(tenant.getName());
            throw new RateLimitExceededException(tenant.getRateLimitPerMinute(), decision.retryAfterSeconds());
        }

        return true;
    }
}
