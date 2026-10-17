package com.example.notification.web;

import com.example.notification.ratelimit.RateLimitInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers the request interceptors. Order matters: identity (API key) runs first,
 * then the per-tenant rate limiter. Operator metrics use a separate credential so a
 * tenant API key never grants visibility into service-wide data.
 *
 * <p>Auth covers the reads too ({@code GET /notify/{id}}, {@code GET /notifications}),
 * which are tenant-scoped in their controllers. The rate limiter stays on
 * {@code POST /notify} only — reads do not consume the tenant's send budget.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ApiKeyAuthInterceptor apiKeyAuthInterceptor;
    private final RateLimitInterceptor rateLimitInterceptor;
    private final ManagementAuthInterceptor managementAuthInterceptor;

    public WebConfig(ApiKeyAuthInterceptor apiKeyAuthInterceptor,
                     RateLimitInterceptor rateLimitInterceptor,
                     ManagementAuthInterceptor managementAuthInterceptor) {
        this.apiKeyAuthInterceptor = apiKeyAuthInterceptor;
        this.rateLimitInterceptor = rateLimitInterceptor;
        this.managementAuthInterceptor = managementAuthInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(apiKeyAuthInterceptor)
                .addPathPatterns("/notify", "/notify/*", "/notifications");
        registry.addInterceptor(rateLimitInterceptor).addPathPatterns("/notify");
        registry.addInterceptor(managementAuthInterceptor)
                .addPathPatterns("/actuator/prometheus", "/actuator/metrics", "/actuator/metrics/**");
    }
}
