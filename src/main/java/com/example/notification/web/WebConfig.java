package com.example.notification.web;

import com.example.notification.ratelimit.RateLimitInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers the request interceptors. Order matters: identity (API key) runs first,
 * then the per-tenant rate limiter.
 *
 * <p>Auth covers the reads too ({@code GET /notify/{id}}, {@code GET /notifications}),
 * which are tenant-scoped in their controllers. The rate limiter stays on
 * {@code POST /notify} only — reads do not consume the tenant's send budget.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ApiKeyAuthInterceptor apiKeyAuthInterceptor;
    private final RateLimitInterceptor rateLimitInterceptor;

    public WebConfig(ApiKeyAuthInterceptor apiKeyAuthInterceptor,
                     RateLimitInterceptor rateLimitInterceptor) {
        this.apiKeyAuthInterceptor = apiKeyAuthInterceptor;
        this.rateLimitInterceptor = rateLimitInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(apiKeyAuthInterceptor)
                .addPathPatterns("/notify", "/notify/*", "/notifications");
        registry.addInterceptor(rateLimitInterceptor).addPathPatterns("/notify");
    }
}
