package com.example.notification.web;

import com.example.notification.ratelimit.RateLimitInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers the request interceptors for {@code POST /notify}. Order matters:
 * identity (API key) runs first, then the per-tenant rate limiter.
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
        registry.addInterceptor(apiKeyAuthInterceptor).addPathPatterns("/notify");
        registry.addInterceptor(rateLimitInterceptor).addPathPatterns("/notify");
    }
}
