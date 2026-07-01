package com.example.notification.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers the API-key identity interceptor for {@code POST /notify}.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ApiKeyAuthInterceptor apiKeyAuthInterceptor;

    public WebConfig(ApiKeyAuthInterceptor apiKeyAuthInterceptor) {
        this.apiKeyAuthInterceptor = apiKeyAuthInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(apiKeyAuthInterceptor).addPathPatterns("/notify");
    }
}
