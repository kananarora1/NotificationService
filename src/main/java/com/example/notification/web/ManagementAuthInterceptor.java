package com.example.notification.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Protects the metrics surfaces from being scraped by arbitrary callers.
 * Health remains outside this interceptor so a platform liveness probe does not
 * need an operator credential.
 */
@Component
public class ManagementAuthInterceptor implements HandlerInterceptor {

    public static final String MANAGEMENT_TOKEN_HEADER = "X-Management-Token";

    private final ManagementSecurityProperties properties;

    public ManagementAuthInterceptor(ManagementSecurityProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!properties.enabled()) {
            return true;
        }

        String supplied = request.getHeader(MANAGEMENT_TOKEN_HEADER);
        if (!StringUtils.hasText(supplied)
                || !MessageDigest.isEqual(properties.token().getBytes(StandardCharsets.UTF_8),
                supplied.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }
        return true;
    }
}
