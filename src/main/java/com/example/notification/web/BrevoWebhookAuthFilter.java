package com.example.notification.web;

import com.example.notification.provider.BrevoProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Brevo supports HTTP Basic authentication in a webhook URL. Authenticate before MVC reads the
 * payload so arbitrary callers cannot consume JSON-deserialization capacity on this public URL.
 */
@Component
public class BrevoWebhookAuthFilter extends OncePerRequestFilter {

    private static final String WEBHOOK_PATH = "/webhooks/brevo";

    private final BrevoProperties properties;

    public BrevoWebhookAuthFilter(BrevoProperties properties) {
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // getServletPath() is empty under some servlet mappings (and in MockMvc). The request
        // URI minus the context path is stable in both embedded Tomcat and a reverse-proxy mount.
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return !WEBHOOK_PATH.equals(path);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        if (!isAuthorized(request.getHeader(HttpHeaders.AUTHORIZATION))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"RelayNest Brevo webhook\"");
            return;
        }
        filterChain.doFilter(request, response);
    }

    boolean isAuthorized(String authorization) {
        if (!StringUtils.hasText(properties.webhookUsername())
                || !StringUtils.hasText(properties.webhookPassword())
                || !StringUtils.hasText(authorization)
                || !authorization.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
            return false;
        }

        final String presented;
        try {
            byte[] decoded = Base64.getDecoder().decode(authorization.substring("Basic ".length()));
            presented = new String(decoded, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            return false;
        }

        int separator = presented.indexOf(':');
        if (separator < 0) {
            return false;
        }
        String username = presented.substring(0, separator);
        String password = presented.substring(separator + 1);
        return MessageDigest.isEqual(properties.webhookUsername().getBytes(StandardCharsets.UTF_8),
                username.getBytes(StandardCharsets.UTF_8))
                && MessageDigest.isEqual(properties.webhookPassword().getBytes(StandardCharsets.UTF_8),
                password.getBytes(StandardCharsets.UTF_8));
    }
}
