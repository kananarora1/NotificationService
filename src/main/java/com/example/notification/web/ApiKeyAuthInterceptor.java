package com.example.notification.web;

import com.example.notification.tenant.ApiKeyHasher;
import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Optional;

/**
 * Identity gate for the notification endpoints (see {@code WebConfig} for the exact
 * paths). Requires an {@code X-API-Key} header, resolves it to a {@link Tenant} by the
 * SHA-256 hash of that key (the plaintext is never stored — see {@link ApiKeyHasher}), and
 * stashes the tenant as a request attribute for downstream use (rate limiting,
 * persisting tenant_id, scoping reads). A missing or unknown key is rejected with 401
 * before the controller runs. Applies to every HTTP method, reads included — the read
 * endpoints return other tenants' recipients and message bodies otherwise.
 */
@Component
public class ApiKeyAuthInterceptor implements HandlerInterceptor {

    public static final String API_KEY_HEADER = "X-API-Key";
    public static final String TENANT_ATTRIBUTE = "tenant";

    private final TenantRepository tenants;

    public ApiKeyAuthInterceptor(TenantRepository tenants) {
        this.tenants = tenants;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String apiKey = request.getHeader(API_KEY_HEADER);
        if (!StringUtils.hasText(apiKey)) {
            throw new ApiAuthenticationException("Missing " + API_KEY_HEADER + " header.");
        }

        // Hash first, then look up by hash: a single indexed lookup, same cost as the
        // old plaintext one. The presented key is never compared against stored
        // plaintext because none exists.
        Optional<Tenant> tenant = tenants.findByApiKeyHash(ApiKeyHasher.sha256Hex(apiKey));
        if (tenant.isEmpty()) {
            // Same answer for every unrecognised key: nothing about which keys exist.
            throw new ApiAuthenticationException("Unknown API key.");
        }

        request.setAttribute(TENANT_ATTRIBUTE, tenant.get());
        return true;
    }
}
