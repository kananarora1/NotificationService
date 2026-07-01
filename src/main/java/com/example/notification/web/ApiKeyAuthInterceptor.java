package com.example.notification.web;

import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Optional;

/**
 * Identity gate for POST /notify. Requires an {@code X-API-Key} header, resolves it
 * to a {@link Tenant}, and stashes the tenant as a request attribute for downstream
 * use (rate limiting, persisting tenant_id). A missing or unknown key is rejected
 * with 401 before the controller runs.
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
        // Only POST /notify is gated; the read endpoint stays open.
        if (!HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }

        String apiKey = request.getHeader(API_KEY_HEADER);
        if (!StringUtils.hasText(apiKey)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }

        Optional<Tenant> tenant = tenants.findByApiKey(apiKey);
        if (tenant.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }

        request.setAttribute(TENANT_ATTRIBUTE, tenant.get());
        return true;
    }
}
