package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.tenant.Tenant;
import com.example.notification.web.ApiKeyAuthInterceptor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only listing endpoint powering the demo UI's live table. Requires an API key
 * and returns only the calling tenant's rows. Newest-first, capped at 200 rows,
 * optionally filtered by status.
 */
@RestController
public class NotificationListController {

    private static final int MAX_LIMIT = 200;

    private final NotificationRepository repository;
    private final NotificationMetrics metrics; // reused only for tenant-id -> name resolution

    public NotificationListController(NotificationRepository repository, NotificationMetrics metrics) {
        this.repository = repository;
        this.metrics = metrics;
    }

    @GetMapping("/notifications")
    public List<NotificationSummary> list(
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(required = false) NotificationStatus status,
            @RequestAttribute(ApiKeyAuthInterceptor.TENANT_ATTRIBUTE) Tenant tenant) {
        Pageable page = PageRequest.of(0, Math.min(Math.max(limit, 1), MAX_LIMIT));

        List<Notification> rows = (status == null)
                ? repository.findByTenantIdOrderByCreatedAtDesc(tenant.getId(), page)
                : repository.findByTenantIdAndStatusOrderByCreatedAtDesc(tenant.getId(), status, page);

        return rows.stream()
                .map(n -> NotificationSummary.of(n, metrics.tenantName(n.getTenantId())))
                .toList();
    }
}
