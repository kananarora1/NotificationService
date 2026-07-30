package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.repository.NotificationRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only listing endpoint powering the demo UI's live table. Open (no API key):
 * only POST /notify is gated. Newest-first, capped at 200 rows, optionally filtered
 * by status.
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
            @RequestParam(required = false) NotificationStatus status) {
        Pageable page = PageRequest.of(0, Math.min(Math.max(limit, 1), MAX_LIMIT));

        List<Notification> rows = (status == null)
                ? repository.findByOrderByCreatedAtDesc(page)
                : repository.findByStatusOrderByCreatedAtDesc(status, page);

        return rows.stream()
                .map(n -> NotificationSummary.of(n, metrics.tenantName(n.getTenantId())))
                .toList();
    }
}
