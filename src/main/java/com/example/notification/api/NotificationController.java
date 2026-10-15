
package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.metrics.NotificationMetrics;
import com.example.notification.service.CreateResult;
import com.example.notification.service.NotificationService;
import com.example.notification.tenant.Tenant;
import com.example.notification.web.ApiKeyAuthInterceptor;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/notify")
public class NotificationController {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    /** The idempotency_key column is varchar(255). */
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 255;

    private final NotificationService service;
    private final NotificationMetrics metrics;

    public NotificationController(NotificationService service, NotificationMetrics metrics) {
        this.service = service;
        this.metrics = metrics;
    }

    @PostMapping
    public ResponseEntity<NotifyResponse> notify(
            @Valid @RequestBody NotifyRequest request,
            @RequestHeader(name = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKey,
            @RequestAttribute(ApiKeyAuthInterceptor.TENANT_ATTRIBUTE) Tenant tenant) {
        if (idempotencyKey != null && idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new InvalidRequestException(IDEMPOTENCY_KEY_HEADER,
                    "size must be between 0 and " + MAX_IDEMPOTENCY_KEY_LENGTH);
        }
        Notification notification = new Notification(
                request.recipient(),
                request.channel(),
                request.message()
        );
        notification.setTenantId(tenant.getId());
        // Persist as PENDING and publish for async delivery (202). If the
        // Idempotency-Key matches an existing notification, the existing one is
        // returned without creating or publishing a duplicate (200).
        CreateResult result = service.create(notification, idempotencyKey);
        if (result.created()) {
            metrics.submitted(tenant.getName(), request.channel().name());
        }
        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(NotifyResponse.from(result.notification()));
    }

    @GetMapping("/{id}")
    public NotificationView get(@PathVariable UUID id,
                                @RequestAttribute(ApiKeyAuthInterceptor.TENANT_ATTRIBUTE) Tenant tenant) {
        // Scoped to the caller's tenant: another tenant's id reads as 404.
        return NotificationView.from(service.get(id, tenant.getId()));
    }
}
