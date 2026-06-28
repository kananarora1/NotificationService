
package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.service.CreateResult;
import com.example.notification.service.NotificationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/notify")
public class NotificationController {

    private final NotificationService service;

    public NotificationController(NotificationService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<NotifyResponse> notify(
            @Valid @RequestBody NotifyRequest request,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {
        Notification notification = new Notification(
                request.recipient(),
                request.channel(),
                request.message()
        );
        // Persist as PENDING and publish for async delivery (202). If the
        // Idempotency-Key matches an existing notification, the existing one is
        // returned without creating or publishing a duplicate (200).
        CreateResult result = service.create(notification, idempotencyKey);
        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(NotifyResponse.from(result.notification()));
    }

    @GetMapping("/{id}")
    public NotificationView get(@PathVariable UUID id) {
        return NotificationView.from(service.get(id));
    }
}
