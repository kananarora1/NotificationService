
package com.example.notification.api;

import com.example.notification.domain.Notification;
import com.example.notification.service.NotificationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
    public ResponseEntity<NotifyResponse> notify(@Valid @RequestBody NotifyRequest request) {
        Notification notification = new Notification(
                request.recipient(),
                request.channel(),
                request.message()
        );
        // Persist as PENDING and enqueue for async delivery; do not block on the
        // provider here. Returns 202 Accepted with {id, status: PENDING}.
        Notification accepted = service.create(notification);
        return ResponseEntity.accepted().body(NotifyResponse.from(accepted));
    }

    @GetMapping("/{id}")
    public NotificationView get(@PathVariable UUID id) {
        return NotificationView.from(service.get(id));
    }
}
