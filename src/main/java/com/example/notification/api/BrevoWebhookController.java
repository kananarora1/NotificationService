package com.example.notification.api;

import com.example.notification.provider.BrevoWebhookPayload;
import com.example.notification.provider.BrevoWebhookService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Receives authenticated delivery callbacks from Brevo; authentication runs in a servlet filter. */
@RestController
@RequestMapping("/webhooks/brevo")
public class BrevoWebhookController {

    private final BrevoWebhookService service;

    public BrevoWebhookController(BrevoWebhookService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Void> receive(@RequestBody BrevoWebhookPayload payload) {
        service.record(payload);
        return ResponseEntity.noContent().build();
    }
}
