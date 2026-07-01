package com.example.notification.tenant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

/**
 * A tenant (API consumer). Identified to the API by a unique {@code apiKey}, and
 * carries its own per-minute request budget ({@code rateLimitPerMinute}).
 */
@Entity
@Table(name = "tenants")
public class Tenant {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(name = "api_key", nullable = false, unique = true)
    private String apiKey;

    @Column(name = "rate_limit_per_minute", nullable = false)
    private int rateLimitPerMinute;

    protected Tenant() {
        // for JPA
    }

    public Tenant(String name, String apiKey, int rateLimitPerMinute) {
        this.id = UUID.randomUUID();
        this.name = name;
        this.apiKey = apiKey;
        this.rateLimitPerMinute = rateLimitPerMinute;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getApiKey() {
        return apiKey;
    }

    public int getRateLimitPerMinute() {
        return rateLimitPerMinute;
    }
}
