package com.example.notification.tenant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

/**
 * A tenant (API consumer). Identified to the API by an API key, and carries its own
 * per-minute request budget ({@code rateLimitPerMinute}).
 *
 * <p>The key itself is never stored or held in memory on this entity — only its
 * SHA-256 hash (see {@link ApiKeyHasher}). A caller presents the plaintext key in the
 * {@code X-API-Key} header; the interceptor hashes it and looks the tenant up by hash.
 * A dump of the {@code tenants} table therefore yields no usable credentials.
 */
@Entity
@Table(name = "tenants")
public class Tenant {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private String name;

    /**
     * Lowercase hex SHA-256 of the API key — 64 characters. Unique, and indexed by
     * {@code uk_tenants_api_key_hash}, because it is the lookup key on every request.
     */
    @Column(name = "api_key_hash", nullable = false, unique = true)
    private String apiKeyHash;

    @Column(name = "rate_limit_per_minute", nullable = false)
    private int rateLimitPerMinute;

    protected Tenant() {
        // for JPA
    }

    /**
     * Creates a tenant from a <em>plaintext</em> API key, which is hashed here and then
     * discarded. The caller is responsible for delivering that plaintext to the client
     * once — it cannot be recovered from this object or from the database afterwards.
     */
    public Tenant(String name, String apiKey, int rateLimitPerMinute) {
        this.id = UUID.randomUUID();
        this.name = name;
        this.apiKeyHash = ApiKeyHasher.sha256Hex(apiKey);
        this.rateLimitPerMinute = rateLimitPerMinute;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getApiKeyHash() {
        return apiKeyHash;
    }

    public int getRateLimitPerMinute() {
        return rateLimitPerMinute;
    }
}
