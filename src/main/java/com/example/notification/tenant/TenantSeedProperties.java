package com.example.notification.tenant;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Tenants to seed on startup, bound from {@code notifications.seeding.*}.
 *
 * <p>The API keys here are supplied by configuration rather than hardcoded, so they can
 * be injected from the environment. Seeding is a <em>local-development convenience</em>:
 * in production set {@code SEED_TENANTS_ENABLED=false} and provision tenants
 * deliberately (generate a random key, hand it to the client once, store only its hash
 * — see the README).
 *
 * @param enabled whether to seed at all
 * @param tenants the tenants to ensure exist
 */
@ConfigurationProperties(prefix = "notifications.seeding")
public record TenantSeedProperties(boolean enabled, List<SeedTenant> tenants) {

    public TenantSeedProperties {
        tenants = tenants == null ? List.of() : List.copyOf(tenants);
    }

    /**
     * @param apiKey plaintext key, hashed before it is stored and never persisted as-is
     */
    public record SeedTenant(String name, String apiKey, int rateLimitPerMinute) {
    }
}
