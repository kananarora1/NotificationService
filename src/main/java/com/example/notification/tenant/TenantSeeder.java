package com.example.notification.tenant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/**
 * Seeds the configured tenants on startup (see {@link TenantSeedProperties}). Seeding is
 * idempotent (keyed on the unique api key hash) so it is safe to run repeatedly and
 * across multiple app instances sharing the same Postgres — a concurrent double-insert
 * simply hits the UNIQUE constraint and is ignored.
 *
 * <p>Only the SHA-256 hash of each key is written; the plaintext lives in configuration
 * and never reaches the database.
 */
@Component
public class TenantSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TenantSeeder.class);

    private final TenantRepository tenants;
    private final TenantSeedProperties properties;

    public TenantSeeder(TenantRepository tenants, TenantSeedProperties properties) {
        this.tenants = tenants;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.enabled()) {
            log.info("Tenant seeding disabled; expecting tenants to be provisioned externally");
            return;
        }

        for (TenantSeedProperties.SeedTenant seed : properties.tenants()) {
            // Existence is checked by hash — the same lookup the auth path uses.
            if (tenants.findByApiKeyHash(ApiKeyHasher.sha256Hex(seed.apiKey())).isPresent()) {
                continue;
            }
            try {
                tenants.save(new Tenant(seed.name(), seed.apiKey(), seed.rateLimitPerMinute()));
                // The key itself is never logged.
                log.info("Seeded tenant '{}' (limit {}/min)", seed.name(), seed.rateLimitPerMinute());
            } catch (DataIntegrityViolationException e) {
                // Another instance seeded this tenant concurrently — fine.
                log.debug("Tenant '{}' already seeded by another instance", seed.name());
            }
        }
    }
}
