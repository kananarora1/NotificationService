package com.example.notification.tenant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Seeds a small fixed set of tenants on startup. Seeding is idempotent (keyed on the
 * unique api key) so it is safe to run repeatedly and across multiple app instances
 * sharing the same Postgres — a concurrent double-insert simply hits the UNIQUE
 * constraint and is ignored.
 */
@Component
public class TenantSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TenantSeeder.class);

    private static final List<Tenant> SEEDS = List.of(
            new Tenant("acme", "key-acme", 60),
            new Tenant("globex", "key-globex", 10),
            new Tenant("initech", "key-initech", 5));

    private final TenantRepository tenants;

    public TenantSeeder(TenantRepository tenants) {
        this.tenants = tenants;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (Tenant seed : SEEDS) {
            if (tenants.findByApiKey(seed.getApiKey()).isPresent()) {
                continue;
            }
            try {
                tenants.save(new Tenant(seed.getName(), seed.getApiKey(), seed.getRateLimitPerMinute()));
                log.info("Seeded tenant '{}' (limit {}/min)", seed.getName(), seed.getRateLimitPerMinute());
            } catch (DataIntegrityViolationException e) {
                // Another instance seeded this tenant concurrently — fine.
                log.debug("Tenant '{}' already seeded by another instance", seed.getName());
            }
        }
    }
}
