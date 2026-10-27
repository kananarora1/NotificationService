package com.example.notification.config;

import com.example.notification.tenant.TenantSeedProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Refuses an incomplete production configuration before any startup runner can
 * create development tenants. {@code application-prod.yml} removes the normal
 * fallbacks; this validator also makes the error explicit for deployments that
 * supply configuration through a non-YAML property source.
 */
@Component
@Profile("prod")
public class ProductionConfigurationValidator {

    private static final List<String> REQUIRED_ENVIRONMENT_VARIABLES = List.of(
            "DB_URL",
            "DB_USERNAME",
            "DB_PASSWORD",
            "REDIS_HOST",
            "REDIS_PASSWORD",
            "RABBITMQ_HOST",
            "RABBITMQ_USERNAME",
            "RABBITMQ_PASSWORD",
            "MANAGEMENT_API_TOKEN",
            "BREVO_API_KEY",
            "BREVO_SENDER_EMAIL",
            "BREVO_WEBHOOK_USERNAME",
            "BREVO_WEBHOOK_PASSWORD"
    );

    private final Environment environment;
    private final TenantSeedProperties tenantSeedProperties;

    public ProductionConfigurationValidator(Environment environment, TenantSeedProperties tenantSeedProperties) {
        this.environment = environment;
        this.tenantSeedProperties = tenantSeedProperties;
    }

    @PostConstruct
    void validate() {
        List<String> missing = REQUIRED_ENVIRONMENT_VARIABLES.stream()
                .filter(name -> environment.getProperty(name) == null || environment.getProperty(name).isBlank())
                .toList();

        if (!missing.isEmpty() || tenantSeedProperties.enabled()) {
            String details = missing.isEmpty() ? "" : "Missing required variables: " + String.join(", ", missing) + ". ";
            String seeding = tenantSeedProperties.enabled()
                    ? "notifications.seeding.enabled must be false in the prod profile."
                    : "";
            throw new IllegalStateException("Unsafe production configuration. " + details + seeding);
        }
    }
}
