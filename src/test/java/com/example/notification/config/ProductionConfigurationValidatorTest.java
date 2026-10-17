package com.example.notification.config;

import com.example.notification.tenant.TenantSeedProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductionConfigurationValidatorTest {

    @Test
    void acceptsCompleteConfigurationWithTenantSeedingDisabled() {
        assertThatCode(() -> validator(completeEnvironment(), false).validate())
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingProductionCredential() {
        MockEnvironment environment = completeEnvironment();
        environment.setProperty("DB_PASSWORD", "");

        assertThatThrownBy(() -> validator(environment, false).validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DB_PASSWORD");
    }

    @Test
    void rejectsDevelopmentTenantSeedingInProduction() {
        assertThatThrownBy(() -> validator(completeEnvironment(), true).validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("notifications.seeding.enabled must be false");
    }

    private static ProductionConfigurationValidator validator(MockEnvironment environment, boolean seedingEnabled) {
        TenantSeedProperties properties = new TenantSeedProperties(seedingEnabled, List.of());
        return new ProductionConfigurationValidator(environment, properties);
    }

    private static MockEnvironment completeEnvironment() {
        return new MockEnvironment()
                .withProperty("DB_URL", "jdbc:postgresql://db.internal/notifications")
                .withProperty("DB_USERNAME", "notification_service")
                .withProperty("DB_PASSWORD", "a-real-secret")
                .withProperty("REDIS_HOST", "redis.internal")
                .withProperty("REDIS_PASSWORD", "a-real-secret")
                .withProperty("RABBITMQ_HOST", "rabbitmq.internal")
                .withProperty("RABBITMQ_USERNAME", "notification_service")
                .withProperty("RABBITMQ_PASSWORD", "a-real-secret")
                .withProperty("MANAGEMENT_API_TOKEN", "operator-secret");
    }
}
