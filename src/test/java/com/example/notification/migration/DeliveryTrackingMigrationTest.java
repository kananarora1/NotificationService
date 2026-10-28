package com.example.notification.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DeliveryTrackingMigrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Test
    void upgradePreservesLegacySentNotifications() {
        var dataSource = new DriverManagerDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target(MigrationVersion.fromVersion("9"))
                .load()
                .migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        UUID tenantId = UUID.randomUUID();
        UUID notificationId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant sentAt = Instant.parse("2026-01-01T00:00:02Z");

        jdbc.update("""
                insert into tenants (id, name, api_key_hash, rate_limit_per_minute)
                values (?, 'legacy', ?, 10)
                """, tenantId, "0".repeat(64));
        jdbc.update("""
                insert into notifications
                    (id, attempts, channel, created_at, message, recipient, sent_at, status, tenant_id)
                values (?, 1, 'EMAIL', ?, 'legacy notification', 'legacy@example.invalid', ?, 'SENT', ?)
                """, notificationId, Timestamp.from(createdAt), Timestamp.from(sentAt), tenantId);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        Map<String, Object> row = jdbc.queryForMap("""
                select status, sent_at, delivery_status, provider_accepted_at, delivered_at
                from notifications where id = ?
                """, notificationId);
        assertThat(row.get("status")).isEqualTo("SENT");
        assertThat(((Timestamp) row.get("sent_at")).toInstant()).isEqualTo(sentAt);
        assertThat(row.get("delivery_status")).isNull();
        assertThat(row.get("provider_accepted_at")).isNull();
        assertThat(row.get("delivered_at")).isNull();
        assertThat(jdbc.queryForObject("""
                select count(*) from notifications
                where status = 'SENDING' and provider_accepted_at < now() - interval '30 minutes'
                """, Long.class)).isZero();
    }
}
