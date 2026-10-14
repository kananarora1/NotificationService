package com.example.notification.metrics;

import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central place for the application's custom business metrics. Counters and timers
 * are created lazily by the registry and reused across calls.
 *
 * <p>All per-tenant metrics are tagged with the tenant <em>name</em> (resolved from
 * the id and cached) so dashboards read nicely, e.g. the "429 by tenant" panel.
 */
@Component
public class NotificationMetrics {

    private final MeterRegistry registry;
    private final TenantRepository tenants;
    private final ConcurrentHashMap<UUID, String> tenantNameCache = new ConcurrentHashMap<>();
    private final Counter reclaimed;

    public NotificationMetrics(MeterRegistry registry, TenantRepository tenants) {
        this.registry = registry;
        this.tenants = tenants;
        // Registered up front, so the series exists at 0 from startup. A counter created
        // on first increment is born at its full value, which rate()/increase() read as
        // no change — the blind spot described on DeadLetterGrowing in alerts.yml.
        this.reclaimed = Counter.builder("notifications.reclaimed")
                .description("SENDING rows whose lease expired, returned to PENDING and republished")
                .register(registry);
    }

    /** A notification was accepted and created (not a deduplicated duplicate). */
    public void submitted(String tenant, String channel) {
        registry.counter("notifications.submitted", "tenant", tenant, "channel", channel).increment();
    }

    /** A notification was successfully delivered. */
    public void sent(String tenant, String channel) {
        registry.counter("notifications.sent", "tenant", tenant, "channel", channel).increment();
    }

    /** A single delivery attempt failed (fires on every failure, including retries). */
    public void failedAttempt(String tenant) {
        registry.counter("notifications.failed_attempt", "tenant", tenant).increment();
    }

    /** A notification was dead-lettered (retries exhausted). */
    public void dead(String tenant) {
        registry.counter("notifications.dead", "tenant", tenant).increment();
    }

    /** The reconciler returned stranded SENDING rows to PENDING and republished them. */
    public void reclaimed(int count) {
        reclaimed.increment(count);
    }

    /** A request was rejected by the rate limiter (429). */
    public void rateLimitRejected(String tenant) {
        registry.counter("ratelimit.rejected", "tenant", tenant).increment();
    }

    /** Duration of a single provider.send() call. */
    public void recordDeliveryLatency(Duration duration) {
        registry.timer("notifications.delivery.latency").record(duration);
    }

    /** Wall-clock time from the notification being accepted (createdAt) to sent (sentAt). */
    public void recordEndToEndLatency(Duration duration) {
        registry.timer("notifications.end_to_end.latency").record(duration);
    }

    /** Resolves a tenant id to its name (cached), for tagging delivery-side metrics. */
    public String tenantName(UUID tenantId) {
        if (tenantId == null) {
            return "unknown";
        }
        return tenantNameCache.computeIfAbsent(tenantId,
                id -> tenants.findById(id).map(Tenant::getName).orElse(id.toString()));
    }
}
