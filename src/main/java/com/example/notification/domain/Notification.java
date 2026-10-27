package com.example.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "notifications",
        indexes = {
                // Serves countByStatus(PENDING), polled every 5s by MetricsPoller.
                @Index(name = "idx_notifications_status", columnList = "status"),
                // Serves findByOrderByCreatedAtDesc.
                @Index(name = "idx_notifications_created_at", columnList = "created_at"),
                // Serves findByStatusOrderByCreatedAtDesc.
                @Index(name = "idx_notifications_status_created_at", columnList = "status, created_at")
        },
        // Idempotency is scoped to the tenant: two tenants may reuse the same key
        // without colliding (and without one seeing the other's notification).
        uniqueConstraints = @UniqueConstraint(
                name = "uk_notifications_tenant_idempotency_key",
                columnNames = {"tenant_id", "idempotency_key"})
)
public class Notification {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String recipient;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Channel channel;

    @Column(nullable = false)
    private String message;

    /**
     * Caller-supplied idempotency key (from the {@code Idempotency-Key} header).
     * Nullable, and UNIQUE <em>per tenant</em> (see the table's uniqueConstraints):
     * at most one row may exist per (tenant, key). Postgres treats NULLs as distinct
     * in a unique constraint, so key-less notifications never collide.
     */
    @Column(name = "idempotency_key")
    private String idempotencyKey;

    /** The tenant (resolved from the API key) that created this notification. */
    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationStatus status;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column
    private Instant sentAt;

    /** Latest outcome reported by the delivery provider; distinct from the internal queue status. */
    @Enumerated(EnumType.STRING)
    @Column(name = "delivery_status")
    private DeliveryStatus deliveryStatus;

    /** Timestamp reported by the provider for the latest delivery event. */
    @Column(name = "provider_event_at")
    private Instant providerEventAt;

    @Column(name = "provider_accepted_at")
    private Instant providerAcceptedAt;

    /** Timestamp of the provider's delivered event, not proof of inbox placement. */
    @Column(name = "delivered_at")
    private Instant deliveredAt;

    /** Number of delivery attempts made so far (incremented at the start of each attempt). */
    @Column(nullable = false)
    private int attempts;

    /** Populated when the notification is dead-lettered; null otherwise. */
    @Column
    private String failureReason;

    /**
     * Identifies the consumer holding the SENDING lease; null while awaiting a webhook.
     * Read-only here: only the guarded UPDATEs in {@code NotificationRepository} write
     * it, so a stray {@code save()} can never resurrect or clobber a lease.
     */
    @Column(name = "claim_token", insertable = false, updatable = false)
    private UUID claimToken;

    /** When the current worker lease began; null while awaiting a webhook. */
    @Column(name = "claimed_at", insertable = false, updatable = false)
    private Instant claimedAt;

    /**
     * When the broker confirmed this row's message (publisher confirms). Null while no
     * message for it is known to be on the broker — the reconciler republishes PENDING
     * rows in that state. Written only by guarded UPDATEs, like the lease columns.
     */
    @Column(name = "published_at", insertable = false, updatable = false)
    private Instant publishedAt;

    /** When the latest delivery attempt claimed the row (database clock); null if never attempted. */
    @Column(name = "last_attempt_at", insertable = false, updatable = false)
    private Instant lastAttemptAt;

    protected Notification() {
        // for JPA
    }

    public Notification(String recipient, Channel channel, String message) {
        this.id = UUID.randomUUID();
        this.recipient = recipient;
        this.channel = channel;
        this.message = message;
        this.status = NotificationStatus.PENDING;
        this.createdAt = Instant.now();
        this.attempts = 0;
    }

    public UUID getId() {
        return id;
    }

    public String getRecipient() {
        return recipient;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getMessage() {
        return message;
    }

    public NotificationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public DeliveryStatus getDeliveryStatus() {
        return deliveryStatus;
    }

    public Instant getProviderEventAt() {
        return providerEventAt;
    }

    public Instant getProviderAcceptedAt() {
        return providerAcceptedAt;
    }

    public Instant getDeliveredAt() {
        return deliveredAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public UUID getClaimToken() {
        return claimToken;
    }

    public Instant getClaimedAt() {
        return claimedAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public void setTenantId(UUID tenantId) {
        this.tenantId = tenantId;
    }
}
