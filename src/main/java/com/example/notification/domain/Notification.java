package com.example.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
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
     * Nullable, but UNIQUE: at most one row may exist per key. Postgres allows
     * multiple NULLs, so key-less notifications never collide.
     */
    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationStatus status;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column
    private Instant sentAt;

    /** Number of delivery attempts made so far (incremented at the start of each attempt). */
    @Column(nullable = false)
    private int attempts;

    /** Populated when the notification is dead-lettered; null otherwise. */
    @Column
    private String failureReason;

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

    public void markSent(Instant sentAt) {
        this.status = NotificationStatus.SENT;
        this.sentAt = sentAt;
    }

    public void markFailed() {
        this.status = NotificationStatus.FAILED;
    }

    /** Marks the notification permanently failed (retries exhausted, dead-lettered). */
    public void markDead(String failureReason) {
        this.status = NotificationStatus.DEAD;
        this.failureReason = failureReason;
    }

    /** Records that another delivery attempt is starting and returns the new count. */
    public int incrementAttempts() {
        return ++this.attempts;
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

    public int getAttempts() {
        return attempts;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }
}
