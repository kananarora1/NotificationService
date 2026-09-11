package com.example.notification.repository;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    /**
     * Atomically claims a notification for delivery (PENDING -> SENDING) in a single
     * conditional UPDATE. Returns 1 if this caller won the claim, 0 if the row is
     * missing, already claimed by another consumer, or already terminal.
     *
     * <p>The precondition lives in the WHERE clause deliberately: reading the status
     * first and then updating is the time-of-check/time-of-use race this replaces.
     * Runs in its own short transaction — the provider call must NOT be inside it.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("update Notification n set n.status = com.example.notification.domain.NotificationStatus.SENDING "
            + "where n.id = :id and n.status = com.example.notification.domain.NotificationStatus.PENDING")
    int claimForSending(@Param("id") UUID id);

    /**
     * Releases a claim (SENDING -> PENDING) after a failed delivery attempt, so the
     * listener's in-process retry can re-claim it. Conditional on SENDING so it can
     * never clobber a row that has since reached SENT or DEAD.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("update Notification n set n.status = com.example.notification.domain.NotificationStatus.PENDING "
            + "where n.id = :id and n.status = com.example.notification.domain.NotificationStatus.SENDING")
    int releaseClaim(@Param("id") UUID id);

    Optional<Notification> findByTenantIdAndIdempotencyKey(UUID tenantId, String idempotencyKey);

    long countByStatus(NotificationStatus status);

    List<Notification> findByTenantIdOrderByCreatedAtDesc(UUID tenantId, Pageable pageable);

    List<Notification> findByTenantIdAndStatusOrderByCreatedAtDesc(
            UUID tenantId, NotificationStatus status, Pageable pageable);
}
