package com.example.notification.repository;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    /**
     * Atomically claims a notification for delivery (PENDING -> SENDING) in a single
     * conditional UPDATE, taking a lease: the caller's {@code token} and the database's
     * {@code now()} are stamped on the row, and the attempt counter is incremented in the
     * same statement. Returns 1 if this caller won the claim, 0 if the row is missing,
     * already claimed by another consumer, or already terminal.
     *
     * <p>The precondition lives in the WHERE clause deliberately: reading the status
     * first and then updating is the time-of-check/time-of-use race this replaces.
     * Runs in its own short transaction — the provider call must NOT be inside it.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'SENDING', claim_token = :token, claimed_at = now(), attempts = attempts + 1
             where id = :id and status = 'PENDING'
            """, nativeQuery = true)
    int claimForSending(@Param("id") UUID id, @Param("token") UUID token);

    /**
     * Records a successful delivery (SENDING -> SENT), but only if the caller still holds
     * the lease. Returns 0 if the lease was reclaimed in the meantime: the row now belongs
     * to whoever claimed it next, and this caller must not overwrite it.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'SENT', sent_at = :sentAt, claim_token = null, claimed_at = null
             where id = :id and status = 'SENDING' and claim_token = :token
            """, nativeQuery = true)
    int markSent(@Param("id") UUID id, @Param("token") UUID token, @Param("sentAt") Instant sentAt);

    /**
     * Releases a claim (SENDING -> PENDING) after a failed delivery attempt, so the
     * listener's in-process retry can re-claim it. Guarded by the lease token, so it can
     * never clobber a row that has since reached SENT or DEAD, nor release a lease that
     * the reconciler has handed to another consumer. Returns 0 in either case.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'PENDING', claim_token = null, claimed_at = null
             where id = :id and status = 'SENDING' and claim_token = :token
            """, nativeQuery = true)
    int releaseClaim(@Param("id") UUID id, @Param("token") UUID token);

    /**
     * Records a dead-letter (PENDING -> DEAD). Only PENDING qualifies: the last failed
     * attempt released its claim before the message was dead-lettered, so a row in any
     * other status means a different delivery has since claimed it (SENDING) or finished
     * it (SENT), and a late dead-letter must not overwrite that. Returns 0 if so.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'DEAD', failure_reason = :reason
             where id = :id and status = 'PENDING'
            """, nativeQuery = true)
    int markDead(@Param("id") UUID id, @Param("reason") String reason);

    /**
     * Locks up to {@code limit} SENDING rows whose lease has expired — or that have no
     * lease at all, which only rows stranded before V5 can lack. {@code SKIP LOCKED}
     * lets several instances sweep at once without blocking on, or double-reclaiming,
     * each other's rows. Must run inside the caller's transaction: the locks are what
     * make the follow-up {@link #reclaim} safe, and they end with the transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(value = """
            select id from notifications
             where status = 'SENDING'
               and (claimed_at is null or claimed_at < now() - :leaseMillis * interval '1 millisecond')
             order by claimed_at nulls first
             limit :limit
             for update skip locked
            """, nativeQuery = true)
    List<UUID> lockExpiredLeases(@Param("leaseMillis") long leaseMillis, @Param("limit") int limit);

    /** Returns locked, expired SENDING rows to PENDING and clears their lease. */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'PENDING', claim_token = null, claimed_at = null
             where id in (:ids) and status = 'SENDING'
            """, nativeQuery = true)
    int reclaim(@Param("ids") Collection<UUID> ids);

    /** Same predicate as {@link #lockExpiredLeases}, counted, for the stuck_sending gauge. */
    @Query(value = """
            select count(*) from notifications
             where status = 'SENDING'
               and (claimed_at is null or claimed_at < now() - :leaseMillis * interval '1 millisecond')
            """, nativeQuery = true)
    long countExpiredLeases(@Param("leaseMillis") long leaseMillis);

    Optional<Notification> findByTenantIdAndIdempotencyKey(UUID tenantId, String idempotencyKey);

    long countByStatus(NotificationStatus status);

    List<Notification> findByTenantIdOrderByCreatedAtDesc(UUID tenantId, Pageable pageable);

    List<Notification> findByTenantIdAndStatusOrderByCreatedAtDesc(
            UUID tenantId, NotificationStatus status, Pageable pageable);
}
