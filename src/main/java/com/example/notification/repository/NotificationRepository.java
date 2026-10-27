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
     * {@code now()} are stamped on the row (as the lease start and as
     * {@code last_attempt_at}), and the attempt counter is incremented in the same statement. Returns 1 if this caller won the claim, 0 if the row is missing,
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
               set status = 'SENDING', claim_token = :token, claimed_at = now(), last_attempt_at = now(),
                   attempts = attempts + 1
             where id = :id and status = 'PENDING'
            """, nativeQuery = true)
    int claimForSending(@Param("id") UUID id, @Param("token") UUID token);

    /**
     * Records a simulated delivery (SENDING -> SENT), but only if the caller still holds
     * the lease. Returns 0 if the lease was reclaimed in the meantime: the row now belongs
     * to whoever claimed it next, and this caller must not overwrite it.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'SENT', sent_at = :sentAt, delivery_status = 'DELIVERED',
                   delivered_at = :sentAt,
                   claim_token = null, claimed_at = null
             where id = :id and status = 'SENDING' and claim_token = :token
            """, nativeQuery = true)
    int markSent(@Param("id") UUID id, @Param("token") UUID token, @Param("sentAt") Instant sentAt);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set delivery_status = coalesce(delivery_status, 'ACCEPTED'),
                   provider_accepted_at = now(), claim_token = null, claimed_at = null
             where id = :id and status = 'SENDING' and claim_token = :token
            """, nativeQuery = true)
    int markProviderAccepted(@Param("id") UUID id, @Param("token") UUID token);

    /**
     * Inserts a provider callback exactly once. Brevo's event id is globally unique for that
     * provider, so a replay or a webhook retry does not alter notification state twice.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            insert into notification_delivery_events
                (provider, provider_event_id, notification_id, event_type, occurred_at, reason)
            values
                ('BREVO', :eventId, :notificationId, :eventType, :occurredAt, :reason)
            on conflict (provider, provider_event_id) do nothing
            """, nativeQuery = true)
    int recordBrevoEvent(@Param("eventId") long eventId,
                         @Param("notificationId") UUID notificationId,
                         @Param("eventType") String eventType,
                         @Param("occurredAt") Instant occurredAt,
                         @Param("reason") String reason);

    /**
     * Stores the latest relevant provider outcome. Callbacks can arrive out of order, so an
     * older event is kept in the audit table but cannot overwrite the newer visible state.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set delivery_status = cast(:deliveryStatus as varchar),
                   provider_event_at = :occurredAt
             where id = :notificationId
               and status in ('SENDING', 'SENT', 'DEAD')
               and (provider_event_at is null or provider_event_at <= :occurredAt)
            """, nativeQuery = true)
    int updateDeliveryStatus(@Param("notificationId") UUID notificationId,
                             @Param("deliveryStatus") String deliveryStatus,
                             @Param("occurredAt") Instant occurredAt);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'SENT', sent_at = :occurredAt, delivered_at = :occurredAt,
                   failure_reason = null, claim_token = null, claimed_at = null
             where id = :notificationId and status in ('SENDING', 'DEAD')
               and delivery_status = 'DELIVERED' and provider_event_at = :occurredAt
            """, nativeQuery = true)
    int markProviderDelivered(@Param("notificationId") UUID notificationId,
                              @Param("occurredAt") Instant occurredAt);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'DEAD', sent_at = null, failure_reason = :reason,
                   claim_token = null, claimed_at = null
             where id = :notificationId and status in ('SENDING', 'SENT')
               and delivery_status = cast(:deliveryStatus as varchar)
               and provider_event_at = :occurredAt
            """, nativeQuery = true)
    int markProviderDead(@Param("notificationId") UUID notificationId,
                         @Param("deliveryStatus") String deliveryStatus,
                         @Param("occurredAt") Instant occurredAt,
                         @Param("reason") String reason);

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
     * lease at all, which only rows stranded before V5 can lack. Accepted rows wait for
     * a webhook and are not reclaimed. {@code SKIP LOCKED}
     * lets several instances sweep at once without blocking on, or double-reclaiming,
     * each other's rows. Must run inside the caller's transaction: the locks are what
     * make the follow-up {@link #reclaim} safe, and they end with the transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(value = """
            select id from notifications
             where status = 'SENDING'
               and provider_accepted_at is null
               and (claimed_at is null or claimed_at < now() - :leaseMillis * interval '1 millisecond')
             order by claimed_at nulls first
             limit :limit
             for update skip locked
            """, nativeQuery = true)
    List<UUID> lockExpiredLeases(@Param("leaseMillis") long leaseMillis, @Param("limit") int limit);

    /**
     * Returns locked, expired SENDING rows to PENDING and clears their lease. Also clears
     * published_at: the dead consumer's message is gone, so the row now needs a new one,
     * which the reconciler's republish pass provides.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'PENDING', claim_token = null, claimed_at = null, published_at = null
             where id in (:ids) and status = 'SENDING' and provider_accepted_at is null
            """, nativeQuery = true)
    int reclaim(@Param("ids") Collection<UUID> ids);

    /**
     * Locks up to {@code limit} PENDING rows that have used up their retries
     * ({@code attempts >= maxAttempts}) and whose last attempt began more than
     * {@code graceMillis} ago — rows whose dead-letter never got DEAD written. A row with
     * no recorded attempt time (attempted before V8) is aged by {@code created_at}.
     * {@code SKIP LOCKED}, like the lease sweep; must run inside the caller's transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(value = """
            select id from notifications
             where status = 'PENDING' and attempts >= :maxAttempts
               and coalesce(last_attempt_at, created_at) < now() - :graceMillis * interval '1 millisecond'
             order by coalesce(last_attempt_at, created_at)
             limit :limit
             for update skip locked
            """, nativeQuery = true)
    List<UUID> lockExhaustedPending(@Param("maxAttempts") int maxAttempts,
                                    @Param("graceMillis") long graceMillis,
                                    @Param("limit") int limit);

    /**
     * Marks locked, retry-exhausted rows DEAD. Guarded like {@link #markDead}: only a row
     * still PENDING changes, so this and the dead-letter handler never both record one.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'DEAD', failure_reason = :reason
             where id in (:ids) and status = 'PENDING'
            """, nativeQuery = true)
    int markExhaustedDead(@Param("ids") Collection<UUID> ids, @Param("reason") String reason);

    /**
     * Gives a DEAD or retry-exhausted PENDING row a fresh start: PENDING, attempts and
     * failure reason cleared, and published_at cleared so that, if the caller's publish
     * is not confirmed, the reconciler republishes it. SENDING and SENT rows are left
     * alone (in flight, or already delivered); returns 0 for them.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            update notifications
               set status = 'PENDING', attempts = 0, failure_reason = null,
                   claim_token = null, claimed_at = null, published_at = null, last_attempt_at = null,
                   sent_at = null, delivery_status = null, provider_event_at = null,
                   provider_accepted_at = null, delivered_at = null
             where id = :id and status in ('PENDING', 'DEAD')
            """, nativeQuery = true)
    int resetForRedelivery(@Param("id") UUID id);

    /**
     * Records that the broker confirmed this row's message. Only fills an empty value, so
     * a late or repeated confirm never moves it.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = "update notifications set published_at = now() where id = :id and published_at is null",
            nativeQuery = true)
    int markPublished(@Param("id") UUID id);

    /**
     * PENDING rows with no confirmed message, older than {@code graceMillis} — the
     * reconciler's republish candidates. The grace keeps it off a row whose create() is
     * still publishing. Unlocked: each candidate is locked and re-checked individually.
     */
    @Query(value = """
            select id from notifications
             where status = 'PENDING' and published_at is null
               and created_at < now() - :graceMillis * interval '1 millisecond'
             order by created_at
             limit :limit
            """, nativeQuery = true)
    List<UUID> findUnpublishedPending(@Param("graceMillis") long graceMillis, @Param("limit") int limit);

    /**
     * Locks one republish candidate if it still needs it. Empty if it has since been
     * published, claimed or finished — or if another instance holds it (SKIP LOCKED), so
     * two instances never republish the same row at once.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(value = """
            select id from notifications
             where id = :id and status = 'PENDING' and published_at is null
             for update skip locked
            """, nativeQuery = true)
    List<UUID> lockUnpublishedPending(@Param("id") UUID id);

    /** Same predicate as {@link #findUnpublishedPending}, counted, for the unpublished_pending gauge. */
    @Query(value = """
            select count(*) from notifications
             where status = 'PENDING' and published_at is null
               and created_at < now() - :graceMillis * interval '1 millisecond'
            """, nativeQuery = true)
    long countUnpublishedPending(@Param("graceMillis") long graceMillis);

    /** Same predicate as {@link #lockExpiredLeases}, counted, for the stuck_sending gauge. */
    @Query(value = """
            select count(*) from notifications
             where status = 'SENDING'
               and provider_accepted_at is null
               and (claimed_at is null or claimed_at < now() - :leaseMillis * interval '1 millisecond')
            """, nativeQuery = true)
    long countExpiredLeases(@Param("leaseMillis") long leaseMillis);

    @Query(value = """
            select count(*) from notifications
             where status = 'SENDING' and provider_accepted_at is not null
            """, nativeQuery = true)
    long countAwaitingConfirmation();

    Optional<Notification> findByTenantIdAndIdempotencyKey(UUID tenantId, String idempotencyKey);

    long countByStatus(NotificationStatus status);

    List<Notification> findByTenantIdOrderByCreatedAtDesc(UUID tenantId, Pageable pageable);

    List<Notification> findByTenantIdAndStatusOrderByCreatedAtDesc(
            UUID tenantId, NotificationStatus status, Pageable pageable);
}
