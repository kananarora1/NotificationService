-- Turn the SENDING claim into a lease, so a consumer that dies between claiming a row
-- and writing SENT no longer strands it in SENDING forever.
--
-- claim_token identifies the consumer holding the claim: SENT and the release back to
-- PENDING are both conditional on it, so a holder whose lease was reclaimed cannot
-- overwrite whatever happened after. claimed_at is when the lease began, written with
-- the database clock (now()) so every instance measures expiry against the same clock.
-- Both are non-null exactly while status = 'SENDING'.

ALTER TABLE notifications ADD COLUMN claim_token uuid;
ALTER TABLE notifications ADD COLUMN claimed_at timestamp(6) with time zone;

-- Serves LeaseReconciler's sweep and the notifications.stuck_sending gauge. Partial on
-- SENDING because those are the only rows either one reads, and there are never more
-- of them than consumers in flight — so the index stays a few entries in size however
-- large the table grows, instead of indexing every SENT row too.
CREATE INDEX idx_notifications_sending_claimed_at
    ON notifications USING btree (claimed_at)
    WHERE status = 'SENDING';

-- Rows already stuck in SENDING (stranded before this migration) keep claimed_at NULL.
-- That is deliberate: the reconciler treats a SENDING row with no lease as expired, so
-- the first sweep after this deploy recovers them. No backfill needed.
