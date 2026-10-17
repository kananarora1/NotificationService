-- Record when a notification's latest delivery attempt began, so the reconciler can tell
-- a row that has used up its retries and was never recorded DEAD from one whose
-- dead-letter is still on its way.
--
-- After the last failed attempt the row is PENDING with attempts at the retry limit, and
-- the dead-letter handler is about to write DEAD. If that write fails (Postgres down),
-- nothing else ever would: the row has a confirmed message, so the republish pass skips
-- it. LeaseReconciler now marks such rows DEAD once last_attempt_at is older than its
-- dead grace. claimed_at cannot serve: it is cleared when an attempt releases its claim.
--
-- Written with the database clock by the claim (claimForSending), like claimed_at.

ALTER TABLE notifications ADD COLUMN last_attempt_at timestamp(6) with time zone;

-- No index: the sweep reads PENDING rows only, a small set already served by
-- idx_notifications_status. No backfill either: the reconciler reads a NULL as
-- created_at, so a row stranded this way before this migration is marked DEAD by the
-- first sweep after deploy.
