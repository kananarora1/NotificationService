-- Narrow the status CHECK constraint to the states the code actually uses:
-- PENDING, SENDING, SENT, DEAD. FAILED was a v3 single-attempt state that nothing has
-- written since v4 introduced retries; DEAD is the terminal-failure state.
--
-- Refuse to run if any row is still FAILED, rather than letting ADD CONSTRAINT fail
-- with a less useful message. Decide what each such row means (DEAD to keep it as a
-- failure, PENDING to retry it) and update it before re-running.
DO $$
DECLARE
    failed_rows bigint;
BEGIN
    SELECT count(*) INTO failed_rows FROM notifications WHERE status = 'FAILED';
    IF failed_rows > 0 THEN
        RAISE EXCEPTION 'V7 aborted: % notification(s) still have status FAILED. Set them to DEAD or PENDING first.', failed_rows;
    END IF;
END
$$;

-- IF EXISTS: a database baselined from an old ddl-auto schema may not have this
-- constraint at all, and either way it ends up with the correct one. This also fixes a
-- pre-0d6d771 database that never got SENDING added to the constraint by hand.
ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_status_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_status_check
    CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'DEAD'));
