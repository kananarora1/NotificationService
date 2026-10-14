-- Record when the broker confirmed a notification's message, so a row whose publish
-- failed is no longer indistinguishable from one that is queued.
--
-- published_at is set only after RabbitMQ acks the publish (publisher confirms) and the
-- message was routed to notifications.send. NULL on a PENDING row means no message for
-- it is known to be on the broker: the publish on create failed or was never confirmed,
-- or the row was just reclaimed from an expired SENDING lease. LeaseReconciler
-- republishes those rows. The column is only consulted while a row is PENDING.

ALTER TABLE notifications ADD COLUMN published_at timestamp(6) with time zone;

-- Serves LeaseReconciler's republish scan and the notifications.unpublished_pending
-- gauge. Partial, so it holds only rows still waiting for a confirmed publish — normally
-- none — rather than every row in the table.
CREATE INDEX idx_notifications_unpublished_pending
    ON notifications USING btree (created_at)
    WHERE status = 'PENDING' AND published_at IS NULL;

-- No backfill. Existing PENDING rows keep published_at NULL because nothing records
-- whether their message reached the broker, so the first sweep after this deploy
-- republishes each once. That is safe — delivery is idempotent on the id, so a second
-- copy of a message still in the queue is skipped at the claim — and it also rescues
-- any row stranded by a failed publish before this migration.
