-- Indexes for the tenant-scoped list queries added when the read endpoints were
-- scoped to the tenant (commit 92c3cc2). They are missing from the baseline because
-- the baseline is a snapshot of what already existed, so they go in their own
-- migration: V1 never runs on an already-populated database, V2 runs everywhere.

-- Serves NotificationRepository.findByTenantIdOrderByCreatedAtDesc.
-- DESC matches the query's ordering; Postgres can also scan it backwards, but
-- spelling it out keeps the intent visible.
CREATE INDEX idx_notifications_tenant_id_created_at
    ON notifications USING btree (tenant_id, created_at DESC);

-- Serves NotificationRepository.findByTenantIdAndStatusOrderByCreatedAtDesc.
CREATE INDEX idx_notifications_tenant_id_status_created_at
    ON notifications USING btree (tenant_id, status, created_at DESC);

-- findByTenantIdAndIdempotencyKey is already served by the btree behind
-- uk_notifications_tenant_idempotency_key, so it needs no index of its own.

-- Note: plain CREATE INDEX takes a write lock for the duration. Fine at this table's
-- size; a large table would want CREATE INDEX CONCURRENTLY in a migration marked
-- with "-- flyway:executeInTransaction=false".
