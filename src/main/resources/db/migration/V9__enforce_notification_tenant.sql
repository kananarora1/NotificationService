-- Every notification is created through an authenticated tenant API key. Leaving
-- tenant_id nullable permits orphaned rows that cannot be listed, authorized or
-- attributed in metrics; without a foreign key it also permits ids for deleted or
-- nonexistent tenants. Validate the existing data before enforcing the invariant so
-- an upgrade fails with an actionable count instead of an opaque ALTER TABLE error.
DO $$
DECLARE
    invalid_rows bigint;
BEGIN
    SELECT count(*)
      INTO invalid_rows
      FROM notifications n
      LEFT JOIN tenants t ON t.id = n.tenant_id
     WHERE n.tenant_id IS NULL OR t.id IS NULL;

    IF invalid_rows > 0 THEN
        RAISE EXCEPTION
            'V9 aborted: % notification(s) have no valid tenant. Assign each to a tenant or remove it before retrying.',
            invalid_rows;
    END IF;
END
$$;

ALTER TABLE notifications
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE notifications
    ADD CONSTRAINT fk_notifications_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT;
