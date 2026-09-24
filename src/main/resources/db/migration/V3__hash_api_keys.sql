-- Store a SHA-256 hash of each API key instead of the key itself, so a database dump
-- no longer hands over working credentials. Expand half of an expand/contract pair:
-- this migration adds and backfills the hash but leaves the plaintext column in place,
-- so the backfill can be verified before V4 destroys the original.

ALTER TABLE tenants ADD COLUMN api_key_hash varchar(64);

-- Backfill in pure SQL. Postgres has had a built-in sha256(bytea) since 11, so this
-- needs no pgcrypto extension and no Java-based migration — and it produces exactly
-- the same lowercase hex digest as ApiKeyHasher.sha256Hex, which is what the auth path
-- computes at request time. A Java migration or a startup backfill would have to be
-- guarded to run once and could not run inside this transaction; SQL is simply cleaner.
-- On a fresh database this updates zero rows: tenants are seeded after Flyway runs.
UPDATE tenants
   SET api_key_hash = encode(sha256(convert_to(api_key, 'UTF8')), 'hex')
 WHERE api_key_hash IS NULL;

ALTER TABLE tenants ALTER COLUMN api_key_hash SET NOT NULL;

-- The hot-path lookup: every authenticated request resolves its tenant through this
-- index (TenantRepository.findByApiKeyHash).
CREATE UNIQUE INDEX uk_tenants_api_key_hash ON tenants USING btree (api_key_hash);

-- The application no longer writes api_key, so it must stop being mandatory before the
-- new code inserts a tenant. Without this, seeding a new tenant between V3 and V4 would
-- fail the NOT NULL — which is also what would break a rolling deploy where old and new
-- instances run side by side.
ALTER TABLE tenants ALTER COLUMN api_key DROP NOT NULL;
