-- Contract half of the expand/contract pair begun in V3: now that api_key_hash is
-- populated, unique and in use by the auth path, the plaintext column goes.
--
-- Deliberately a separate migration from V3 so the backfill is verifiable while the
-- original values still exist. Once this has run the plaintext keys are unrecoverable —
-- which is the point, but it also means a rollback past V3 invalidates every key and
-- requires re-issuing them.

-- Drops the column's UNIQUE constraint (uk3en99tewvbc4g3a7ou4mt1ono) along with it.
ALTER TABLE tenants DROP COLUMN api_key;
