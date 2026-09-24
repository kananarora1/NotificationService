package com.example.notification.tenant;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TenantRepository extends JpaRepository<Tenant, UUID> {

    /**
     * Resolves a tenant by the SHA-256 hash of its API key — the hot-path lookup, run
     * on every authenticated request and served by the unique index on
     * {@code api_key_hash}. Callers hash the incoming plaintext with
     * {@link ApiKeyHasher#sha256Hex(String)}; the plaintext never reaches the query.
     */
    Optional<Tenant> findByApiKeyHash(String apiKeyHash);
}
