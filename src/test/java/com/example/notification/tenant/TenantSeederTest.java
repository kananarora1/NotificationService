package com.example.notification.tenant;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TenantSeederTest {

    private static final TenantSeedProperties.SeedTenant ACME =
            new TenantSeedProperties.SeedTenant("acme", "key-acme", 60);

    @Test
    void ignoresOnlyARaceThatCreatedTheSameApiKey() {
        TenantRepository tenants = mock(TenantRepository.class);
        String hash = ApiKeyHasher.sha256Hex(ACME.apiKey());
        when(tenants.findByApiKeyHash(hash))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(new Tenant("other", "other-key", 1)));
        when(tenants.save(any(Tenant.class))).thenThrow(new DataIntegrityViolationException("duplicate key"));

        assertThatCode(() -> seeder(tenants).run(null)).doesNotThrowAnyException();
    }

    @Test
    void exposesAnUnrelatedConstraintFailure() {
        TenantRepository tenants = mock(TenantRepository.class);
        when(tenants.findByApiKeyHash(anyString())).thenReturn(Optional.empty());
        DataIntegrityViolationException expected = new DataIntegrityViolationException("tenant name already exists");
        when(tenants.save(any(Tenant.class))).thenThrow(expected);

        assertThatThrownBy(() -> seeder(tenants).run(null)).isSameAs(expected);
    }

    private static TenantSeeder seeder(TenantRepository tenants) {
        return new TenantSeeder(tenants, new TenantSeedProperties(true, List.of(ACME)));
    }
}
