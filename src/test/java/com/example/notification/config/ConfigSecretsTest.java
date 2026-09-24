package com.example.notification.config;

import com.example.notification.tenant.TenantSeedProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the "no secrets in the repo" property of the configuration.
 *
 * <p>These tests read the real {@code application.yml}, {@code .env.example} and
 * {@code .gitignore} from the working tree rather than a fixture — the point is to fail
 * if someone pins a credential back into a committed file.
 *
 * <p>What this proves: every credential is reached through a {@code ${ENV_VAR:default}}
 * placeholder, so the environment always wins and nothing is hardcoded. It does not
 * claim the defaults are absent — local development deliberately falls back to
 * non-secret docker-compose values.
 */
class ConfigSecretsTest {

    private static final Path APPLICATION_YML = Path.of("src/main/resources/application.yml");
    private static final Path ENV_EXAMPLE = Path.of(".env.example");
    private static final Path GITIGNORE = Path.of(".gitignore");

    /** Matches ${VAR} and ${VAR:default}, capturing VAR. */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Z0-9_]+)(?::[^}]*)?}");

    /** Every property that carries a credential or an endpoint an operator must be able to move. */
    @ParameterizedTest
    @ValueSource(strings = {
            "spring.datasource.url",
            "spring.datasource.username",
            "spring.datasource.password",
            "spring.data.redis.host",
            "spring.data.redis.port",
            "spring.data.redis.password",
            "spring.rabbitmq.host",
            "spring.rabbitmq.port",
            "spring.rabbitmq.username",
            "spring.rabbitmq.password",
            "server.port",
            "notifications.seeding.enabled"
    })
    void sensitivePropertiesAreEnvironmentPlaceholders(String property) throws IOException {
        Object value = valueAt(loadYaml(), property);

        assertThat(value)
                .as("%s must be present in application.yml", property)
                .isNotNull();
        assertThat(String.valueOf(value))
                .as("%s must be supplied by the environment, not hardcoded", property)
                .matches(PLACEHOLDER.pattern());
    }

    @Test
    void seededApiKeysAreEnvironmentPlaceholders() throws IOException {
        String yaml = Files.readString(APPLICATION_YML);

        // The seeded keys must be overridable per environment, never pinned in the file.
        assertThat(yaml).contains("${ACME_API_KEY:");
        assertThat(yaml).contains("${GLOBEX_API_KEY:");
        assertThat(yaml).contains("${INITECH_API_KEY:");
    }

    @Test
    void everyEnvironmentVariableUsedByTheAppIsDocumentedInEnvExample() throws IOException {
        String envExample = Files.readString(ENV_EXAMPLE);

        Matcher matcher = PLACEHOLDER.matcher(Files.readString(APPLICATION_YML));
        var referenced = matcher.results()
                .map(r -> r.group(1))
                .collect(Collectors.toCollection(java.util.TreeSet::new));

        assertThat(referenced).isNotEmpty();
        for (String variable : referenced) {
            assertThat(envExample)
                    .as("%s is read by application.yml but missing from .env.example", variable)
                    .containsPattern("(?m)^" + Pattern.quote(variable) + "=");
        }
    }

    @Test
    void dotEnvIsGitignoredButTheExampleIsNot() throws IOException {
        String gitignore = Files.readString(GITIGNORE);

        assertThat(gitignore).containsPattern("(?m)^\\.env$");
        assertThat(gitignore).containsPattern("(?m)^!\\.env\\.example$");
    }

    @Test
    void noRealDotEnvFileIsCommitted() {
        // .env holds real credentials locally; it must never be part of the tree.
        assertThat(Path.of(".env").toFile().exists() && isTracked())
                .as(".env must not be committed")
                .isFalse();
    }

    @Test
    void environmentVariablesOverrideTheSeededApiKeys() {
        // Exercises the exact placeholder expression the yml uses: supplied -> wins,
        // absent -> falls back to the documented development key.
        new ApplicationContextRunner()
                .withUserConfiguration(SeedPropertiesConfig.class)
                .withPropertyValues(
                        "notifications.seeding.enabled=${SEED_TENANTS_ENABLED:true}",
                        "notifications.seeding.tenants[0].name=acme",
                        "notifications.seeding.tenants[0].api-key=${ACME_API_KEY:key-acme}",
                        "notifications.seeding.tenants[0].rate-limit-per-minute=60",
                        "ACME_API_KEY=supplied-by-environment")
                .run(context -> {
                    TenantSeedProperties properties = context.getBean(TenantSeedProperties.class);
                    assertThat(properties.enabled()).isTrue();
                    assertThat(properties.tenants().get(0).apiKey()).isEqualTo("supplied-by-environment");
                });

        new ApplicationContextRunner()
                .withUserConfiguration(SeedPropertiesConfig.class)
                .withPropertyValues(
                        "notifications.seeding.enabled=true",
                        "notifications.seeding.tenants[0].name=acme",
                        "notifications.seeding.tenants[0].api-key=${ACME_API_KEY:key-acme}",
                        "notifications.seeding.tenants[0].rate-limit-per-minute=60")
                .run(context -> assertThat(context.getBean(TenantSeedProperties.class)
                        .tenants().get(0).apiKey()).isEqualTo("key-acme"));
    }

    @EnableConfigurationProperties(TenantSeedProperties.class)
    static class SeedPropertiesConfig {
    }

    // ---- helpers ----

    private static Map<String, Object> loadYaml() throws IOException {
        try (var in = Files.newInputStream(APPLICATION_YML)) {
            return new Yaml().load(in);
        }
    }

    /** Walks a dotted property path through the parsed YAML tree. */
    @SuppressWarnings("unchecked")
    private static Object valueAt(Map<String, Object> yaml, String property) {
        Object current = yaml;
        for (String segment : property.split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = ((Map<String, Object>) map).get(segment);
        }
        return current;
    }

    private static boolean isTracked() {
        try {
            Process process = new ProcessBuilder("git", "ls-files", "--error-unmatch", ".env")
                    .redirectErrorStream(true)
                    .start();
            return process.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
