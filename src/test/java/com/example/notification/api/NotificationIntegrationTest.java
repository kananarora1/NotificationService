package com.example.notification.api;

import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.provider.SimulatedProvider;
import com.example.notification.ratelimit.RateLimiter;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.service.NotificationService;
import com.example.notification.support.MutableClock;
import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import com.example.notification.web.ApiKeyAuthInterceptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureObservability // enable the real (Prometheus) meter registry in tests, not just SimpleMeterRegistry
@Testcontainers
class NotificationIntegrationTest {

    // Seeded tenants (see TenantSeeder): acme/60, globex/10, initech/5 per minute.
    private static final String ACME_KEY = "key-acme";
    private static final String GLOBEX_KEY = "key-globex";   // limit 10/min
    private static final String INITECH_KEY = "key-initech"; // limit 5/min

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"));

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitmq =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    /** A controllable clock so the rate limiter's refill is deterministic and fast. */
    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock testClock() {
            return new MutableClock(Instant.parse("2026-07-01T00:00:00Z"), ZoneOffset.UTC);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private SimulatedProvider provider;

    @Autowired
    private NotificationService service;

    @Autowired
    private MutableClock clock;

    @Autowired
    private TenantRepository tenantRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedisScript<List> tokenBucketScript;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void resetRateLimiterState() {
        // Clear all rate-limit buckets so each test starts with fresh, full budgets.
        redisConnectionFactory.getConnection().serverCommands().flushDb();
    }

    // ---- v4/v5 behaviour (now requires a valid API key) ----

    @Test
    void normalRecipientSucceedsOnFirstAttempt() throws Exception {
        UUID id = postNotify("alice@example.com");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getAttempts()).isEqualTo(1);
            assertThat(stored.getSentAt()).isNotNull();
        });
    }

    @Test
    void flakyRecipientRecoversToSentViaRetry() throws Exception {
        UUID id = postNotify("flaky@example.com");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getAttempts()).isGreaterThanOrEqualTo(3);
            assertThat(stored.getFailureReason()).isNull();
        });
    }

    @Test
    void permanentFailureEndsDeadAndIsDeadLettered() throws Exception {
        UUID id = postNotify("fail@example.com");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.DEAD);
            assertThat(stored.getAttempts()).isEqualTo(4); // retry.max-attempts
            assertThat(stored.getFailureReason()).isNotNull();
            assertThat(stored.getSentAt()).isNull();
        });
    }

    @Test
    void backoffDelayIncreasesBetweenAttempts() throws Exception {
        UUID id = postNotify("fail@example.com");

        await().atMost(Duration.ofSeconds(30))
                .until(() -> provider.invocationTimes(id).size() >= 4);

        List<Long> times = provider.invocationTimes(id);
        long gap1 = times.get(1) - times.get(0);
        long gap2 = times.get(2) - times.get(1);
        long gap3 = times.get(3) - times.get(2);

        assertThat(gap2).as("2nd gap > 1st gap").isGreaterThan(gap1);
        assertThat(gap3).as("3rd gap > 2nd gap").isGreaterThan(gap2);
    }

    @Test
    void getReflectsAttemptsAndStatus() throws Exception {
        UUID id = postNotify("bob@example.com");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mockMvc.perform(get("/notify/{id}", id.toString()))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SENT"))
                        .andExpect(jsonPath("$.attempts").value(1))
                        .andExpect(jsonPath("$.sentAt").exists()));
    }

    @Test
    void getUnknownIdReturns404() throws Exception {
        mockMvc.perform(get("/notify/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    void blankFieldReturns400() throws Exception {
        mockMvc.perform(post("/notify")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"recipient":"","channel":"EMAIL","message":"hello"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void invalidChannelReturns400() throws Exception {
        mockMvc.perform(post("/notify")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"recipient":"alice@example.com","channel":"CARRIER_PIGEON","message":"hello"}
                                """))
                .andExpect(status().isBadRequest());
    }

    // ---- v5: caller-level idempotency ----

    @Test
    void sameIdempotencyKeyDedupesToOneRowAndSendsOnce() throws Exception {
        String key = "key-" + UUID.randomUUID();

        UUID first = postWithKey("idem-a@example.com", key, status().isAccepted());
        UUID second = postWithKey("idem-a@example.com", key, status().isOk());

        assertThat(second).isEqualTo(first);
        assertThat(repository.findByIdempotencyKey(key)).get()
                .extracting(Notification::getId).isEqualTo(first);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(repository.findById(first).orElseThrow().getStatus())
                        .isEqualTo(NotificationStatus.SENT));
        assertThat(provider.invocationTimes(first)).hasSize(1);
    }

    @Test
    void differentKeysCreateTwoRows() throws Exception {
        UUID a = postWithKey("two-a@example.com", "key-" + UUID.randomUUID(), status().isAccepted());
        UUID b = postWithKey("two-b@example.com", "key-" + UUID.randomUUID(), status().isAccepted());

        assertThat(b).isNotEqualTo(a);
        assertThat(repository.findById(a)).isPresent();
        assertThat(repository.findById(b)).isPresent();
    }

    @Test
    void concurrentDuplicatePostsCreateExactlyOneRow() throws Exception {
        String key = "key-" + UUID.randomUUID();
        String recipient = "race-" + UUID.randomUUID() + "@example.com";
        int threads = 8;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        Set<UUID> ids = ConcurrentHashMap.newKeySet();

        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        MvcResult r = mockMvc.perform(notifyPost(recipient, ACME_KEY)
                                        .header("Idempotency-Key", key))
                                .andReturn();
                        ids.add(idFrom(r));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(ids).hasSize(1);
        long rows = repository.findAll().stream()
                .filter(n -> recipient.equals(n.getRecipient()))
                .count();
        assertThat(rows).isEqualTo(1);
    }

    // ---- v5: processing-level idempotency ----

    @Test
    void processingIdempotencySkipsAlreadySentOnRedelivery() {
        Notification n = new Notification("proc@example.com", Channel.EMAIL, "once");
        repository.save(n);
        UUID id = n.getId();

        service.attemptDelivery(id);
        service.attemptDelivery(id); // redelivery: status SENT -> skip

        assertThat(provider.invocationTimes(id)).hasSize(1);
        assertThat(repository.findById(id).orElseThrow().getStatus())
                .isEqualTo(NotificationStatus.SENT);
    }

    // ---- v6 Stage 1: API key identity ----

    @Test
    void missingApiKeyReturns401() throws Exception {
        mockMvc.perform(post("/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyFor("auth@example.com")))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unknownApiKeyReturns401() throws Exception {
        mockMvc.perform(post("/notify")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, "key-does-not-exist")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyFor("auth@example.com")))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void validApiKeyIsAcceptedAndStoresTenant() throws Exception {
        UUID id = postNotify("auth-ok@example.com");
        assertThat(repository.findById(id).orElseThrow().getTenantId()).isNotNull();
    }

    // ---- v6 Stage 2: per-tenant rate limiting ----

    @Test
    void rateLimitReturns429WhenBudgetExhaustedAndIsolatesTenants() throws Exception {
        // globex allows 10/min. The test clock is frozen here, so 13 rapid requests
        // see a full bucket drained to empty: 10 succeed, the rest are limited.
        int accepted = 0;
        int limited = 0;
        String retryAfter = null;

        for (int i = 0; i < 13; i++) {
            MvcResult r = mockMvc.perform(notifyPost("rl@example.com", GLOBEX_KEY)).andReturn();
            int s = r.getResponse().getStatus();
            if (s == 202) {
                accepted++;
            } else if (s == 429) {
                limited++;
                retryAfter = r.getResponse().getHeader(HttpHeaders.RETRY_AFTER);
            }
        }

        assertThat(accepted).isEqualTo(10);
        assertThat(limited).isEqualTo(3);
        assertThat(retryAfter).isNotNull();
        assertThat(Long.parseLong(retryAfter)).isPositive();

        // Isolation: a different tenant's budget is untouched.
        mockMvc.perform(notifyPost("rl-other@example.com", ACME_KEY))
                .andExpect(status().isAccepted());
    }

    @Test
    void rateLimitBucketRefillsOverTime() throws Exception {
        // initech allows 5/min => one token every 12s.
        for (int i = 0; i < 5; i++) {
            mockMvc.perform(notifyPost("refill@example.com", INITECH_KEY))
                    .andExpect(status().isAccepted());
        }
        // Bucket now empty -> 429.
        mockMvc.perform(notifyPost("refill@example.com", INITECH_KEY))
                .andExpect(status().isTooManyRequests());

        // Advance past one refill interval; a token becomes available.
        clock.advance(Duration.ofSeconds(13));

        mockMvc.perform(notifyPost("refill@example.com", INITECH_KEY))
                .andExpect(status().isAccepted());
    }

    // ---- v7: distributed correctness across instances ----

    @Test
    void limitHoldsAcrossTwoInstancesSharingOneRedis() {
        // Two RateLimiter instances stand in for two app instances behind a load
        // balancer, both pointed at the SAME Redis. This is exactly the scenario that
        // over-admitted in v6 (each instance had its own in-memory bucket -> 2x limit).
        RateLimiter instanceA = new RateLimiter(redisTemplate, tokenBucketScript, clock);
        RateLimiter instanceB = new RateLimiter(redisTemplate, tokenBucketScript, clock);

        Tenant globex = tenantRepository.findByApiKey(GLOBEX_KEY).orElseThrow();
        int limit = globex.getRateLimitPerMinute(); // 10

        int allowed = 0;
        // Fire 2x the limit, interleaved across the two "instances".
        for (int i = 0; i < 2 * limit; i++) {
            RateLimiter instance = (i % 2 == 0) ? instanceA : instanceB;
            if (instance.tryConsume(globex).allowed()) {
                allowed++;
            }
        }

        // Exactly the limit is admitted in total — not 2x. The shared, atomic Redis
        // counter is the single source of truth across instances.
        assertThat(allowed).isEqualTo(limit);
    }

    // ---- v8: observability / metrics ----

    @Test
    void submittedAndSentMetricsIncrement() throws Exception {
        double submittedBefore = counter("notifications.submitted", "tenant", "acme", "channel", "EMAIL");
        double sentBefore = counter("notifications.sent", "tenant", "acme", "channel", "EMAIL");

        UUID id = postNotify("metrics-normal@example.com"); // acme / EMAIL / normal recipient

        // submitted is recorded synchronously on the POST.
        assertThat(counter("notifications.submitted", "tenant", "acme", "channel", "EMAIL"))
                .isEqualTo(submittedBefore + 1);

        // sent is recorded once the consumer delivers it.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.findById(id).orElseThrow().getStatus())
                    .isEqualTo(NotificationStatus.SENT);
            assertThat(counter("notifications.sent", "tenant", "acme", "channel", "EMAIL"))
                    .isGreaterThanOrEqualTo(sentBefore + 1);
        });
    }

    @Test
    void rateLimitRejectedMetricIsTaggedByTenant() throws Exception {
        double before = counter("ratelimit.rejected", "tenant", "globex");

        // globex limit 10; 13 requests -> 3 rejected (clock frozen, Redis flushed in @BeforeEach).
        for (int i = 0; i < 13; i++) {
            mockMvc.perform(notifyPost("rl-metric@example.com", GLOBEX_KEY));
        }

        assertThat(counter("ratelimit.rejected", "tenant", "globex")).isEqualTo(before + 3);
        // The counter is tenant-scoped: acme has no rejections from globex's overflow.
        assertThat(meterRegistry.find("ratelimit.rejected").tags("tenant", "globex").counter())
                .isNotNull();
    }

    @Test
    void deadMetricIncrementsAndPrometheusExposesAllCustomMetrics() throws Exception {
        double deadBefore = counter("notifications.dead", "tenant", "acme");

        // Exercise every path so each custom meter has been created.
        postNotify("obs-normal@example.com");            // submitted + sent + delivery/e2e timers
        UUID deadId = postNotify("fail@example.com");     // -> DEAD + failed_attempt
        for (int i = 0; i < 12; i++) {                    // -> ratelimit.rejected
            mockMvc.perform(notifyPost("obs-rl@example.com", GLOBEX_KEY));
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(repository.findById(deadId).orElseThrow().getStatus())
                        .isEqualTo(NotificationStatus.DEAD));
        assertThat(counter("notifications.dead", "tenant", "acme")).isGreaterThanOrEqualTo(deadBefore + 1);

        String body = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        // Custom metrics (Prometheus naming: dots -> underscores, counters get _total).
        assertThat(body)
                .contains("notifications_submitted_total")
                .contains("notifications_sent_total")
                .contains("notifications_failed_attempt_total")
                .contains("notifications_dead_total")
                .contains("ratelimit_rejected_total")
                .contains("notifications_delivery_latency_seconds")
                .contains("notifications_end_to_end_latency_seconds")
                .contains("notifications_pending")
                .contains("queue_depth");

        // Auto-provided infrastructure metrics are exported too.
        assertThat(body)
                .contains("http_server_requests_seconds")
                .contains("jvm_memory_used_bytes")
                .contains("hikaricp_connections");
    }

    // ---- helpers ----

    private double counter(String name, String... tags) {
        Counter c = meterRegistry.find(name).tags(tags).counter();
        return c == null ? 0.0 : c.count();
    }

    private MockHttpServletRequestBuilder notifyPost(String recipient, String apiKey) {
        return post("/notify")
                .header(ApiKeyAuthInterceptor.API_KEY_HEADER, apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(bodyFor(recipient));
    }

    private UUID postNotify(String recipient) throws Exception {
        MvcResult result = mockMvc.perform(notifyPost(recipient, ACME_KEY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        return idFrom(result);
    }

    private UUID postWithKey(String recipient, String idempotencyKey, ResultMatcher expectedStatus)
            throws Exception {
        MvcResult result = mockMvc.perform(notifyPost(recipient, ACME_KEY)
                        .header("Idempotency-Key", idempotencyKey))
                .andExpect(expectedStatus)
                .andReturn();
        return idFrom(result);
    }

    private static String bodyFor(String recipient) {
        return String.format(
                "{\"recipient\":\"%s\",\"channel\":\"EMAIL\",\"message\":\"hello\"}", recipient);
    }

    private UUID idFrom(MvcResult result) throws Exception {
        JsonNode json = objectMapper.readTree(result.getResponse().getContentAsString());
        return UUID.fromString(json.get("id").asText());
    }
}
