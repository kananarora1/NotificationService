package com.example.notification.api;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.notification.domain.Channel;
import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.messaging.NotificationPublisher;
import com.example.notification.messaging.RabbitConfig;
import com.example.notification.provider.NotificationProvider;
import com.example.notification.provider.SimulatedProvider;
import com.example.notification.ratelimit.RateLimiter;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.service.NotificationService;
import com.example.notification.support.MutableClock;
import com.example.notification.tenant.ApiKeyHasher;
import com.example.notification.tenant.Tenant;
import com.example.notification.tenant.TenantRepository;
import com.example.notification.web.ApiKeyAuthInterceptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.transaction.support.TransactionTemplate;
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

import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
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

        /** Wraps the real provider with the test instrumentation (see RecordingProvider). */
        @Bean
        @Primary
        RecordingProvider recordingProvider(SimulatedProvider delegate) {
            return new RecordingProvider(delegate);
        }
    }

    /**
     * Test-side instrumentation around {@link SimulatedProvider}, so the production class
     * carries none of it. Records every send() call per notification (the backoff test
     * reads the timestamps), and can make the next send of one id <em>deliver and then
     * hang</em> until released — a provider that accepted the notification but is slow to
     * answer, so the send outlives its lease.
     */
    static class RecordingProvider implements NotificationProvider {

        private final SimulatedProvider delegate;
        private final Map<UUID, List<Long>> invocationTimes = new ConcurrentHashMap<>();
        private final Map<UUID, Hold> holds = new ConcurrentHashMap<>();

        private record Hold(CountDownLatch reached, CountDownLatch release) {
        }

        RecordingProvider(SimulatedProvider delegate) {
            this.delegate = delegate;
        }

        /** Returns a latch that opens once that send has delivered and is now hanging. */
        CountDownLatch holdAfterDelivering(UUID id, CountDownLatch release) {
            CountDownLatch reached = new CountDownLatch(1);
            holds.put(id, new Hold(reached, release));
            return reached;
        }

        /** Snapshot of the send() call timestamps for one notification. */
        List<Long> invocationTimes(UUID id) {
            List<Long> times = invocationTimes.getOrDefault(id, List.of());
            synchronized (times) {
                return List.copyOf(times);
            }
        }

        @Override
        public void send(Notification notification) {
            invocationTimes
                    .computeIfAbsent(notification.getId(), k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(System.currentTimeMillis());
            Hold hold = holds.remove(notification.getId());
            delegate.send(notification);
            if (hold != null) {
                hold.reached().countDown();
                try {
                    hold.release().await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private RecordingProvider provider;

    /** Real service; a test can make one call fail unexpectedly to exercise the 500 path. */
    @SpyBean
    private NotificationService service;

    @Autowired
    private NotificationPublisher publisher;

    @Autowired
    private TransactionTemplate transactionTemplate;

    /** Real template; a test can make one send fail the way it does while the broker is down. */
    @SpyBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

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

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
            assertThat(stored.getPublishedAt()).as("broker confirmed the publish").isNotNull();
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
                mockMvc.perform(get("/notify/{id}", id.toString())
                                .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SENT"))
                        .andExpect(jsonPath("$.attempts").value(1))
                        .andExpect(jsonPath("$.sentAt").exists()));
    }

    @Test
    void getUnknownIdReturns404() throws Exception {
        mockMvc.perform(get("/notify/{id}", UUID.randomUUID())
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
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
        assertThat(repository.findByTenantIdAndIdempotencyKey(tenantId(ACME_KEY), key)).get()
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

    // ---- per-tenant idempotency: the key is scoped to the tenant, not global ----

    @Test
    void sameIdempotencyKeyFromTwoTenantsCreatesSeparateRows() throws Exception {
        String key = "shared-" + UUID.randomUUID();

        MvcResult acmeResult = mockMvc.perform(notifyPost("tenant-a@example.com", ACME_KEY)
                        .header("Idempotency-Key", key))
                .andExpect(status().isAccepted())
                .andReturn();
        // Previously this returned 200 with acme's id — a cross-tenant leak.
        MvcResult globexResult = mockMvc.perform(notifyPost("tenant-b@example.com", GLOBEX_KEY)
                        .header("Idempotency-Key", key))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID acmeId = idFrom(acmeResult);
        UUID globexId = idFrom(globexResult);
        assertThat(globexId).isNotEqualTo(acmeId);

        // Two distinct rows, each owned by its own tenant.
        assertThat(repository.findByTenantIdAndIdempotencyKey(tenantId(ACME_KEY), key))
                .get().extracting(Notification::getId).isEqualTo(acmeId);
        assertThat(repository.findByTenantIdAndIdempotencyKey(tenantId(GLOBEX_KEY), key))
                .get().extracting(Notification::getId).isEqualTo(globexId);
        assertThat(repository.findById(acmeId).orElseThrow().getTenantId())
                .isEqualTo(tenantId(ACME_KEY));
        assertThat(repository.findById(globexId).orElseThrow().getTenantId())
                .isEqualTo(tenantId(GLOBEX_KEY));

        // Dedup still holds *within* a tenant.
        mockMvc.perform(notifyPost("tenant-a@example.com", ACME_KEY)
                        .header("Idempotency-Key", key))
                .andExpect(status().isOk());
    }

    // ---- tenant-scoped reads ----

    @Test
    void unauthenticatedGetByIdReturns401() throws Exception {
        UUID id = postNotify("read-auth@example.com");

        mockMvc.perform(get("/notify/{id}", id.toString()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void unauthenticatedListReturns401() throws Exception {
        mockMvc.perform(get("/notifications"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getAnotherTenantsNotificationReturns404() throws Exception {
        UUID acmeId = postNotify("acme-private@example.com");

        // 404, not 403: the endpoint must not confirm the id exists.
        mockMvc.perform(get("/notify/{id}", acmeId.toString())
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, GLOBEX_KEY))
                .andExpect(status().isNotFound());

        // The owning tenant still reads it.
        mockMvc.perform(get("/notify/{id}", acmeId.toString())
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(acmeId.toString()));
    }

    @Test
    void listReturnsOnlyCallersRows() throws Exception {
        UUID acmeId = postNotify("list-acme@example.com");
        UUID globexId = idFrom(mockMvc.perform(notifyPost("list-globex@example.com", GLOBEX_KEY))
                .andExpect(status().isAccepted())
                .andReturn());

        String body = mockMvc.perform(get("/notifications")
                        .param("limit", "200")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, GLOBEX_KEY))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        List<String> ids = new ArrayList<>();
        List<String> tenants = new ArrayList<>();
        objectMapper.readTree(body).forEach(n -> {
            ids.add(n.get("id").asText());
            tenants.add(n.get("tenant").asText());
        });

        assertThat(ids).contains(globexId.toString());
        assertThat(ids).doesNotContain(acmeId.toString());
        assertThat(tenants).isNotEmpty().containsOnly("globex");
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

    @Test
    void concurrentDeliveryAttemptsSendExactlyOnce() throws Exception {
        // Two consumers racing on the same id — the shape a broker redelivery creates
        // while the original delivery is still in flight. Sequential skipping (see
        // processingIdempotencySkipsAlreadySentOnRedelivery) does not cover this: both
        // threads read PENDING before either writes.
        Notification n = new Notification("race-send@example.com", Channel.EMAIL, "once");
        repository.save(n);
        UUID id = n.getId();

        int threads = 2;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);

        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        service.attemptDelivery(id);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        // Exactly one thread won the PENDING -> SENDING claim and called the provider.
        assertThat(provider.invocationTimes(id)).hasSize(1);
        Notification stored = repository.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
        assertThat(stored.getAttempts()).isEqualTo(1);
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

        Tenant globex = tenantByKey(GLOBEX_KEY);
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
                .contains("notifications_sending")
                .contains("notifications_stuck_sending")
                .contains("notifications_reclaimed_total")
                .contains("notifications_republished_total")
                .contains("notifications_unpublished_pending")
                .contains("queue_depth");

        // Auto-provided infrastructure metrics are exported too.
        assertThat(body)
                .contains("http_server_requests_seconds")
                .contains("jvm_memory_used_bytes")
                .contains("hikaricp_connections");
    }

    // ---- v9: API keys stored as SHA-256 hashes ----

    @Test
    void existingDevKeysStillAuthenticateAfterHashing() throws Exception {
        // The storage format changed; the contract with the client did not. The same
        // keys that worked before V3/V4 still authenticate and resolve the same tenant.
        for (String key : List.of(ACME_KEY, GLOBEX_KEY, INITECH_KEY)) {
            mockMvc.perform(get("/notifications").header(ApiKeyAuthInterceptor.API_KEY_HEADER, key))
                    .andExpect(status().isOk());
        }

        assertThat(tenantByKey(ACME_KEY).getName()).isEqualTo("acme");
        assertThat(tenantByKey(GLOBEX_KEY).getName()).isEqualTo("globex");
        assertThat(tenantByKey(INITECH_KEY).getName()).isEqualTo("initech");
    }

    @Test
    void unknownKeyIsStillRejectedWhenHashed() throws Exception {
        // A key that hashes to nothing on file: the lookup misses, so 401 — the same
        // outcome as before, reached through the hash index.
        mockMvc.perform(notifyPost("alice@example.com", "key-not-issued"))
                .andExpect(status().isUnauthorized());

        assertThat(tenantRepository.findByApiKeyHash(ApiKeyHasher.sha256Hex("key-not-issued")))
                .isEmpty();
    }

    @Test
    void tenantsTableHasNoPlaintextApiKeyColumnAfterV4() {
        List<String> columns = jdbcTemplate.queryForList(
                "select column_name from information_schema.columns "
                        + "where table_schema = 'public' and table_name = 'tenants'",
                String.class);

        assertThat(columns).contains("api_key_hash");
        // V4 dropped it. If this fails, the plaintext column is back.
        assertThat(columns).doesNotContain("api_key");
    }

    @Test
    void storedValueIsTheSha256HashAndNotTheKeyItself() {
        String storedHash = jdbcTemplate.queryForObject(
                "select api_key_hash from tenants where name = 'acme'", String.class);

        // Matches the digest the auth path computes, and Postgres's own sha256() —
        // which is what the V3 backfill used, so both producers agree.
        assertThat(storedHash).isEqualTo(ApiKeyHasher.sha256Hex(ACME_KEY));
        assertThat(storedHash).hasSize(64).matches("[0-9a-f]{64}");
        assertThat(storedHash).isNotEqualTo(ACME_KEY);
    }

    @Test
    void noPlaintextKeyAppearsAnywhereInTheTenantsTable() {
        // Scan every text column of every row, not just the ones we expect — a dump of
        // this table must not contain a usable credential.
        List<java.util.Map<String, Object>> rows = jdbcTemplate.queryForList("select * from tenants");
        assertThat(rows).isNotEmpty();

        for (java.util.Map<String, Object> row : rows) {
            for (Object value : row.values()) {
                String text = String.valueOf(value);
                assertThat(text).doesNotContain(ACME_KEY).doesNotContain(GLOBEX_KEY).doesNotContain(INITECH_KEY);
            }
        }
    }

    @Test
    void hashingIsDeterministicAndKeySpecific() {
        assertThat(ApiKeyHasher.sha256Hex(ACME_KEY)).isEqualTo(ApiKeyHasher.sha256Hex(ACME_KEY));
        assertThat(ApiKeyHasher.sha256Hex(ACME_KEY)).isNotEqualTo(ApiKeyHasher.sha256Hex(GLOBEX_KEY));
    }

    // ---- v10: a stranded SENDING claim recovers (lease + reconciler) ----

    @Test
    void strandedSendingClaimIsRecoveredAndDelivered() {
        double reclaimedBefore = counter("notifications.reclaimed");
        Notification n = new Notification("strand@example.com", Channel.EMAIL, "recover me");
        repository.save(n);
        UUID id = n.getId();

        // A consumer wins the claim and then dies (kill -9, OOM, a DB error after the
        // claim) before it writes SENT. Nothing ever completes or releases this claim.
        claimAndAbandon(id);

        // The broker redelivers the dead consumer's unacked message to a live one.
        publisher.publish(id);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).as("status of the stranded notification")
                    .isEqualTo(NotificationStatus.SENT);
            assertThat(provider.invocationTimes(id)).as("provider calls").hasSize(1);
        });
        assertThat(counter("notifications.reclaimed")).isGreaterThanOrEqualTo(reclaimedBefore + 1);
    }

    @Test
    void staleLeaseHolderCannotOverwriteOrReleaseAfterReclaim() {
        Notification n = new Notification("lease@example.com", Channel.EMAIL, "guarded");
        repository.save(n);
        UUID id = n.getId();
        UUID tokenA = UUID.randomUUID();
        UUID tokenB = UUID.randomUUID();

        // A claims; its lease is reclaimed (the sweep's UPDATE, minus the republish);
        // B claims next.
        assertThat(repository.claimForSending(id, tokenA)).isEqualTo(1);
        transactionTemplate.executeWithoutResult(tx -> repository.reclaim(List.of(id)));
        assertThat(repository.claimForSending(id, tokenB)).isEqualTo(1);

        // A comes back late. Neither of its writes may touch B's lease.
        assertThat(repository.markSent(id, tokenA, Instant.now())).as("stale SENT").isZero();
        assertThat(repository.releaseClaim(id, tokenA)).as("stale release").isZero();
        Notification held = repository.findById(id).orElseThrow();
        assertThat(held.getStatus()).isEqualTo(NotificationStatus.SENDING);
        assertThat(held.getClaimToken()).isEqualTo(tokenB);
        assertThat(held.getAttempts()).isEqualTo(2);

        // The current holder's write still works, and clears the lease.
        assertThat(repository.markSent(id, tokenB, Instant.now())).isEqualTo(1);
        Notification sent = repository.findById(id).orElseThrow();
        assertThat(sent.getStatus()).isEqualTo(NotificationStatus.SENT);
        assertThat(sent.getClaimToken()).isNull();
        assertThat(sent.getClaimedAt()).isNull();
    }

    @Test
    void workerWhoseLeaseWasReclaimedDropsItsLateResultWithoutOverwriting() throws Exception {
        Notification n = new Notification("slow-ack@example.com", Channel.EMAIL, "outlives its lease");
        repository.save(n);
        UUID id = n.getId();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch delivered = provider.holdAfterDelivering(id, release);
        ListAppender<ILoggingEvent> providerLog = captureProviderLog();

        ExecutorService workerA = Executors.newSingleThreadExecutor();
        try {
            // Worker A claims and delivers, then hangs before it can record SENT.
            Future<?> a = workerA.submit(() -> service.attemptDelivery(id));
            assertThat(delivered.await(5, TimeUnit.SECONDS)).as("worker A delivered").isTrue();

            // A's lease runs out while it hangs. The scheduled reconciler reclaims and
            // republishes, and a live consumer (B) claims it and finishes the job.
            expireLease(id);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                Notification stored = repository.findById(id).orElseThrow();
                assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
                assertThat(stored.getAttempts()).isEqualTo(2);
            });
            Instant sentAtByB = repository.findById(id).orElseThrow().getSentAt();

            // A finally resumes. Its SENT write is guarded by its own (stale) token, so it
            // matches no row; it must neither overwrite B's result nor throw (a throw
            // would send this copy round the retry loop and on to the DLQ).
            release.countDown();
            a.get(10, TimeUnit.SECONDS);

            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getSentAt()).as("sentAt written by B, untouched by A").isEqualTo(sentAtByB);
            assertThat(stored.getAttempts()).isEqualTo(2);

            // The double-send contract: called twice, delivered once. The simulator's
            // "sent to" log line is its delivery, so that is what is counted.
            assertThat(provider.invocationTimes(id)).hasSize(2);
            List<String> messages = messages(providerLog);
            assertThat(messages).filteredOn(m -> m.startsWith("sent to slow-ack@example.com")).hasSize(1);
            assertThat(messages).filteredOn(m -> m.startsWith("already delivered " + id)).hasSize(1);
        } finally {
            release.countDown();
            workerA.shutdownNow();
            releaseProviderLog(providerLog);
        }
    }

    @Test
    void redeliveryOfASentNotificationIsStillSkipped() throws Exception {
        UUID id = postNotify("resend@example.com");
        await().atMost(Duration.ofSeconds(10)).until(() ->
                repository.findById(id).orElseThrow().getStatus() == NotificationStatus.SENT);
        Instant sentAt = repository.findById(id).orElseThrow().getSentAt();

        publisher.publish(id); // a broker redelivery arriving after SENT

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Notification stored = repository.findById(id).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(stored.getAttempts()).isEqualTo(1);
            assertThat(stored.getSentAt()).isEqualTo(sentAt);
            assertThat(provider.invocationTimes(id)).hasSize(1);
        });
    }

    @Test
    void lateDeadLetterDoesNotOverwriteASentNotification() {
        Notification n = new Notification("late-dlq@example.com", Channel.EMAIL, "already sent");
        repository.save(n);
        UUID id = n.getId();
        service.attemptDelivery(id);
        double deadBefore = counter("notifications.dead", "tenant", "unknown");

        // A stale copy of this message exhausted its retries and reached the DLQ.
        service.markDead(id, "late dead-letter");

        Notification stored = repository.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(NotificationStatus.SENT);
        assertThat(stored.getFailureReason()).isNull();
        assertThat(counter("notifications.dead", "tenant", "unknown")).isEqualTo(deadBefore);
    }

    // ---- v11: a publish that fails on create is recovered ----

    @Test
    void publishFailureOnCreateIsRecoveredAndDelivered() throws Exception {
        // The broker refuses the next publish, as it does while RabbitMQ is down. Every
        // RabbitTemplate send overload goes through this one.
        doThrow(new AmqpConnectException(new ConnectException("broker down (test)")))
                .doCallRealMethod()
                .when(rabbitTemplate)
                .convertAndSend(anyString(), anyString(), any(Object.class), nullable(CorrelationData.class));
        String key = "b2-" + UUID.randomUUID();

        int firstStatus = statusOf(notifyPost("b2@example.com", ACME_KEY).header("Idempotency-Key", key));

        // The row committed before the publish failed, and no message exists for it.
        Notification row = repository.findByTenantIdAndIdempotencyKey(tenantId(ACME_KEY), key).orElseThrow();
        UUID id = row.getId();
        assertThat(row.getStatus()).isEqualTo(NotificationStatus.PENDING);
        assertThat(row.getAttempts()).as("never claimed by a consumer").isZero();
        assertThat(amqpAdmin.getQueueInfo(RabbitConfig.SEND_QUEUE).getMessageCount()).as("ready messages").isZero();

        // The client retries with the same key and is told the notification is PENDING.
        mockMvc.perform(notifyPost("b2@example.com", ACME_KEY).header("Idempotency-Key", key))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"));

        // Time passes beyond the grace period a just-created row gets before it is swept.
        backdateCreatedAt(id);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(repository.findById(id).orElseThrow().getStatus())
                    .as("status of the notification whose publish failed (first POST answered %d)", firstStatus)
                    .isEqualTo(NotificationStatus.SENT);
            assertThat(provider.invocationTimes(id)).as("provider calls").hasSize(1);
        });
        // Durably accepted despite the failed publish: 202, not 500.
        assertThat(firstStatus).isEqualTo(HttpStatus.ACCEPTED.value());
    }

    @Test
    void republishingAnIdThatIsAlreadyQueuedDeliversItOnce() {
        Notification n = new Notification("requeued@example.com", Channel.EMAIL, "already queued");
        repository.save(n);
        UUID id = n.getId();
        double republishedBefore = counter("notifications.republished");

        withSendQueuePaused(() -> {
            // A message for this id reaches the queue, but its confirm is never recorded
            // (lost, or the app died first), so the row still reads as unpublished.
            assertThat(publisher.publish(id)).as("first copy confirmed").isTrue();
            backdateCreatedAt(id);

            // The scheduled reconciler sees PENDING + published_at null and publishes again.
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> repository.findById(id).orElseThrow().getPublishedAt() != null);
            assertThat(amqpAdmin.getQueueInfo(RabbitConfig.SEND_QUEUE).getMessageCount())
                    .as("both copies queued").isEqualTo(2);
        });

        // Both copies are consumed: one claims and delivers, the other finds it claimed.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(amqpAdmin.getQueueInfo(RabbitConfig.SEND_QUEUE).getMessageCount()).isZero();
        });
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> provider.invocationTimes(id).size() == 1);
        assertThat(counter("notifications.republished")).isGreaterThanOrEqualTo(republishedBefore + 1);
    }

    // ---- v11: FAILED removed ----

    @Test
    void failedStatusIsGoneFromTheEnumAndTheSchema() {
        assertThat(NotificationStatus.values()).containsExactly(
                NotificationStatus.PENDING, NotificationStatus.SENDING, NotificationStatus.SENT, NotificationStatus.DEAD);

        // The CHECK constraint (V7) allows exactly the enum's values.
        String check = jdbcTemplate.queryForObject(
                "select pg_get_constraintdef(oid) from pg_constraint where conname = 'notifications_status_check'",
                String.class);
        List<String> allowed = Pattern.compile("'([A-Z]+)'").matcher(check).results().map(m -> m.group(1)).toList();
        assertThat(allowed).containsExactlyInAnyOrder(
                Arrays.stream(NotificationStatus.values()).map(Enum::name).toArray(String[]::new));

        Notification n = new Notification("check@example.com", Channel.EMAIL, "constraint");
        repository.save(n);
        try {
            assertThatThrownBy(() -> jdbcTemplate.update(
                    "update notifications set status = 'FAILED' where id = ?", n.getId()))
                    .isInstanceOf(DataIntegrityViolationException.class);
            assertThat(repository.findById(n.getId()).orElseThrow().getStatus()).isEqualTo(NotificationStatus.PENDING);
        } finally {
            repository.deleteById(n.getId()); // never published; keep it away from the reconciler
        }
    }

    // ---- v12: error responses ----

    @Test
    void missingAndUnknownApiKeysAreDistinguishedProblems() throws Exception {
        mockMvc.perform(get("/notifications"))
                .andExpect(status().isUnauthorized())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("Missing X-API-Key header."));
        mockMvc.perform(get("/notifications").header(ApiKeyAuthInterceptor.API_KEY_HEADER, "key-not-issued"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.detail").value("Unknown API key."));
    }

    @Test
    void invalidQueryAndPathValuesNameTheParameterAndWhatIsAllowed() throws Exception {
        mockMvc.perform(get("/notifications").param("status", "FAILED")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.errors[0].field").value("status"))
                .andExpect(jsonPath("$.errors[0].message").value(
                        "has invalid value 'FAILED'; expected one of PENDING, SENDING, SENT, DEAD"));
        mockMvc.perform(get("/notifications").param("limit", "abc")
                        .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].message").value("has invalid value 'abc'; expected an integer"));
        mockMvc.perform(get("/notify/{id}", "not-a-uuid").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("id"))
                .andExpect(jsonPath("$.errors[0].message").value("has invalid value 'not-a-uuid'; expected a UUID"));
    }

    @Test
    void invalidBodiesReportEveryFailingFieldAndWriteNothing() throws Exception {
        long rowsBefore = repository.count();

        mockMvc.perform(post("/notify").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"recipient\":\"\",\"channel\":\"EMAIL\",\"message\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.errors.length()").value(2))
                .andExpect(jsonPath("$.errors[0].field").value("message"))
                .andExpect(jsonPath("$.errors[1].field").value("recipient"));

        mockMvc.perform(post("/notify").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"recipient\":\"a@example.com\",\"channel\":\"CARRIER_PIGEON\",\"message\":\"hi\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("channel"))
                .andExpect(jsonPath("$.errors[0].message").value(
                        "has invalid value 'CARRIER_PIGEON'; expected one of EMAIL, SMS, PUSH"));

        mockMvc.perform(post("/notify").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"recipient\":"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.startsWith("Malformed JSON request body")));

        assertThat(repository.count()).as("no row written for an invalid request").isEqualTo(rowsBefore);
    }

    @Test
    void rateLimitedRequestIsAProblemWithRetryAfter() throws Exception {
        for (int i = 0; i < 5; i++) { // initech allows 5/min
            mockMvc.perform(notifyPost("rl-problem@example.com", INITECH_KEY)).andExpect(status().isAccepted());
        }
        mockMvc.perform(notifyPost("rl-problem@example.com", INITECH_KEY))
                .andExpect(status().isTooManyRequests())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(header().exists(HttpHeaders.RETRY_AFTER))
                .andExpect(jsonPath("$.retryAfterSeconds").isNumber())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.startsWith(
                        "Rate limit of 5 requests per minute exceeded.")));
    }

    @Test
    void unexpectedFailureIsAGeneric500WithAnErrorIdAndNoInternals() throws Exception {
        doThrow(new IllegalStateException("connection to db-primary:5432 refused for user admin"))
                .when(service).get(any(), any());
        ListAppender<ILoggingEvent> handlerLog = captureLog(ApiExceptionHandler.class);
        try {
            MvcResult result = mockMvc.perform(get("/notify/{id}", UUID.randomUUID())
                            .header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                    .andExpect(status().isInternalServerError())
                    .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                    .andExpect(jsonPath("$.errorId").isString())
                    .andReturn();
            String body = result.getResponse().getContentAsString();
            assertThat(body).doesNotContain("db-primary", "admin", "IllegalStateException");

            // The errorId the client sees is the one the stack trace was logged under.
            String errorId = objectMapper.readTree(body).get("errorId").asText();
            assertThat(messages(handlerLog)).anyMatch(m -> m.contains(errorId));
        } finally {
            releaseLog(ApiExceptionHandler.class, handlerLog);
        }
    }

    @Test
    void frameworkErrorsUseTheSameProblemFormat() throws Exception {
        mockMvc.perform(get("/nope").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("No endpoint GET /nope."));
        mockMvc.perform(delete("/notifications").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON));
        mockMvc.perform(post("/notify").header(ApiKeyAuthInterceptor.API_KEY_HEADER, ACME_KEY)
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON));
    }

    // ---- helpers ----

    private static ListAppender<ILoggingEvent> captureLog(Class<?> loggerClass) {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(loggerClass)).addAppender(appender);
        return appender;
    }

    private static void releaseLog(Class<?> loggerClass, ListAppender<ILoggingEvent> appender) {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(loggerClass)).detachAppender(appender);
    }

    /** The status a client would see: MockMvc rethrows an unhandled exception instead of answering 500. */
    private int statusOf(MockHttpServletRequestBuilder request) throws Exception {
        try {
            return mockMvc.perform(request).andReturn().getResponse().getStatus();
        } catch (ServletException e) {
            return HttpStatus.INTERNAL_SERVER_ERROR.value();
        }
    }

    /** Stands in for waiting out the reconciler's publish grace: moves created_at 10 minutes back. */
    private void backdateCreatedAt(UUID id) {
        jdbcTemplate.update(
                "update notifications set created_at = created_at - interval '10 minutes' where id = ?", id);
    }

    /** Captures SimulatedProvider's log, where each "sent to …" line is one delivery. */
    private static ListAppender<ILoggingEvent> captureProviderLog() {
        return captureLog(SimulatedProvider.class);
    }

    private static void releaseProviderLog(ListAppender<ILoggingEvent> appender) {
        releaseLog(SimulatedProvider.class, appender);
    }

    /** Listener threads may still be logging; copy under the appender's own lock. */
    private static List<String> messages(ListAppender<ILoggingEvent> appender) {
        synchronized (appender) {
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }
    }

    /** Claims the row the way a consumer does, then never finishes: the crash. */
    private void claimAndAbandon(UUID id) {
        assertThat(repository.claimForSending(id, UUID.randomUUID())).isEqualTo(1);
        // The holder is dead, so nothing renews its lease and it runs out.
        expireLease(id);
    }

    /**
     * Stands in for waiting out the 60s lease: moves the claim 10 minutes into the past.
     * created_at moves with it — a row whose lease expired is at least that old — which
     * also puts it past the publish grace, as a really reclaimed row always is.
     */
    private void expireLease(UUID id) {
        jdbcTemplate.update(
                "update notifications set claimed_at = now() - interval '10 minutes' where id = ?", id);
        backdateCreatedAt(id);
    }

    /** Stops the notifications.send consumers while {@code action} runs, so messages stay queued. */
    private void withSendQueuePaused(Runnable action) {
        MessageListenerContainer container = listenerRegistry.getListenerContainers().stream()
                .filter(c -> c instanceof AbstractMessageListenerContainer listener
                        && Arrays.asList(listener.getQueueNames()).contains(RabbitConfig.SEND_QUEUE))
                .findFirst().orElseThrow();
        container.stop();
        try {
            action.run();
        } finally {
            container.start();
        }
    }

    private UUID tenantId(String apiKey) {
        return tenantByKey(apiKey).getId();
    }

    /** Resolves a tenant the way the auth path does: by the hash, never the plaintext. */
    private Tenant tenantByKey(String apiKey) {
        return tenantRepository.findByApiKeyHash(ApiKeyHasher.sha256Hex(apiKey)).orElseThrow();
    }

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
