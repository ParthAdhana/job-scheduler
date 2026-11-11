package com.jobscheduler.idempotency;

import com.jobscheduler.job.JobStore;
import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class IdempotencyIntegrationTest extends IntegrationTest {

    private static final String BODY = """
            {"type": "noop", "payload": {"orderId": 42}, "runAt": "2099-01-01T00:00:00Z"}
            """;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JobStore store;

    @Test
    void repeatedRequestReturnsTheOriginalJob() throws Exception {
        String tenant = uniqueTenant();
        String key = UUID.randomUUID().toString();

        String first = post(tenant, key, BODY).andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String second = post(tenant, key, BODY).andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andReturn().getResponse().getContentAsString();

        assertThat(id(second)).isEqualTo(id(first));
        assertThat(jobCount(tenant)).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentBodyIsRejected() throws Exception {
        String tenant = uniqueTenant();
        String key = UUID.randomUUID().toString();
        post(tenant, key, BODY).andExpect(status().isCreated());

        post(tenant, key, BODY.replace("42", "43")).andExpect(status().isConflict());
    }

    @Test
    void keysAreScopedPerTenant() throws Exception {
        String key = UUID.randomUUID().toString();
        String a = post(uniqueTenant(), key, BODY).andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String b = post(uniqueTenant(), key, BODY).andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        assertThat(id(a)).isNotEqualTo(id(b));
    }

    @Test
    void postgresStillDeduplicatesWhenRedisLostTheKey() throws Exception {
        String tenant = uniqueTenant();
        String key = UUID.randomUUID().toString();
        post(tenant, key, BODY).andExpect(status().isCreated());

        redis.delete("idempotency:" + tenant + ":" + key);

        post(tenant, key, BODY).andExpect(status().isOk());
        assertThat(jobCount(tenant)).isEqualTo(1);
    }

    @Test
    void concurrentDuplicatesCreateExactlyOneJob() throws Exception {
        String tenant = uniqueTenant();
        String key = UUID.randomUUID().toString();
        int threads = 10;
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(threads);
        Set<String> ids = ConcurrentHashMap.newKeySet();
        List<Future<Integer>> statuses = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                statuses.add(pool.submit((Callable<Integer>) () -> {
                    start.await();
                    var response = post(tenant, key, BODY).andReturn().getResponse();
                    ids.add(id(response.getContentAsString()));
                    return response.getStatus();
                }));
            }
            start.countDown();
            List<Integer> codes = new ArrayList<>();
            for (Future<Integer> f : statuses) {
                codes.add(f.get());
            }

            assertThat(codes).containsOnly(200, 201);
            assertThat(codes).filteredOn(c -> c == 201).hasSize(1);
            assertThat(ids).hasSize(1);
            assertThat(jobCount(tenant)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void messageIdIsOnlyProcessedOnce() {
        UUID messageId = UUID.randomUUID();
        Boolean first = new org.springframework.transaction.support.TransactionTemplate(txManager)
                .execute(s -> store.markMessageProcessed(messageId, Instant.now()));
        Boolean second = new org.springframework.transaction.support.TransactionTemplate(txManager)
                .execute(s -> store.markMessageProcessed(messageId, Instant.now()));

        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }

    @Autowired
    org.springframework.transaction.PlatformTransactionManager txManager;

    private ResultActions post(String tenant, String key, String body) throws Exception {
        return mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/jobs")
                .header("X-Tenant-Id", tenant)
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private String id(String json) throws Exception {
        return objectMapper.readTree(json).get("id").asText();
    }

    private int jobCount(String tenant) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE tenant_id = :t").param("t", tenant)
                .query(Integer.class).single();
    }
}
