package com.jobscheduler.worker;

import com.jobscheduler.queue.Topics;
import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConcurrencyAndPriorityIntegrationTest extends IntegrationTest {

    @Test
    void typeLimitedToOneNeverRunsTwoAtOnce() throws Exception {
        // application-test.yml limits "sleep" to 1 concurrent job.
        String tenant = uniqueTenant();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ids.add(submit(tenant, """
                    {"type": "sleep", "payload": {"durationMs": 300}}
                    """));
        }

        AtomicInteger maxRunning = new AtomicInteger();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(25)).until(() -> {
            int running = jdbc.sql("SELECT count(*) FROM jobs WHERE tenant_id = :t AND status = 'RUNNING'")
                    .param("t", tenant).query(Integer.class).single();
            maxRunning.accumulateAndGet(running, Math::max);
            return ids.stream().allMatch(id -> "SUCCEEDED".equals(statusOf(id)));
        });

        assertThat(maxRunning.get()).isEqualTo(1);
        // Deferral does not spend attempts.
        for (UUID id : ids) {
            assertThat(attemptOutcomes(id)).containsExactly("SUCCEEDED");
        }
    }

    @Test
    void priorityDecidesTheTopic() throws Exception {
        UUID high = submit(uniqueTenant(), """
                {"type": "counting", "priority": "HIGH"}
                """);
        UUID low = submit(uniqueTenant(), """
                {"type": "counting", "priority": "LOW"}
                """);

        awaitStatus(high, "SUCCEEDED");
        awaitStatus(low, "SUCCEEDED");
        assertThat(topicOf(high)).isEqualTo(Topics.READY_HIGH);
        assertThat(topicOf(low)).isEqualTo(Topics.READY_LOW);
    }

    private String topicOf(UUID jobId) {
        return jdbc.sql("SELECT topic FROM outbox WHERE message_key = :k ORDER BY id LIMIT 1")
                .param("k", jobId.toString()).query(String.class).single();
    }
}
