package com.jobscheduler.worker;

import com.jobscheduler.queue.JobMessage;
import com.jobscheduler.queue.Topics;
import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WorkerIntegrationTest extends IntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Test
    void submittedJobRunsToCompletion() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "counting"}
                """);

        awaitStatus(id, "SUCCEEDED");
        assertThat(countingHandler.runs(id)).isEqualTo(1);
        assertThat(fetch(tenant, id).get("attempts").asInt()).isEqualTo(1);
        assertThat(attemptOutcomes(id)).containsExactly("SUCCEEDED");
    }

    @Test
    void failingJobWithOneAttemptEndsFailed() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "always-fail", "maxAttempts": 1}
                """);

        awaitStatus(id, "FAILED");
        assertThat(fetch(tenant, id).get("lastError").asText()).contains("always fails");
        assertThat(attemptOutcomes(id)).containsExactly("FAILED");
    }

    @Test
    void duplicateMessageDoesNotRunJobTwice() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "counting"}
                """);
        awaitStatus(id, "SUCCEEDED");

        // Redeliver the same logical message, as Kafka may after a consumer rebalance.
        var duplicate = new JobMessage(UUID.randomUUID(), id, "any", "counting", 1);
        kafka.send(Topics.READY, id.toString(), objectMapper.writeValueAsString(duplicate)).get();

        // Give the worker time to (not) act on it.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .until(() -> countingHandler.runs(id) == 1);
        assertThat(statusOf(id)).isEqualTo("SUCCEEDED");
    }
}
