package com.jobscheduler.worker;

import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class RetryIntegrationTest extends IntegrationTest {

    @AfterEach
    void resetDownstream() {
        switchableHandler.setFailing(true);
    }

    @Test
    void flakyJobSucceedsAfterRetries() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "flaky", "payload": {"failTimes": 2}, "maxAttempts": 3}
                """);

        awaitStatus(id, "SUCCEEDED");
        assertThat(attemptOutcomes(id)).containsExactly("FAILED", "FAILED", "SUCCEEDED");
    }

    @Test
    void jobThatKeepsFailingIsDeadLettered() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "always-fail", "maxAttempts": 3}
                """);

        awaitStatus(id, "FAILED");
        assertThat(fetch(tenant, id).get("attempts").asInt()).isEqualTo(3);
        assertThat(attemptOutcomes(id)).containsExactly("FAILED", "FAILED", "FAILED");
        assertThat(deadLetters.contains(r -> id.toString().equals(r.key()), Duration.ofSeconds(10))).isTrue();
    }

    @Test
    void failedJobCanBeReplayedOnceDownstreamRecovers() throws Exception {
        String tenant = uniqueTenant();
        UUID id = submit(tenant, """
                {"type": "switchable", "maxAttempts": 1}
                """);
        awaitStatus(id, "FAILED");

        switchableHandler.setFailing(false);
        mvc.perform(post("/jobs/{id}/replay", id).header("X-Tenant-Id", tenant))
                .andExpect(status().isOk());

        awaitStatus(id, "SUCCEEDED");
        assertThat(attemptOutcomes(id)).containsExactly("FAILED", "SUCCEEDED");
    }

    @Test
    void unparseableMessageGoesToDeadLetterTopic() throws Exception {
        String marker = "not-json-" + UUID.randomUUID();
        kafka().send(com.jobscheduler.queue.Topics.READY, marker, "{this is not json " + marker).get();

        assertThat(deadLetters.contains(r -> marker.equals(r.key()), Duration.ofSeconds(15))).isTrue();
    }

    @Test
    void delayedJobWaitsUntilItsRunAt() throws Exception {
        Instant runAt = Instant.now().plusSeconds(2);
        UUID id = submit(uniqueTenant(), """
                {"type": "counting", "runAt": "%s"}
                """.formatted(runAt));

        Thread.sleep(1000);
        assertThat(statusOf(id)).isEqualTo("PENDING");

        awaitStatus(id, "SUCCEEDED");
        Instant startedAt = jdbc.sql("SELECT started_at FROM job_attempts WHERE job_id = :id")
                .param("id", id).query(java.sql.Timestamp.class).single().toInstant();
        assertThat(startedAt).isAfterOrEqualTo(runAt);
    }

    @Test
    void jobFromCrashedWorkerIsReclaimedAndRetried() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "counting", "maxAttempts": 3, "runAt": "2099-01-01T00:00:00Z"}
                """);
        // Simulate a worker that claimed attempt 1 and then died without finishing.
        jdbc.sql("""
                UPDATE jobs SET status = 'RUNNING', attempts = 1, locked_by = 'dead-worker',
                    lease_expires_at = now() - interval '1 second'
                WHERE id = :id
                """).param("id", id).update();
        jdbc.sql("""
                INSERT INTO job_attempts (job_id, attempt_number, worker_id, outcome, started_at)
                VALUES (:id, 1, 'dead-worker', 'RUNNING', now())
                """).param("id", id).update();

        awaitStatus(id, "SUCCEEDED");
        assertThat(attemptOutcomes(id)).containsExactly("FAILED", "SUCCEEDED");
        assertThat(countingHandler.runs(id)).isEqualTo(1);
    }

    @Test
    void recurringJobSchedulesItsNextOccurrence() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "counting", "cron": "0 0 0 1 1 *", "runAt": "%s"}
                """.formatted(Instant.now()));

        awaitStatus(id, "SUCCEEDED");
        var next = jdbc.sql("SELECT status, run_at FROM jobs WHERE parent_job_id = :id")
                .param("id", id)
                .query((rs, n) -> new Object[]{rs.getString("status"), rs.getTimestamp("run_at").toInstant()})
                .single();
        assertThat(next[0]).isEqualTo("PENDING");
        assertThat(((Instant) next[1]).toString()).endsWith("-01-01T00:00:00Z");
    }

    @Test
    void rejectsInvalidCron() throws Exception {
        mvc.perform(post("/jobs").contentType("application/json").content("""
                        {"type": "noop", "cron": "every tuesday"}
                        """))
                .andExpect(status().isBadRequest());
    }

    private org.springframework.kafka.core.KafkaTemplate<String, String> kafka() {
        return kafkaTemplate;
    }

    @org.springframework.beans.factory.annotation.Autowired
    org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate;
}
