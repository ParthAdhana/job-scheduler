package com.jobscheduler.job;

import com.jobscheduler.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobStoreIntegrationTest extends IntegrationTest {

    @Autowired
    JobStore store;

    @Test
    void onlyOneWorkerCanClaimAnAttempt() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "noop", "runAt": "2099-01-01T00:00:00Z"}
                """);
        jdbc.sql("UPDATE jobs SET status = 'QUEUED' WHERE id = :id").param("id", id).update();
        Instant now = Instant.now();

        var first = store.claim(id, 1, "worker-a", now, now.plusSeconds(30));
        var second = store.claim(id, 1, "worker-b", now, now.plusSeconds(30));

        assertThat(first).isPresent();
        assertThat(second).isEmpty();
        assertThat(statusOf(id)).isEqualTo("RUNNING");
    }

    @Test
    void workerThatLostItsLeaseCannotRecordSuccess() throws Exception {
        UUID id = submit(uniqueTenant(), """
                {"type": "noop", "runAt": "2099-01-01T00:00:00Z"}
                """);
        jdbc.sql("UPDATE jobs SET status = 'QUEUED' WHERE id = :id").param("id", id).update();
        Instant now = Instant.now();
        ClaimedJob job = store.claim(id, 1, "worker-a", now, now.plusSeconds(30)).orElseThrow();

        // Someone else took over the job (e.g. the reaper after the lease expired).
        jdbc.sql("UPDATE jobs SET locked_by = 'worker-b' WHERE id = :id").param("id", id).update();

        assertThat(store.markSucceeded(job, "worker-a", now)).isFalse();
        assertThat(statusOf(id)).isEqualTo("RUNNING");
    }
}
