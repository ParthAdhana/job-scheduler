package com.jobscheduler.job;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    void newJobIsPendingAndRunsNowWhenNoRunAtGiven() {
        Job job = Job.create("t1", "send-email", Map.of("to", "a@b.com"), null, 3, NOW);

        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getRunAt()).isEqualTo(NOW);
        assertThat(job.getAttempts()).isZero();
    }

    @Test
    void pendingJobCanBeCancelled() {
        Job job = Job.create("t1", "send-email", Map.of(), null, 3, NOW);

        job.cancel(NOW.plusSeconds(5));

        assertThat(job.getStatus()).isEqualTo(JobStatus.CANCELLED);
        assertThat(job.getCompletedAt()).isEqualTo(NOW.plusSeconds(5));
    }

    @Test
    void cancelledJobCannotBeCancelledAgain() {
        Job job = Job.create("t1", "send-email", Map.of(), null, 3, NOW);
        job.cancel(NOW);

        assertThatThrownBy(() -> job.cancel(NOW)).isInstanceOf(IllegalJobStateException.class);
    }
}
