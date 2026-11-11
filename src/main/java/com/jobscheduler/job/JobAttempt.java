package com.jobscheduler.job;

import java.time.Instant;

public record JobAttempt(int attemptNumber, String workerId, String outcome, String error,
                         Instant startedAt, Instant finishedAt) {
}
