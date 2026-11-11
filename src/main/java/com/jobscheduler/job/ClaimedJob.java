package com.jobscheduler.job;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** A job a worker has taken a lease on. {@code attempt} is the attempt now in progress. */
public record ClaimedJob(UUID id, String tenantId, String type, Map<String, Object> payload,
                         int attempt, int maxAttempts, String cron, Instant runAt) {

    public boolean hasAttemptsLeft() {
        return attempt < maxAttempts;
    }
}
