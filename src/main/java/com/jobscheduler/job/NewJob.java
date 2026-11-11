package com.jobscheduler.job;

import java.time.Instant;
import java.util.Map;

/** What a client asks for. Nullable fields fall back to defaults in {@link JobService}. */
public record NewJob(String type, Map<String, Object> payload, Instant runAt, Integer maxAttempts, String cron,
                     JobPriority priority) {
}
