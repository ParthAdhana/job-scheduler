package com.jobscheduler.queue;

import java.time.Instant;
import java.util.UUID;

/** Published to the dead-letter topic when a job runs out of attempts. */
public record DeadLetter(UUID messageId, UUID jobId, String tenantId, String type, int attempts,
                         String lastError, Instant failedAt) {
}
