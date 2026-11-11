package com.jobscheduler.job;

import java.util.UUID;

/** A job the poller just moved to QUEUED. {@code attempts} is how many it has made so far. */
public record DueJob(UUID id, String tenantId, String type, int attempts) {
}
