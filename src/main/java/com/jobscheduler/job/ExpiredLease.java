package com.jobscheduler.job;

import java.util.UUID;

/** A RUNNING job the reaper took back because its worker stopped renewing the lease. */
public record ExpiredLease(UUID id, String tenantId, String type, JobStatus newStatus, int attempts,
                           String cron) {
}
