package com.jobscheduler.api;

import com.jobscheduler.job.Job;
import com.jobscheduler.job.JobStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String type,
        Map<String, Object> payload,
        JobStatus status,
        Instant runAt,
        int attempts,
        int maxAttempts,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getType(), job.getPayload(), job.getStatus(),
                job.getRunAt(), job.getAttempts(), job.getMaxAttempts(), job.getLastError(),
                job.getCreatedAt(), job.getUpdatedAt(), job.getCompletedAt());
    }
}
