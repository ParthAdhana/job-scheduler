package com.jobscheduler.job;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class Job {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(nullable = false)
    private String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> payload = new HashMap<>();

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(name = "run_at", nullable = false)
    private Instant runAt;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    protected Job() {
        // for JPA
    }

    public static Job create(String tenantId, String type, Map<String, Object> payload,
                             Instant runAt, int maxAttempts, Instant now) {
        Job job = new Job();
        job.id = UUID.randomUUID();
        job.tenantId = tenantId;
        job.type = type;
        job.payload = payload == null ? new HashMap<>() : new HashMap<>(payload);
        job.status = JobStatus.PENDING;
        job.runAt = runAt == null ? now : runAt;
        job.maxAttempts = maxAttempts;
        job.createdAt = now;
        job.updatedAt = now;
        return job;
    }

    public void cancel(Instant now) {
        if (!status.isCancellable()) {
            throw new IllegalJobStateException(id, status, JobStatus.CANCELLED);
        }
        status = JobStatus.CANCELLED;
        completedAt = now;
        updatedAt = now;
    }

    public UUID getId() { return id; }
    public String getTenantId() { return tenantId; }
    public String getType() { return type; }
    public Map<String, Object> getPayload() { return payload; }
    public JobStatus getStatus() { return status; }
    public Instant getRunAt() { return runAt; }
    public int getAttempts() { return attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public String getLastError() { return lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Instant getCompletedAt() { return completedAt; }
}
