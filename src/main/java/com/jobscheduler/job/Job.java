package com.jobscheduler.job;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
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

    @Column(name = "locked_by")
    private String lockedBy;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    private String cron;

    @Column(name = "parent_job_id")
    private UUID parentJobId;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(name = "request_hash")
    private String requestHash;

    /** Workers update jobs with plain SQL that also bumps this, so JPA notices concurrent changes. */
    @Version
    private Long version;

    protected Job() {
        // for JPA
    }

    public static Job create(String tenantId, String type, Map<String, Object> payload,
                             Instant runAt, int maxAttempts, Instant now) {
        return create(tenantId, type, payload, runAt, maxAttempts, null, now);
    }

    public static Job create(String tenantId, String type, Map<String, Object> payload,
                             Instant runAt, int maxAttempts, String cron, Instant now) {
        Job job = new Job();
        job.id = UUID.randomUUID();
        job.tenantId = tenantId;
        job.type = type;
        job.payload = payload == null ? new HashMap<>() : new HashMap<>(payload);
        job.status = JobStatus.PENDING;
        job.runAt = runAt == null ? now : runAt;
        job.maxAttempts = maxAttempts;
        job.cron = cron;
        job.createdAt = now;
        job.updatedAt = now;
        return job;
    }

    public void assignIdempotencyKey(String key, String hash) {
        this.idempotencyKey = key;
        this.requestHash = hash;
    }

    /** Due now: hand it to the queue. Only valid for a job that has not been queued yet. */
    public void markQueued(Instant now) {
        if (status != JobStatus.PENDING) {
            throw new IllegalJobStateException(id, status, JobStatus.QUEUED);
        }
        status = JobStatus.QUEUED;
        updatedAt = now;
    }

    public boolean isDue(Instant now) {
        return !runAt.isAfter(now);
    }

    /** Give a dead-lettered job a fresh set of attempts, starting now. */
    public void replay(Instant now) {
        if (status != JobStatus.FAILED) {
            throw new IllegalJobStateException(id, status, JobStatus.QUEUED);
        }
        status = JobStatus.QUEUED;
        attempts = 0;
        lastError = null;
        completedAt = null;
        runAt = now;
        updatedAt = now;
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
    public String getLockedBy() { return lockedBy; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public Long getVersion() { return version; }
    public String getCron() { return cron; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getRequestHash() { return requestHash; }
    public UUID getParentJobId() { return parentJobId; }
}
