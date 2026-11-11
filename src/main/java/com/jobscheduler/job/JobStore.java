package com.jobscheduler.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * State transitions used by workers, written as single conditional UPDATEs.
 *
 * <p>Each statement checks the state it expects in its WHERE clause, so two processes racing on
 * the same job cannot both win: the loser's UPDATE matches zero rows. Every statement also bumps
 * {@code version}, which keeps JPA's optimistic locking in the API correct.
 */
@Repository
public class JobStore {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };
    private static final int MAX_ERROR_LENGTH = 2000;

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public JobStore(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Records that a Kafka message was handled. Returns false if it already was, i.e. this is a
     * redelivery. Part of the caller's transaction, so a rollback forgets it too.
     */
    public boolean markMessageProcessed(UUID messageId, Instant now) {
        return jdbc.sql("""
                        INSERT INTO processed_messages (message_id, processed_at) VALUES (:messageId, :now)
                        ON CONFLICT (message_id) DO NOTHING
                        """)
                .param("messageId", messageId)
                .param("now", ts(now))
                .update() == 1;
    }

    /** Housekeeping: dedupe records only need to outlive Kafka's redelivery window. */
    public int deleteProcessedMessagesBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM processed_messages WHERE processed_at < :cutoff")
                .param("cutoff", ts(cutoff)).update();
    }

    public int deletePublishedOutboxBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM outbox WHERE published_at < :cutoff")
                .param("cutoff", ts(cutoff)).update();
    }

    /**
     * QUEUED -> RUNNING. Succeeds only if the job is still queued and has made exactly
     * {@code attempt - 1} attempts, so late or duplicate messages cannot start it again.
     */
    public Optional<ClaimedJob> claim(UUID jobId, int attempt, String workerId, Instant now, Instant leaseUntil) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'RUNNING', attempts = attempts + 1, locked_by = :workerId,
                            lease_expires_at = :leaseUntil, updated_at = :now, version = version + 1
                        WHERE id = :id AND status = 'QUEUED' AND attempts = :previousAttempts
                        RETURNING id, tenant_id, type, payload::text AS payload, attempts, max_attempts, cron
                        """)
                .param("id", jobId)
                .param("previousAttempts", attempt - 1)
                .param("workerId", workerId)
                .param("leaseUntil", ts(leaseUntil))
                .param("now", ts(now))
                .query(this::mapClaimed)
                .optional();
    }

    /** RUNNING -> SUCCEEDED, only if this worker still holds the lease for this attempt. */
    public boolean markSucceeded(ClaimedJob job, String workerId, Instant now) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'SUCCEEDED', completed_at = :now, updated_at = :now,
                            locked_by = NULL, lease_expires_at = NULL, last_error = NULL,
                            version = version + 1
                        WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
                        """)
                .param("id", job.id())
                .param("workerId", workerId)
                .param("attempt", job.attempt())
                .param("now", ts(now))
                .update() == 1;
    }

    /** RUNNING -> FAILED (no attempts left), only if this worker still holds the lease. */
    public boolean markFailed(ClaimedJob job, String workerId, String error, Instant now) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'FAILED', completed_at = :now, updated_at = :now,
                            locked_by = NULL, lease_expires_at = NULL, last_error = :error,
                            version = version + 1
                        WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
                        """)
                .param("id", job.id())
                .param("workerId", workerId)
                .param("attempt", job.attempt())
                .param("error", truncate(error))
                .param("now", ts(now))
                .update() == 1;
    }

    /** RUNNING -> RETRYING: wait until {@code retryAt}, then the poller queues it again. */
    public boolean markRetrying(ClaimedJob job, String workerId, String error, Instant retryAt, Instant now) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'RETRYING', run_at = :retryAt, updated_at = :now,
                            locked_by = NULL, lease_expires_at = NULL, last_error = :error,
                            version = version + 1
                        WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
                        """)
                .param("id", job.id())
                .param("workerId", workerId)
                .param("attempt", job.attempt())
                .param("retryAt", ts(retryAt))
                .param("error", truncate(error))
                .param("now", ts(now))
                .update() == 1;
    }

    /**
     * QUEUED -> PENDING without using up an attempt: the job's type is at its concurrency limit, so
     * try again shortly. The poller will queue it again once {@code retryAt} passes.
     */
    public boolean deferQueued(UUID jobId, int attempt, Instant retryAt, Instant now) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'PENDING', run_at = :retryAt, updated_at = :now, version = version + 1
                        WHERE id = :id AND status = 'QUEUED' AND attempts = :previousAttempts
                        """)
                .param("id", jobId)
                .param("previousAttempts", attempt - 1)
                .param("retryAt", ts(retryAt))
                .param("now", ts(now))
                .update() == 1;
    }

    /** Pushes the lease forward while a long handler is still working. */
    public boolean extendLease(ClaimedJob job, String workerId, Instant leaseUntil) {
        return jdbc.sql("""
                        UPDATE jobs SET lease_expires_at = :leaseUntil
                        WHERE id = :id AND status = 'RUNNING' AND locked_by = :workerId AND attempts = :attempt
                        """)
                .param("id", job.id())
                .param("workerId", workerId)
                .param("attempt", job.attempt())
                .param("leaseUntil", ts(leaseUntil))
                .update() == 1;
    }

    /**
     * PENDING/RETRYING -> QUEUED for jobs whose time has come. {@code SKIP LOCKED} lets several
     * pollers run at once: each grabs a different batch instead of waiting on the others' locks.
     */
    public List<DueJob> queueDueJobs(Instant now, int limit) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'QUEUED', updated_at = :now, version = version + 1
                        WHERE id IN (
                            SELECT id FROM jobs
                            WHERE status IN ('PENDING', 'RETRYING') AND run_at <= :now
                            ORDER BY CASE priority WHEN 'HIGH' THEN 0 WHEN 'NORMAL' THEN 1 ELSE 2 END, run_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        RETURNING id, tenant_id, type, priority, attempts
                        """)
                .param("now", ts(now))
                .param("limit", limit)
                .query((rs, n) -> new DueJob(rs.getObject("id", UUID.class), rs.getString("tenant_id"),
                        rs.getString("type"), JobPriority.valueOf(rs.getString("priority")),
                        rs.getInt("attempts")))
                .list();
    }

    /**
     * Takes back RUNNING jobs whose lease has expired, treating the lost run as a failed attempt:
     * RETRYING if attempts remain, otherwise FAILED.
     */
    public List<ExpiredLease> reapExpiredLeases(Instant now, int limit) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'RETRYING' END,
                            completed_at = CASE WHEN attempts >= max_attempts THEN CAST(:now AS timestamptz) END,
                            run_at = :now, updated_at = :now,
                            last_error = 'Lease expired: worker ' || COALESCE(locked_by, '?') || ' stopped responding',
                            locked_by = NULL, lease_expires_at = NULL, version = version + 1
                        WHERE id IN (
                            SELECT id FROM jobs
                            WHERE status = 'RUNNING' AND lease_expires_at < :now
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        RETURNING id, tenant_id, type, status, attempts, cron
                        """)
                .param("now", ts(now))
                .param("limit", limit)
                .query((rs, n) -> new ExpiredLease(rs.getObject("id", UUID.class), rs.getString("tenant_id"),
                        rs.getString("type"), JobStatus.valueOf(rs.getString("status")),
                        rs.getInt("attempts"), rs.getString("cron")))
                .list();
    }

    /** Inserts the next occurrence of a recurring job, copying the finished one. */
    public UUID scheduleNextOccurrence(UUID finishedJobId, Instant runAt, Instant now) {
        UUID nextId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO jobs (id, tenant_id, type, payload, status, run_at, attempts, max_attempts,
                                          cron, parent_job_id, priority, created_at, updated_at)
                        SELECT :nextId, tenant_id, type, payload, 'PENDING', :runAt, 0, max_attempts,
                               cron, id, priority, :now, :now
                        FROM jobs WHERE id = :id
                        """)
                .param("nextId", nextId)
                .param("id", finishedJobId)
                .param("runAt", ts(runAt))
                .param("now", ts(now))
                .update();
        return nextId;
    }

    public List<JobAttempt> attempts(UUID jobId) {
        return jdbc.sql("""
                        SELECT attempt_number, worker_id, outcome, error, started_at, finished_at
                        FROM job_attempts WHERE job_id = :jobId ORDER BY id
                        """)
                .param("jobId", jobId)
                .query((rs, n) -> new JobAttempt(rs.getInt("attempt_number"), rs.getString("worker_id"),
                        rs.getString("outcome"), rs.getString("error"),
                        toInstant(rs.getTimestamp("started_at")), toInstant(rs.getTimestamp("finished_at"))))
                .list();
    }

    public void recordAttemptStarted(ClaimedJob job, String workerId, Instant now) {
        jdbc.sql("""
                        INSERT INTO job_attempts (job_id, attempt_number, worker_id, outcome, started_at)
                        VALUES (:jobId, :attempt, :workerId, 'RUNNING', :now)
                        """)
                .param("jobId", job.id())
                .param("attempt", job.attempt())
                .param("workerId", workerId)
                .param("now", ts(now))
                .update();
    }

    public void recordAttemptFinished(UUID jobId, int attempt, boolean succeeded, String error, Instant now) {
        jdbc.sql("""
                        UPDATE job_attempts SET outcome = :outcome, error = :error, finished_at = :now
                        WHERE job_id = :jobId AND attempt_number = :attempt AND outcome = 'RUNNING'
                        """)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("outcome", succeeded ? "SUCCEEDED" : "FAILED")
                .param("error", truncate(error))
                .param("now", ts(now))
                .update();
    }

    private ClaimedJob mapClaimed(ResultSet rs, int rowNum) throws SQLException {
        try {
            return new ClaimedJob(rs.getObject("id", UUID.class), rs.getString("tenant_id"),
                    rs.getString("type"), objectMapper.readValue(rs.getString("payload"), MAP),
                    rs.getInt("attempts"), rs.getInt("max_attempts"), rs.getString("cron"));
        } catch (Exception e) {
            throw new SQLException("Could not read payload of job " + rs.getString("id"), e);
        }
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    static Timestamp ts(Instant instant) {
        return Timestamp.from(instant);
    }

    static String truncate(String error) {
        if (error == null || error.length() <= MAX_ERROR_LENGTH) {
            return error;
        }
        return error.substring(0, MAX_ERROR_LENGTH);
    }
}
