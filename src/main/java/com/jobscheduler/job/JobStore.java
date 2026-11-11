package com.jobscheduler.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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
     * QUEUED -> RUNNING. Succeeds only if the job is still queued and has made exactly
     * {@code attempt - 1} attempts, so late or duplicate messages cannot start it again.
     */
    public Optional<ClaimedJob> claim(UUID jobId, int attempt, String workerId, Instant now, Instant leaseUntil) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'RUNNING', attempts = attempts + 1, locked_by = :workerId,
                            lease_expires_at = :leaseUntil, updated_at = :now, version = version + 1
                        WHERE id = :id AND status = 'QUEUED' AND attempts = :previousAttempts
                        RETURNING id, tenant_id, type, payload::text AS payload, attempts, max_attempts
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
                        WHERE job_id = :jobId AND attempt_number = :attempt
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
                    rs.getInt("attempts"), rs.getInt("max_attempts"));
        } catch (Exception e) {
            throw new SQLException("Could not read payload of job " + rs.getString("id"), e);
        }
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
