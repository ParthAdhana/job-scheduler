-- End-to-end results for one load test run.
--   psql -h localhost -p 5433 -U scheduler scheduler -v run_id="'run-123'" -f loadtest/report.sql
WITH run AS (
    SELECT j.*, a.started_at
    FROM jobs j
    JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1
    WHERE j.payload ->> 'runId' = :run_id
)
SELECT
    count(*)                                                                    AS jobs,
    count(*) FILTER (WHERE status = 'SUCCEEDED')                                AS succeeded,
    round(count(*) / NULLIF(extract(epoch FROM max(completed_at) - min(created_at)), 0)::numeric, 1)
                                                                                AS jobs_per_sec,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at) * 1000)::numeric, 1)
                                                                                AS p50_pickup_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at) * 1000)::numeric, 1)
                                                                                AS p95_pickup_ms,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - created_at) * 1000)::numeric, 1)
                                                                                AS p50_end_to_end_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - created_at) * 1000)::numeric, 1)
                                                                                AS p95_end_to_end_ms,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - created_at) * 1000)::numeric, 1)
                                                                                AS p99_end_to_end_ms
FROM run;
