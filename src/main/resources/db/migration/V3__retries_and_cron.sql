-- Recurring jobs and the indexes the poller and reaper scan.

-- A Spring cron expression (6 fields, seconds first). When an occurrence finishes, the next one
-- is inserted as a new job pointing back at it.
ALTER TABLE jobs ADD COLUMN cron VARCHAR(100);
ALTER TABLE jobs ADD COLUMN parent_job_id UUID REFERENCES jobs (id) ON DELETE SET NULL;

-- Poller: due work. Partial, so the index only holds jobs that are actually waiting.
CREATE INDEX idx_jobs_due ON jobs (run_at) WHERE status IN ('PENDING', 'RETRYING');
-- Reaper: running jobs whose worker stopped renewing the lease.
CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at) WHERE status = 'RUNNING';

-- Superseded by idx_jobs_due.
DROP INDEX idx_jobs_status_run_at;

-- A replayed job starts counting attempts from 1 again, so attempt numbers can repeat in the
-- history. Order history by id instead.
ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_unique;
CREATE INDEX idx_job_attempts_job ON job_attempts (job_id, id);
