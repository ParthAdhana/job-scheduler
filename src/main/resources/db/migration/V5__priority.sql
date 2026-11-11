-- Priorities. Each priority has its own Kafka topic and worker pool, so a flood of
-- low-priority work cannot delay high-priority jobs.
ALTER TABLE jobs ADD COLUMN priority VARCHAR(10) NOT NULL DEFAULT 'NORMAL';
ALTER TABLE jobs ADD CONSTRAINT jobs_priority_check CHECK (priority IN ('HIGH', 'NORMAL', 'LOW'));
