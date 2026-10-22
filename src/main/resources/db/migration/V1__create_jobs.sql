CREATE TABLE jobs (
    id            UUID PRIMARY KEY,
    tenant_id     VARCHAR(64)  NOT NULL,
    type          VARCHAR(100) NOT NULL,
    payload       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status        VARCHAR(20)  NOT NULL,
    run_at        TIMESTAMPTZ  NOT NULL,
    attempts      INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 3,
    last_error    TEXT,
    created_at    TIMESTAMPTZ  NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL,
    completed_at  TIMESTAMPTZ,
    CONSTRAINT jobs_status_check CHECK (status IN
        ('PENDING', 'QUEUED', 'RUNNING', 'RETRYING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts BETWEEN 1 AND 20),
    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0)
);

-- The scheduler (day 3) scans for due work: WHERE status = 'PENDING' AND run_at <= now()
CREATE INDEX idx_jobs_status_run_at ON jobs (status, run_at);
-- Tenant-scoped listing, newest first.
CREATE INDEX idx_jobs_tenant_created ON jobs (tenant_id, created_at DESC);

CREATE TABLE job_attempts (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_number  INT         NOT NULL,
    worker_id       VARCHAR(100),
    outcome         VARCHAR(20) NOT NULL,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    CONSTRAINT job_attempts_outcome_check CHECK (outcome IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT job_attempts_unique UNIQUE (job_id, attempt_number)
);
