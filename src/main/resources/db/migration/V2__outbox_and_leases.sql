-- Transactional outbox, worker leases and optimistic locking.

-- Every UPDATE bumps version, so a stale read (e.g. a cancel racing a worker claim) fails
-- instead of silently overwriting the newer state.
ALTER TABLE jobs ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

-- Which worker holds the job and until when. A worker that dies stops renewing the lease,
-- and the reaper hands the job to someone else once it expires.
ALTER TABLE jobs ADD COLUMN locked_by VARCHAR(100);
ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ;

-- Messages waiting to be published to Kafka. Written in the same transaction as the job
-- change that caused them, so the database and Kafka can never disagree.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    message_id    UUID         NOT NULL UNIQUE,
    topic         VARCHAR(200) NOT NULL,
    message_key   VARCHAR(200) NOT NULL,
    payload       TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ
);

-- The relay only ever scans unpublished rows, in insertion order.
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
