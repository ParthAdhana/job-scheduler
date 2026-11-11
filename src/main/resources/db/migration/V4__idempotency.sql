-- Day 4: idempotent submission and consumer-side deduplication.

-- The client's Idempotency-Key and a hash of the request body it was first used with.
ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(255);
ALTER TABLE jobs ADD COLUMN request_hash VARCHAR(64);

-- The durable guarantee: one job per (tenant, key), even if Redis is empty or down and two
-- identical requests race each other.
CREATE UNIQUE INDEX uq_jobs_tenant_idempotency_key
    ON jobs (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Kafka message ids a worker has already handled. Inserted in the same transaction as the job's
-- state change, so a redelivered message is recognized and skipped.
CREATE TABLE processed_messages (
    message_id    UUID PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_processed_messages_at ON processed_messages (processed_at);
