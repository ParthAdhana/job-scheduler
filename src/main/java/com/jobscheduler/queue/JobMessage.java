package com.jobscheduler.queue;

import java.util.UUID;

/**
 * What goes on Kafka: a pointer to the job, not the job itself. Postgres stays the source of
 * truth, so a stale or duplicated message can never resurrect old state.
 *
 * @param attempt the attempt number this message is allowed to start. A worker only claims the
 *                job if it has made exactly {@code attempt - 1} attempts so far, which turns any
 *                late or duplicate delivery into a no-op.
 */
public record JobMessage(UUID messageId, UUID jobId, String tenantId, String type, int attempt) {
}
