package com.jobscheduler;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @DefaultValue("default") String defaultTenant,
        @DefaultValue("3") int defaultMaxAttempts,
        /* Identifies this instance in leases and attempt history. */
        String instanceId,
        @DefaultValue Outbox outbox,
        @DefaultValue Worker worker,
        @DefaultValue Retry retry,
        @DefaultValue Poller poller,
        @DefaultValue Idempotency idempotency,
        @DefaultValue RateLimit rateLimit,
        /* Max jobs of a type running at once across all workers, e.g. {send-email: 5}. Unlisted types are unlimited. */
        @DefaultValue Map<String, Integer> concurrencyLimits) {

    public record Outbox(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("100") int batchSize) {
    }

    public record Worker(
            @DefaultValue("true") boolean enabled,
            /* How long a worker owns a job before others may assume it died. */
            @DefaultValue("30s") Duration leaseDuration) {
    }

    /** Exponential backoff: baseDelay, 2x, 4x, ... capped at maxDelay, with jitter. */
    public record Retry(
            @DefaultValue("1s") Duration baseDelay,
            @DefaultValue("5m") Duration maxDelay) {
    }

    /** Token bucket per tenant: up to {@code capacity} requests in a burst, refilled continuously. */
    public record RateLimit(
            @DefaultValue("true") boolean enabled,
            @DefaultValue Bucket defaults,
            /* Per-tenant overrides of the default bucket. */
            @DefaultValue Map<String, Bucket> tenants) {

        public Bucket forTenant(String tenantId) {
            return tenants.getOrDefault(tenantId, defaults);
        }
    }

    public record Bucket(
            @DefaultValue("50") int capacity,
            @DefaultValue("10") double refillPerSecond) {
    }

    public record Idempotency(@DefaultValue("24h") Duration ttl) {
    }

    /** Moves due jobs into the queue and takes back jobs from dead workers. */
    public record Poller(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("100") int batchSize) {
    }
}
