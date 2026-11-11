package com.jobscheduler;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @DefaultValue("default") String defaultTenant,
        @DefaultValue("3") int defaultMaxAttempts,
        /* Identifies this instance in leases and attempt history. */
        String instanceId,
        @DefaultValue Outbox outbox,
        @DefaultValue Worker worker,
        @DefaultValue Retry retry,
        @DefaultValue Poller poller) {

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

    /** Moves due jobs into the queue and takes back jobs from dead workers. */
    public record Poller(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("100") int batchSize) {
    }
}
