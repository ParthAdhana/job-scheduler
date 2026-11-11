package com.jobscheduler.scheduling;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.job.ExpiredLease;
import com.jobscheduler.job.JobStatus;
import com.jobscheduler.job.JobStore;
import com.jobscheduler.observability.JobMetrics;
import com.jobscheduler.queue.JobDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/**
 * Recovers jobs from workers that crashed or hung. A healthy worker renews its lease while it
 * runs; one that stops renewing is presumed dead, and its job is retried (or dead-lettered).
 *
 * <p>If the "dead" worker was only slow and finishes later, its result is discarded because its
 * final UPDATE checks it still holds the lease. Handlers must therefore be idempotent.
 */
@Component
@ConditionalOnProperty(prefix = "scheduler.poller", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobStore store;
    private final JobDispatcher dispatcher;
    private final Recurrence recurrence;
    private final JobMetrics metrics;
    private final SchedulerProperties properties;
    private final Clock clock;

    public LeaseReaper(JobStore store, JobDispatcher dispatcher, Recurrence recurrence, JobMetrics metrics,
                       SchedulerProperties properties, Clock clock) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.recurrence = recurrence;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${scheduler.poller.reaper-interval-ms:5000}")
    @Transactional
    public void reapExpiredLeases() {
        Instant now = clock.instant();
        for (ExpiredLease job : store.reapExpiredLeases(now, properties.poller().batchSize())) {
            String error = "Lease expired: worker stopped responding";
            store.recordAttemptFinished(job.id(), job.attempts(), false, error, now);
            metrics.leaseReclaimed(job.type());
            if (job.newStatus() == JobStatus.FAILED) {
                dispatcher.deadLetter(job.id(), job.tenantId(), job.type(), job.attempts(), error, now);
                metrics.deadLettered(job.type());
                recurrence.scheduleNext(job.id(), job.cron(), now);
            }
            log.warn("Reclaimed job {} from an expired lease; now {}", job.id(), job.newStatus());
        }
    }
}
