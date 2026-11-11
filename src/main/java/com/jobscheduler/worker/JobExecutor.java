package com.jobscheduler.worker;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.job.ClaimedJob;
import com.jobscheduler.job.JobStore;
import com.jobscheduler.queue.JobDispatcher;
import com.jobscheduler.queue.JobMessage;
import com.jobscheduler.scheduling.Recurrence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs one job message: claim, execute, record the outcome.
 *
 * <p>The handler runs outside any database transaction. Holding a transaction (and a pooled
 * connection) open for the length of arbitrary user work would exhaust the pool under load.
 */
@Component
public class JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);

    private final JobStore store;
    private final JobHandlerRegistry handlers;
    private final JobDispatcher dispatcher;
    private final Recurrence recurrence;
    private final RetryPolicy retryPolicy;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final Duration leaseDuration;
    private final String workerId;
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("lease-heartbeat").daemon().factory());

    public JobExecutor(JobStore store, JobHandlerRegistry handlers, JobDispatcher dispatcher,
                       Recurrence recurrence, RetryPolicy retryPolicy, TransactionTemplate tx,
                       SchedulerProperties properties, Clock clock) {
        this.store = store;
        this.handlers = handlers;
        this.dispatcher = dispatcher;
        this.recurrence = recurrence;
        this.retryPolicy = retryPolicy;
        this.tx = tx;
        this.clock = clock;
        this.leaseDuration = properties.worker().leaseDuration();
        this.workerId = properties.instanceId() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public void process(JobMessage message) {
        Optional<ClaimedJob> claimed = tx.execute(status -> claim(message));
        if (claimed == null || claimed.isEmpty()) {
            log.debug("Skipping message {} for job {}: not claimable", message.messageId(), message.jobId());
            return;
        }
        run(claimed.get());
    }

    private Optional<ClaimedJob> claim(JobMessage message) {
        Instant now = clock.instant();
        Optional<ClaimedJob> job = store.claim(message.jobId(), message.attempt(), workerId,
                now, now.plus(leaseDuration));
        job.ifPresent(j -> store.recordAttemptStarted(j, workerId, now));
        return job;
    }

    private void run(ClaimedJob job) {
        var context = new JobContext(job.id(), job.tenantId(), job.type(), job.payload(),
                job.attempt(), job.maxAttempts());
        ScheduledFuture<?> heartbeat = startHeartbeat(job);
        try {
            JobHandler handler = handlers.find(job.type())
                    .orElseThrow(() -> new IllegalStateException("No handler for job type " + job.type()));
            handler.execute(context);
            heartbeat.cancel(false);
            onSuccess(job);
        } catch (Exception e) {
            heartbeat.cancel(false);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            onFailure(job, e);
        }
    }

    /** Renews the lease every third of its length, so only a dead worker lets it expire. */
    private ScheduledFuture<?> startHeartbeat(ClaimedJob job) {
        long periodMs = Math.max(leaseDuration.toMillis() / 3, 100);
        return heartbeats.scheduleAtFixedRate(() -> {
            try {
                if (!store.extendLease(job, workerId, clock.instant().plus(leaseDuration))) {
                    log.warn("Lost lease on job {} while it was running", job.id());
                }
            } catch (RuntimeException e) {
                log.warn("Could not extend lease on job {}", job.id(), e);
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private void onSuccess(ClaimedJob job) {
        tx.executeWithoutResult(status -> {
            Instant now = clock.instant();
            if (!store.markSucceeded(job, workerId, now)) {
                log.warn("Job {} finished but this worker no longer holds its lease; result discarded", job.id());
                return;
            }
            store.recordAttemptFinished(job.id(), job.attempt(), true, null, now);
            recurrence.scheduleNext(job.id(), job.cron(), now);
            log.info("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempt());
        });
    }

    private void onFailure(ClaimedJob job, Exception error) {
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();
        tx.executeWithoutResult(status -> {
            Instant now = clock.instant();
            if (job.hasAttemptsLeft()) {
                Instant retryAt = now.plus(retryPolicy.delayAfter(job.attempt()));
                if (store.markRetrying(job, workerId, message, retryAt, now)) {
                    store.recordAttemptFinished(job.id(), job.attempt(), false, message, now);
                    log.warn("Job {} ({}) failed attempt {}/{}, retrying at {}: {}", job.id(), job.type(),
                            job.attempt(), job.maxAttempts(), retryAt, message);
                }
            } else if (store.markFailed(job, workerId, message, now)) {
                store.recordAttemptFinished(job.id(), job.attempt(), false, message, now);
                dispatcher.deadLetter(job.id(), job.tenantId(), job.type(), job.attempt(), message, now);
                recurrence.scheduleNext(job.id(), job.cron(), now);
                log.error("Job {} ({}) failed permanently after {} attempts: {}", job.id(), job.type(),
                        job.attempt(), message);
            }
        });
    }

    public String workerId() {
        return workerId;
    }
}
