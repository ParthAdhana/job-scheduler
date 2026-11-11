package com.jobscheduler.worker;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.job.ClaimedJob;
import com.jobscheduler.job.JobStore;
import com.jobscheduler.queue.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

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
    private final TransactionTemplate tx;
    private final SchedulerProperties properties;
    private final Clock clock;
    private final String workerId;

    public JobExecutor(JobStore store, JobHandlerRegistry handlers, TransactionTemplate tx,
                       SchedulerProperties properties, Clock clock) {
        this.store = store;
        this.handlers = handlers;
        this.tx = tx;
        this.properties = properties;
        this.clock = clock;
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
                now, now.plus(properties.worker().leaseDuration()));
        job.ifPresent(j -> store.recordAttemptStarted(j, workerId, now));
        return job;
    }

    private void run(ClaimedJob job) {
        var context = new JobContext(job.id(), job.tenantId(), job.type(), job.payload(),
                job.attempt(), job.maxAttempts());
        try {
            JobHandler handler = handlers.find(job.type())
                    .orElseThrow(() -> new IllegalStateException("No handler for job type " + job.type()));
            handler.execute(context);
            onSuccess(job);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            onFailure(job, e);
        }
    }

    private void onSuccess(ClaimedJob job) {
        tx.executeWithoutResult(status -> {
            Instant now = clock.instant();
            if (store.markSucceeded(job, workerId, now)) {
                store.recordAttemptFinished(job.id(), job.attempt(), true, null, now);
                log.info("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempt());
            } else {
                log.warn("Job {} finished but this worker no longer holds its lease; result discarded", job.id());
            }
        });
    }

    private void onFailure(ClaimedJob job, Exception error) {
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();
        tx.executeWithoutResult(status -> {
            Instant now = clock.instant();
            if (store.markFailed(job, workerId, message, now)) {
                store.recordAttemptFinished(job.id(), job.attempt(), false, message, now);
                log.warn("Job {} ({}) failed on attempt {}: {}", job.id(), job.type(), job.attempt(), message);
            }
        });
    }

    public String workerId() {
        return workerId;
    }
}
