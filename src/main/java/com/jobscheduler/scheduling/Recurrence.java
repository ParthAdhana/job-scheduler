package com.jobscheduler.scheduling;

import com.jobscheduler.job.CronSchedule;
import com.jobscheduler.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Recurring jobs are a chain: when one occurrence reaches a terminal state, the next is inserted
 * in the same transaction. That transaction only commits for whoever won the state change, so the
 * next occurrence is created exactly once even with many workers. Cancelling the pending
 * occurrence ends the chain.
 */
@Component
public class Recurrence {

    private static final Logger log = LoggerFactory.getLogger(Recurrence.class);

    private final JobStore store;

    public Recurrence(JobStore store) {
        this.store = store;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void scheduleNext(UUID finishedJobId, String cron, Instant now) {
        if (cron == null) {
            return;
        }
        Instant next = CronSchedule.next(cron, now);
        UUID nextId = store.scheduleNextOccurrence(finishedJobId, next, now);
        log.info("Recurring job {} scheduled next occurrence {} at {}", finishedJobId, nextId, next);
    }
}
