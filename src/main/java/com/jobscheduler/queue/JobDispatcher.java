package com.jobscheduler.queue;

import com.jobscheduler.job.JobPriority;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/** Turns job state changes into outbox messages on the right topic. */
@Component
public class JobDispatcher {

    private final OutboxWriter outbox;

    public JobDispatcher(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    public void dispatch(UUID jobId, String tenantId, String type, JobPriority priority, int nextAttempt) {
        var message = new JobMessage(UUID.randomUUID(), jobId, tenantId, type, nextAttempt);
        outbox.append(Topics.ready(priority), message.messageId(), jobId.toString(), message);
    }

    public void deadLetter(UUID jobId, String tenantId, String type, int attempts, String error, Instant now) {
        var message = new DeadLetter(UUID.randomUUID(), jobId, tenantId, type, attempts, error, now);
        outbox.append(Topics.DEAD_LETTER, message.messageId(), jobId.toString(), message);
    }
}
