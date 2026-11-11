package com.jobscheduler.queue;

import org.springframework.stereotype.Component;

import java.util.UUID;

/** Turns "this job should run now" into an outbox message on the right topic. */
@Component
public class JobDispatcher {

    private final OutboxWriter outbox;

    public JobDispatcher(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    public void dispatch(UUID jobId, String tenantId, String type, int nextAttempt) {
        var message = new JobMessage(UUID.randomUUID(), jobId, tenantId, type, nextAttempt);
        outbox.append(Topics.READY, message);
    }
}
