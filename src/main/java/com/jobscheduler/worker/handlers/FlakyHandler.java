package com.jobscheduler.worker.handlers;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;
import org.springframework.stereotype.Component;

/** Fails the first {@code failTimes} attempts (default 2), then succeeds. Exercises retries. */
@Component
public class FlakyHandler implements JobHandler {

    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public void execute(JobContext context) {
        int failTimes = context.intParam("failTimes", 2);
        if (context.attempt() <= failTimes) {
            throw new IllegalStateException("Simulated failure on attempt " + context.attempt());
        }
    }
}
