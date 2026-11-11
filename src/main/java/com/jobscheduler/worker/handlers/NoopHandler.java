package com.jobscheduler.worker.handlers;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;
import org.springframework.stereotype.Component;

/** Does nothing. Useful for measuring the scheduler's own overhead. */
@Component
public class NoopHandler implements JobHandler {

    @Override
    public String type() {
        return "noop";
    }

    @Override
    public void execute(JobContext context) {
    }
}
