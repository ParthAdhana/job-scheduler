package com.jobscheduler.worker.handlers;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;
import org.springframework.stereotype.Component;

/** Fails every attempt. Exercises the dead-letter path. */
@Component
public class AlwaysFailHandler implements JobHandler {

    @Override
    public String type() {
        return "always-fail";
    }

    @Override
    public void execute(JobContext context) {
        throw new IllegalStateException("This job always fails");
    }
}
