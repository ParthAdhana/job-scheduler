package com.jobscheduler.worker.handlers;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;
import org.springframework.stereotype.Component;

/** Simulates slow work. Payload: {@code {"durationMs": 500}}. */
@Component
public class SleepHandler implements JobHandler {

    @Override
    public String type() {
        return "sleep";
    }

    @Override
    public void execute(JobContext context) throws InterruptedException {
        Thread.sleep(context.intParam("durationMs", 100));
    }
}
