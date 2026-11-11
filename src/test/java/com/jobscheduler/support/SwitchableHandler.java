package com.jobscheduler.support;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;

import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only handler whose failures a test can switch on and off, like a flaky downstream. */
public class SwitchableHandler implements JobHandler {

    private final AtomicBoolean failing = new AtomicBoolean(true);

    @Override
    public String type() {
        return "switchable";
    }

    @Override
    public void execute(JobContext context) {
        if (failing.get()) {
            throw new IllegalStateException("Downstream unavailable");
        }
    }

    public void setFailing(boolean fail) {
        failing.set(fail);
    }
}
