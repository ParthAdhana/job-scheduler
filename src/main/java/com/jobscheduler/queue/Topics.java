package com.jobscheduler.queue;

import com.jobscheduler.job.JobPriority;

public final class Topics {

    public static final String READY_HIGH = "jobs.ready.high";
    public static final String READY_NORMAL = "jobs.ready.normal";
    public static final String READY_LOW = "jobs.ready.low";
    /** Jobs that exhausted their attempts, and messages that could not be parsed at all. */
    public static final String DEAD_LETTER = "jobs.dlq";

    private Topics() {
    }

    public static String ready(JobPriority priority) {
        return switch (priority) {
            case HIGH -> READY_HIGH;
            case NORMAL -> READY_NORMAL;
            case LOW -> READY_LOW;
        };
    }
}
