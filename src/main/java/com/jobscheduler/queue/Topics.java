package com.jobscheduler.queue;

public final class Topics {

    public static final String READY = "jobs.ready";
    /** Jobs that exhausted their attempts, and messages that could not be parsed at all. */
    public static final String DEAD_LETTER = "jobs.dlq";

    private Topics() {
    }
}
