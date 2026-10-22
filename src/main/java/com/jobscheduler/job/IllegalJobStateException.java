package com.jobscheduler.job;

import java.util.UUID;

public class IllegalJobStateException extends RuntimeException {

    public IllegalJobStateException(UUID jobId, JobStatus from, JobStatus to) {
        super("Job " + jobId + " cannot move from " + from + " to " + to);
    }
}
