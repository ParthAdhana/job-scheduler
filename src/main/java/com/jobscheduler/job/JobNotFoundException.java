package com.jobscheduler.job;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID jobId) {
        super("Job " + jobId + " not found");
    }
}
