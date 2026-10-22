package com.jobscheduler.job;

import java.util.EnumSet;
import java.util.Set;

public enum JobStatus {
    PENDING,
    QUEUED,
    RUNNING,
    RETRYING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    private static final Set<JobStatus> CANCELLABLE = EnumSet.of(PENDING, QUEUED, RETRYING);
    private static final Set<JobStatus> TERMINAL = EnumSet.of(SUCCEEDED, FAILED, CANCELLED);

    /** A job can be cancelled until a worker has started running it. */
    public boolean isCancellable() {
        return CANCELLABLE.contains(this);
    }

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
