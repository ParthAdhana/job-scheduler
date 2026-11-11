package com.jobscheduler.support;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only handler that records how many times each job actually ran. */
public class CountingHandler implements JobHandler {

    private final Map<UUID, AtomicInteger> runs = new ConcurrentHashMap<>();

    @Override
    public String type() {
        return "counting";
    }

    @Override
    public void execute(JobContext context) {
        runs.computeIfAbsent(context.jobId(), id -> new AtomicInteger()).incrementAndGet();
    }

    public int runs(UUID jobId) {
        AtomicInteger count = runs.get(jobId);
        return count == null ? 0 : count.get();
    }
}
