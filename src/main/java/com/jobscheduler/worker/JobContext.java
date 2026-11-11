package com.jobscheduler.worker;

import java.util.Map;
import java.util.UUID;

/** Everything a handler may know about the job it is running. */
public record JobContext(UUID jobId, String tenantId, String type, Map<String, Object> payload,
                         int attempt, int maxAttempts) {

    public int intParam(String name, int defaultValue) {
        Object value = payload.get(name);
        return value instanceof Number n ? n.intValue() : defaultValue;
    }

    public double doubleParam(String name, double defaultValue) {
        Object value = payload.get(name);
        return value instanceof Number n ? n.doubleValue() : defaultValue;
    }
}
