package com.jobscheduler;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(String defaultTenant, int defaultMaxAttempts) {
}
