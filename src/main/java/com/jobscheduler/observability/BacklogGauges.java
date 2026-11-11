package com.jobscheduler.observability;

import com.jobscheduler.job.JobStatus;
import com.jobscheduler.job.JobStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Jobs per status and the outbox backlog. Refreshed on a timer and cached, so a Prometheus
 * scrape never runs a COUNT(*) against the jobs table itself.
 */
@Component
public class BacklogGauges {

    private final JobStore store;
    private final Map<JobStatus, AtomicLong> byStatus = new EnumMap<>(JobStatus.class);
    private final AtomicLong outboxBacklog = new AtomicLong();

    public BacklogGauges(JobStore store, MeterRegistry registry) {
        this.store = store;
        for (JobStatus status : JobStatus.values()) {
            AtomicLong value = new AtomicLong();
            byStatus.put(status, value);
            Gauge.builder("jobs.by_status", value, AtomicLong::get)
                    .description("Jobs currently in each status").tag("status", status.name())
                    .register(registry);
        }
        Gauge.builder("outbox.backlog", outboxBacklog, AtomicLong::get)
                .description("Outbox rows not yet published to Kafka").register(registry);
    }

    @Scheduled(fixedDelayString = "${scheduler.metrics.refresh-interval-ms:10000}")
    public void refresh() {
        Map<String, Long> counts = store.countByStatus();
        byStatus.forEach((status, value) -> value.set(counts.getOrDefault(status.name(), 0L)));
        outboxBacklog.set(store.outboxBacklog());
    }
}
