package com.jobscheduler.observability;

import com.jobscheduler.job.JobPriority;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Every metric the scheduler publishes, in one place. Tags are kept low-cardinality (job type,
 * priority, outcome); never tag by tenant or job id, or Prometheus memory grows without bound.
 */
@Component
public class JobMetrics {

    private final MeterRegistry registry;

    public JobMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void submitted(String type, JobPriority priority) {
        Counter.builder("jobs.submitted").description("Jobs accepted by the API")
                .tag("type", type).tag("priority", priority.name())
                .register(registry).increment();
    }

    /** How long a job waited between becoming due and a worker starting it. */
    public void queueWait(String type, Duration wait) {
        Timer.builder("jobs.queue.wait").description("Time from run_at until a worker started the job")
                .tag("type", type)
                .publishPercentileHistogram()
                .register(registry).record(wait.isNegative() ? Duration.ZERO : wait);
    }

    public void executed(String type, boolean succeeded, Duration duration) {
        Timer.builder("jobs.execution").description("Handler run time per attempt")
                .tag("type", type).tag("outcome", succeeded ? "success" : "failure")
                .publishPercentileHistogram()
                .register(registry).record(duration);
    }

    public void retryScheduled(String type) {
        count("jobs.retries", "Failed attempts that will be retried", type);
    }

    public void deadLettered(String type) {
        count("jobs.dead_lettered", "Jobs that ran out of attempts", type);
    }

    public void deferred(String type) {
        count("jobs.deferred", "Jobs put back because their type was at its concurrency limit", type);
    }

    public void leaseReclaimed(String type) {
        count("jobs.leases.reclaimed", "Jobs taken back from workers that stopped responding", type);
    }

    public void rateLimited() {
        Counter.builder("ratelimit.rejected").description("Requests rejected with 429")
                .register(registry).increment();
    }

    public void outboxPublished(int count) {
        Counter.builder("outbox.published").description("Messages published from the outbox to Kafka")
                .register(registry).increment(count);
    }

    private void count(String name, String description, String type) {
        Counter.builder(name).description(description).tag("type", type).register(registry).increment();
    }
}
