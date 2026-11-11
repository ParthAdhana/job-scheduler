package com.jobscheduler.scheduling;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.job.DueJob;
import com.jobscheduler.job.JobStore;
import com.jobscheduler.queue.JobDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.List;

/**
 * Queues delayed jobs and retries once their {@code run_at} arrives.
 *
 * <p>Kafka has no "deliver later", so waiting happens in Postgres and only due work goes to Kafka.
 * Every instance can run this poller: {@code SKIP LOCKED} hands each one a different batch, so
 * there is no leader to elect and no single point of failure.
 */
@Component
@ConditionalOnProperty(prefix = "scheduler.poller", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DueJobPoller {

    private static final Logger log = LoggerFactory.getLogger(DueJobPoller.class);

    private final JobStore store;
    private final JobDispatcher dispatcher;
    private final SchedulerProperties properties;
    private final Clock clock;

    public DueJobPoller(JobStore store, JobDispatcher dispatcher, SchedulerProperties properties, Clock clock) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${scheduler.poller.interval-ms:500}")
    @Transactional
    public void queueDueJobs() {
        List<DueJob> due = store.queueDueJobs(clock.instant(), properties.poller().batchSize());
        for (DueJob job : due) {
            dispatcher.dispatch(job.id(), job.tenantId(), job.type(), job.attempts() + 1);
        }
        if (!due.isEmpty()) {
            log.debug("Queued {} due jobs", due.size());
        }
    }
}
