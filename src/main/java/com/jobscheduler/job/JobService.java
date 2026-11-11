package com.jobscheduler.job;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.queue.JobDispatcher;
import com.jobscheduler.worker.JobHandlerRegistry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobs;
    private final JobDispatcher dispatcher;
    private final JobHandlerRegistry handlers;
    private final SchedulerProperties properties;
    private final Clock clock;

    public JobService(JobRepository jobs, JobDispatcher dispatcher, JobHandlerRegistry handlers,
                      SchedulerProperties properties, Clock clock) {
        this.jobs = jobs;
        this.dispatcher = dispatcher;
        this.handlers = handlers;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional
    public Job create(String tenantId, String type, Map<String, Object> payload,
                      Instant runAt, Integer maxAttempts) {
        if (!handlers.supports(type)) {
            throw new UnknownJobTypeException(type, handlers.types());
        }
        Instant now = clock.instant();
        int attempts = maxAttempts == null ? properties.defaultMaxAttempts() : maxAttempts;
        Job job = Job.create(tenantId, type, payload, runAt, attempts, now);
        if (job.isDue(now)) {
            // Due already: queue it in this same transaction rather than waiting for the poller.
            job.markQueued(now);
            dispatcher.dispatch(job.getId(), tenantId, type, 1);
        }
        return jobs.save(job);
    }

    @Transactional(readOnly = true)
    public Job get(String tenantId, UUID id) {
        return jobs.findByIdAndTenantId(id, tenantId).orElseThrow(() -> new JobNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Page<Job> list(String tenantId, JobStatus status, Pageable pageable) {
        return status == null
                ? jobs.findByTenantId(tenantId, pageable)
                : jobs.findByTenantIdAndStatus(tenantId, status, pageable);
    }

    @Transactional
    public Job cancel(String tenantId, UUID id) {
        Job job = get(tenantId, id);
        job.cancel(clock.instant());
        return job;
    }
}
