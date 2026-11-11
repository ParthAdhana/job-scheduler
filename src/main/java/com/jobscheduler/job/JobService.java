package com.jobscheduler.job;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.queue.JobDispatcher;
import com.jobscheduler.worker.JobHandlerRegistry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobs;
    private final JobStore store;
    private final JobDispatcher dispatcher;
    private final JobHandlerRegistry handlers;
    private final SchedulerProperties properties;
    private final Clock clock;

    public JobService(JobRepository jobs, JobStore store, JobDispatcher dispatcher, JobHandlerRegistry handlers,
                      SchedulerProperties properties, Clock clock) {
        this.jobs = jobs;
        this.store = store;
        this.dispatcher = dispatcher;
        this.handlers = handlers;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional
    public Job create(String tenantId, NewJob request) {
        return create(tenantId, request, null, null);
    }

    /**
     * Creates the job, queueing it immediately if it is already due. With an idempotency key, a
     * concurrent duplicate makes this fail on the unique index at commit; see
     * {@link com.jobscheduler.idempotency.JobSubmissionService}.
     */
    @Transactional
    public Job create(String tenantId, NewJob request, String idempotencyKey, String requestHash) {
        if (!handlers.supports(request.type())) {
            throw new UnknownJobTypeException(request.type(), handlers.types());
        }
        Instant now = clock.instant();
        Instant runAt = request.runAt();
        if (request.cron() != null) {
            if (!CronExpression.isValidExpression(request.cron())) {
                throw new InvalidCronException(request.cron());
            }
            if (runAt == null) {
                runAt = CronSchedule.next(request.cron(), now);
            }
        }
        int attempts = request.maxAttempts() == null ? properties.defaultMaxAttempts() : request.maxAttempts();
        Job job = Job.create(tenantId, request.type(), request.payload(), runAt, attempts, request.cron(), now);
        if (request.priority() != null) {
            job.setPriority(request.priority());
        }
        if (idempotencyKey != null) {
            job.assignIdempotencyKey(idempotencyKey, requestHash);
        }
        String type = request.type();
        if (job.isDue(now)) {
            // Due already: queue it in this same transaction rather than waiting for the poller.
            job.markQueued(now);
            dispatcher.dispatch(job.getId(), tenantId, type, job.getPriority(), 1);
        }
        return jobs.save(job);
    }

    @Transactional(readOnly = true)
    public Job get(String tenantId, UUID id) {
        return jobs.findByIdAndTenantId(id, tenantId).orElseThrow(() -> new JobNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Optional<Job> findByIdempotencyKey(String tenantId, String idempotencyKey) {
        return jobs.findByTenantIdAndIdempotencyKey(tenantId, idempotencyKey);
    }

    @Transactional(readOnly = true)
    public Page<Job> list(String tenantId, JobStatus status, Pageable pageable) {
        return status == null
                ? jobs.findByTenantId(tenantId, pageable)
                : jobs.findByTenantIdAndStatus(tenantId, status, pageable);
    }

    /** Re-run a job that exhausted its attempts, e.g. after fixing the downstream it depends on. */
    @Transactional
    public Job replay(String tenantId, UUID id) {
        Job job = get(tenantId, id);
        job.replay(clock.instant());
        dispatcher.dispatch(job.getId(), tenantId, job.getType(), job.getPriority(), 1);
        return job;
    }

    @Transactional(readOnly = true)
    public List<JobAttempt> attempts(String tenantId, UUID id) {
        get(tenantId, id);
        return store.attempts(id);
    }

    @Transactional
    public Job cancel(String tenantId, UUID id) {
        Job job = get(tenantId, id);
        job.cancel(clock.instant());
        return job;
    }
}
