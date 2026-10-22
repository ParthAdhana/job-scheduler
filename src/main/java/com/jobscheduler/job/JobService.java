package com.jobscheduler.job;

import com.jobscheduler.SchedulerProperties;
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
    private final SchedulerProperties properties;
    private final Clock clock;

    public JobService(JobRepository jobs, SchedulerProperties properties, Clock clock) {
        this.jobs = jobs;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional
    public Job create(String tenantId, String type, Map<String, Object> payload,
                      Instant runAt, Integer maxAttempts) {
        int attempts = maxAttempts == null ? properties.defaultMaxAttempts() : maxAttempts;
        Job job = Job.create(tenantId, type, payload, runAt, attempts, clock.instant());
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
