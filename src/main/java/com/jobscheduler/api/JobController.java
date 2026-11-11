package com.jobscheduler.api;

import com.jobscheduler.idempotency.JobSubmissionService;
import com.jobscheduler.job.Job;
import com.jobscheduler.job.JobAttempt;
import com.jobscheduler.job.JobService;
import com.jobscheduler.job.JobStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    static final String TENANT_HEADER = "X-Tenant-Id";

    static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final JobService jobService;
    private final JobSubmissionService submissions;

    public JobController(JobService jobService, JobSubmissionService submissions) {
        this.jobService = jobService;
        this.submissions = submissions;
    }

    /**
     * Submit a job. With an {@code Idempotency-Key} header, repeating the same request returns the
     * original job (200, {@code Idempotent-Replayed: true}) instead of creating another.
     */
    @PostMapping
    public ResponseEntity<JobResponse> create(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @RequestHeader(name = IDEMPOTENCY_HEADER, required = false) @Size(min = 1, max = 255) String idempotencyKey,
            @Valid @RequestBody CreateJobRequest request) {
        var submission = submissions.submit(tenantId, request.toNewJob(), idempotencyKey);
        Job job = submission.job();
        var location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(job.getId()).toUri();
        if (submission.replayed()) {
            return ResponseEntity.ok().location(location).header(REPLAYED_HEADER, "true").body(JobResponse.from(job));
        }
        return ResponseEntity.created(location).body(JobResponse.from(job));
    }

    @GetMapping("/{id}")
    public JobResponse get(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @PathVariable UUID id) {
        return JobResponse.from(jobService.get(tenantId, id));
    }

    @GetMapping
    public PageResponse<JobResponse> list(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @RequestParam(required = false) JobStatus status,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        var pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        return PageResponse.from(jobService.list(tenantId, status, pageable), JobResponse::from);
    }

    @GetMapping("/{id}/attempts")
    public List<JobAttempt> attempts(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @PathVariable UUID id) {
        return jobService.attempts(tenantId, id);
    }

    /** Re-run a job that ended FAILED, with a fresh set of attempts. */
    @PostMapping("/{id}/replay")
    public JobResponse replay(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @PathVariable UUID id) {
        return JobResponse.from(jobService.replay(tenantId, id));
    }

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @PathVariable UUID id) {
        return JobResponse.from(jobService.cancel(tenantId, id));
    }
}
