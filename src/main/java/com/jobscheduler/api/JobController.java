package com.jobscheduler.api;

import com.jobscheduler.job.Job;
import com.jobscheduler.job.JobService;
import com.jobscheduler.job.JobStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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

import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    static final String TENANT_HEADER = "X-Tenant-Id";

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> create(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @Valid @RequestBody CreateJobRequest request) {
        Job job = jobService.create(tenantId, request.type(), request.payload(),
                request.runAt(), request.maxAttempts());
        var location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(job.getId()).toUri();
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

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(
            @RequestHeader(name = TENANT_HEADER, defaultValue = "${scheduler.default-tenant}") String tenantId,
            @PathVariable UUID id) {
        return JobResponse.from(jobService.cancel(tenantId, id));
    }
}
