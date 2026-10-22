package com.jobscheduler.job;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    Optional<Job> findByIdAndTenantId(UUID id, String tenantId);

    Page<Job> findByTenantId(String tenantId, Pageable pageable);

    Page<Job> findByTenantIdAndStatus(String tenantId, JobStatus status, Pageable pageable);
}
