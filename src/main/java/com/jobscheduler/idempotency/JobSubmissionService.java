package com.jobscheduler.idempotency;

import com.jobscheduler.job.Job;
import com.jobscheduler.job.JobService;
import com.jobscheduler.job.NewJob;
import com.jobscheduler.observability.JobMetrics;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Submission with optional idempotency, so clients can safely retry a POST that timed out.
 *
 * <ol>
 *   <li>Redis hit: return the job created earlier, without touching Postgres.</li>
 *   <li>Postgres hit: same, for keys Redis has evicted or never saw.</li>
 *   <li>Otherwise create the job. If a concurrent request with the same key wins the race, our
 *       insert violates the unique index; we then return the winner's job.</li>
 * </ol>
 *
 * Deliberately not {@code @Transactional}: the losing insert's transaction must roll back before
 * we can read the winner's row in a new one.
 */
@Service
public class JobSubmissionService {

    private final JobService jobService;
    private final IdempotencyCache cache;
    private final RequestHasher hasher;
    private final JobMetrics metrics;

    public JobSubmissionService(JobService jobService, IdempotencyCache cache, RequestHasher hasher,
                                JobMetrics metrics) {
        this.jobService = jobService;
        this.cache = cache;
        this.hasher = hasher;
        this.metrics = metrics;
    }

    public record Submission(Job job, boolean replayed) {
    }

    public Submission submit(String tenantId, NewJob request, String idempotencyKey) {
        if (idempotencyKey == null) {
            return created(jobService.create(tenantId, request));
        }
        String hash = hasher.hash(request);

        Optional<IdempotencyCache.Entry> cached = cache.get(tenantId, idempotencyKey);
        if (cached.isPresent()) {
            requireSameRequest(cached.get().requestHash(), hash, idempotencyKey);
            return new Submission(jobService.get(tenantId, cached.get().jobId()), true);
        }

        Optional<Job> existing = jobService.findByIdempotencyKey(tenantId, idempotencyKey);
        if (existing.isPresent()) {
            return replay(tenantId, idempotencyKey, hash, existing.get());
        }

        try {
            Job created = jobService.create(tenantId, request, idempotencyKey, hash);
            cache.put(tenantId, idempotencyKey, created.getId(), hash);
            return created(created);
        } catch (DataIntegrityViolationException raceLost) {
            Job winner = jobService.findByIdempotencyKey(tenantId, idempotencyKey).orElseThrow(() -> raceLost);
            return replay(tenantId, idempotencyKey, hash, winner);
        }
    }

    private Submission created(Job job) {
        metrics.submitted(job.getType(), job.getPriority());
        return new Submission(job, false);
    }

    private Submission replay(String tenantId, String key, String hash, Job job) {
        requireSameRequest(job.getRequestHash(), hash, key);
        cache.put(tenantId, key, job.getId(), job.getRequestHash());
        return new Submission(job, true);
    }

    private static void requireSameRequest(String originalHash, String hash, String key) {
        if (!originalHash.equals(hash)) {
            throw new IdempotencyKeyReusedException(key);
        }
    }
}
