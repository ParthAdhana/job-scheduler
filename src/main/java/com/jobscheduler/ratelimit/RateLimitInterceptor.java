package com.jobscheduler.ratelimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.observability.JobMetrics;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Applies the tenant's rate limit to job submissions, answering 429 with {@code Retry-After}
 * when the bucket is empty. Reads (GET) are not limited.
 */
@Component
public class RateLimitInterceptor implements HandlerInterceptor {

    private final RateLimiter rateLimiter;
    private final ObjectMapper objectMapper;
    private final String defaultTenant;
    private final JobMetrics metrics;

    public RateLimitInterceptor(RateLimiter rateLimiter, ObjectMapper objectMapper,
                                SchedulerProperties properties, JobMetrics metrics) {
        this.rateLimiter = rateLimiter;
        this.objectMapper = objectMapper;
        this.defaultTenant = properties.defaultTenant();
        this.metrics = metrics;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        if (!"POST".equals(request.getMethod())) {
            return true;
        }
        String tenant = request.getHeader("X-Tenant-Id");
        RateLimiter.Decision decision = rateLimiter.tryAcquire(tenant == null ? defaultTenant : tenant);
        response.setHeader("X-RateLimit-Limit", Integer.toString(decision.limit()));
        response.setHeader("X-RateLimit-Remaining", Long.toString(decision.remaining()));
        if (decision.allowed()) {
            return true;
        }

        metrics.rateLimited();
        long retryAfterSeconds = Math.max(1, (decision.retryAfterMillis() + 999) / 1000);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                "Rate limit exceeded for this tenant. Retry in " + retryAfterSeconds + "s.");
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", Long.toString(retryAfterSeconds));
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        objectMapper.writeValue(response.getOutputStream(), problem);
        return false;
    }
}
