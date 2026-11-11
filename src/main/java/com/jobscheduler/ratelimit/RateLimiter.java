package com.jobscheduler.ratelimit;

import com.jobscheduler.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Token bucket per tenant, kept in Redis so every API instance shares the same budget.
 *
 * <p>The whole check-and-take runs as one Lua script, which Redis executes atomically: two
 * concurrent requests can never both take the last token. Time comes from Redis itself, so API
 * instances with skewed clocks still agree.
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    // KEYS[1] = bucket; ARGV = capacity, refill per ms. Returns {allowed, tokens left, retry after ms}.
    private static final RedisScript<List> TAKE_TOKEN = RedisScript.of("""
            local capacity = tonumber(ARGV[1])
            local refill_per_ms = tonumber(ARGV[2])
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
            local tokens = tonumber(bucket[1]) or capacity
            local ts = tonumber(bucket[2]) or now
            tokens = math.min(capacity, tokens + (now - ts) * refill_per_ms)
            local allowed = 0
            local retry_after = 0
            if tokens >= 1 then
              tokens = tokens - 1
              allowed = 1
            else
              retry_after = math.ceil((1 - tokens) / refill_per_ms)
            end
            redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
            redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / refill_per_ms) + 1000)
            return {allowed, math.floor(tokens), retry_after}
            """, List.class);

    private final StringRedisTemplate redis;
    private final SchedulerProperties.RateLimit config;

    public RateLimiter(StringRedisTemplate redis, SchedulerProperties properties) {
        this.redis = redis;
        this.config = properties.rateLimit();
    }

    public record Decision(boolean allowed, int limit, long remaining, long retryAfterMillis) {
    }

    /** Fails open: an outage in Redis should not take job submission down with it. */
    public Decision tryAcquire(String tenantId) {
        SchedulerProperties.Bucket bucket = config.forTenant(tenantId);
        if (!config.enabled()) {
            return new Decision(true, bucket.capacity(), bucket.capacity(), 0);
        }
        try {
            List<?> result = redis.execute(TAKE_TOKEN, List.of("ratelimit:" + tenantId),
                    Integer.toString(bucket.capacity()), Double.toString(bucket.refillPerSecond() / 1000.0));
            return new Decision(((Number) result.get(0)).longValue() == 1, bucket.capacity(),
                    ((Number) result.get(1)).longValue(), ((Number) result.get(2)).longValue());
        } catch (DataAccessException e) {
            log.warn("Redis unavailable for rate limiting tenant {}, allowing request: {}", tenantId, e.getMessage());
            return new Decision(true, bucket.capacity(), bucket.capacity(), 0);
        }
    }
}
