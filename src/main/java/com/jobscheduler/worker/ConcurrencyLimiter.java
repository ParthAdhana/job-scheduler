package com.jobscheduler.worker;

import com.jobscheduler.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A distributed semaphore per job type, so that, say, at most 5 {@code send-email} jobs run at
 * once across every worker instance. This protects downstream systems with their own limits.
 *
 * <p>Permits live in a Redis sorted set scored by expiry time. A worker that crashes without
 * releasing its permit does not leak it forever: the permit expires and is cleared on the next
 * acquire.
 */
@Component
public class ConcurrencyLimiter {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyLimiter.class);

    // KEYS[1] = permit set; ARGV = limit, permit id, permit ttl (ms)
    private static final RedisScript<Long> ACQUIRE = RedisScript.of("""
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then
              redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
              redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
              return 1
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Map<String, Integer> limits;
    private final Duration permitTtl;

    public ConcurrencyLimiter(StringRedisTemplate redis, SchedulerProperties properties) {
        this.redis = redis;
        this.limits = properties.concurrencyLimits();
        // Comfortably longer than a lease: a live worker always releases first.
        this.permitTtl = properties.worker().leaseDuration().multipliedBy(4);
    }

    public boolean isLimited(String type) {
        return limits.containsKey(type);
    }

    /** True if the job may run now. Fails open: if Redis is down, jobs still run. */
    public boolean tryAcquire(String type, String permitId) {
        Integer limit = limits.get(type);
        if (limit == null) {
            return true;
        }
        try {
            Long acquired = redis.execute(ACQUIRE, List.of(key(type)),
                    limit.toString(), permitId, Long.toString(permitTtl.toMillis()));
            return acquired != null && acquired == 1L;
        } catch (DataAccessException e) {
            log.warn("Redis unavailable for concurrency limit on {}, allowing job: {}", type, e.getMessage());
            return true;
        }
    }

    public void release(String type, String permitId) {
        if (!isLimited(type)) {
            return;
        }
        try {
            redis.opsForZSet().remove(key(type), permitId);
        } catch (DataAccessException e) {
            log.warn("Could not release permit for {}; it will expire on its own: {}", type, e.getMessage());
        }
    }

    private static String key(String type) {
        return "concurrency:" + type;
    }
}
