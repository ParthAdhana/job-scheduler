package com.jobscheduler.idempotency;

import com.jobscheduler.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/**
 * Redis in front of the Postgres unique index: answers repeated keys without touching the
 * database. It is only a cache. If Redis is down, or has evicted a key, Postgres still decides.
 */
@Component
public class IdempotencyCache {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCache.class);

    private final StringRedisTemplate redis;
    private final SchedulerProperties properties;

    public IdempotencyCache(StringRedisTemplate redis, SchedulerProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public record Entry(UUID jobId, String requestHash) {
    }

    public Optional<Entry> get(String tenantId, String key) {
        try {
            String value = redis.opsForValue().get(redisKey(tenantId, key));
            if (value == null) {
                return Optional.empty();
            }
            int separator = value.indexOf(':');
            return Optional.of(new Entry(UUID.fromString(value.substring(0, separator)),
                    value.substring(separator + 1)));
        } catch (DataAccessException e) {
            log.warn("Redis unavailable for idempotency lookup, falling back to Postgres: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public void put(String tenantId, String key, UUID jobId, String requestHash) {
        try {
            redis.opsForValue().set(redisKey(tenantId, key), jobId + ":" + requestHash,
                    properties.idempotency().ttl());
        } catch (DataAccessException e) {
            log.warn("Could not cache idempotency key in Redis: {}", e.getMessage());
        }
    }

    private static String redisKey(String tenantId, String key) {
        return "idempotency:" + tenantId + ":" + key;
    }
}
