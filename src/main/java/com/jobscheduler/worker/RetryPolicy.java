package com.jobscheduler.worker;

import com.jobscheduler.SchedulerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with "equal jitter": half the delay is fixed, half is random.
 *
 * <p>Without jitter, a thousand jobs that failed together (say, a downstream outage) would all
 * retry at the same instant and knock the recovering downstream over again.
 */
@Component
public class RetryPolicy {

    private final Duration baseDelay;
    private final Duration maxDelay;
    private final DoubleSupplier random;

    @Autowired
    public RetryPolicy(SchedulerProperties properties) {
        this(properties.retry().baseDelay(), properties.retry().maxDelay(),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    RetryPolicy(Duration baseDelay, Duration maxDelay, DoubleSupplier random) {
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.random = random;
    }

    /** Delay before the attempt after {@code failedAttempt} (1-based). */
    public Duration delayAfter(int failedAttempt) {
        long base = baseDelay.toMillis();
        int exponent = Math.min(failedAttempt - 1, 30);
        long exponential = base << exponent;
        long capped = exponential < 0 ? maxDelay.toMillis() : Math.min(exponential, maxDelay.toMillis());
        long half = capped / 2;
        return Duration.ofMillis(half + (long) (random.getAsDouble() * half));
    }
}
