package com.jobscheduler.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    void delayDoublesEachAttempt() {
        var policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), () -> 1.0);

        assertThat(policy.delayAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayAfter(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayAfter(3)).isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    void delayIsCappedAtMaximum() {
        var policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), () -> 1.0);

        assertThat(policy.delayAfter(20)).isEqualTo(Duration.ofMinutes(5));
        assertThat(policy.delayAfter(1000)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void jitterKeepsAtLeastHalfTheDelay() {
        var policy = new RetryPolicy(Duration.ofSeconds(8), Duration.ofMinutes(5), () -> 0.0);

        assertThat(policy.delayAfter(1)).isEqualTo(Duration.ofSeconds(4));
    }
}
