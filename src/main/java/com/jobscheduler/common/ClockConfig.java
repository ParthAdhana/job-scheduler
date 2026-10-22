package com.jobscheduler.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ClockConfig {

    /** Injected instead of calling Instant.now() so tests can control time. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
