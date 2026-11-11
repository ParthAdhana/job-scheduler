package com.jobscheduler.scheduling;

import com.jobscheduler.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/** Keeps the outbox and dedupe tables from growing forever. */
@Component
@ConditionalOnProperty(prefix = "scheduler.poller", name = "enabled", havingValue = "true", matchIfMissing = true)
public class Housekeeping {

    private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);
    private static final Duration RETENTION = Duration.ofDays(7);

    private final JobStore store;
    private final Clock clock;

    public Housekeeping(JobStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT1M")
    public void purgeOldRecords() {
        Instant cutoff = clock.instant().minus(RETENTION);
        int messages = store.deleteProcessedMessagesBefore(cutoff);
        int outbox = store.deletePublishedOutboxBefore(cutoff);
        log.info("Housekeeping removed {} dedupe records and {} published outbox rows", messages, outbox);
    }
}
