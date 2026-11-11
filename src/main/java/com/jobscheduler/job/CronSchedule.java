package com.jobscheduler.job;

import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/** Cron expressions are evaluated in UTC so every instance computes the same next run. */
public final class CronSchedule {

    private CronSchedule() {
    }

    public static Instant next(String cron, Instant after) {
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZone(ZoneOffset.UTC));
        if (next == null) {
            throw new InvalidCronException(cron);
        }
        return next.toInstant();
    }
}
