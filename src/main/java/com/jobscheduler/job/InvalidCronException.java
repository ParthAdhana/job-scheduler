package com.jobscheduler.job;

public class InvalidCronException extends RuntimeException {

    public InvalidCronException(String cron) {
        super("Invalid cron expression '" + cron + "'. Use 6 fields, seconds first, e.g. '0 */5 * * * *'");
    }
}
