package com.jobscheduler.worker.handlers;

import com.jobscheduler.worker.JobContext;
import com.jobscheduler.worker.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A stand-in for a real email provider: random latency and a configurable failure rate.
 * Payload: {@code {"to": "a@example.com", "failureRate": 0.1}}.
 */
@Component
public class SendEmailHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(SendEmailHandler.class);

    @Override
    public String type() {
        return "send-email";
    }

    @Override
    public void execute(JobContext context) throws Exception {
        var random = ThreadLocalRandom.current();
        Thread.sleep(random.nextInt(20, 120));
        if (random.nextDouble() < context.doubleParam("failureRate", 0.1)) {
            throw new IllegalStateException("Email provider returned 503");
        }
        log.info("Sent email to {}", context.payload().getOrDefault("to", "unknown"));
    }
}
