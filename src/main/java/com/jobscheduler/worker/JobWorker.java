package com.jobscheduler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.queue.JobMessage;
import com.jobscheduler.queue.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka entry point. All instances share one consumer group, so each message goes to one worker. */
@Component
public class JobWorker {

    private final JobExecutor executor;
    private final ObjectMapper objectMapper;

    public JobWorker(JobExecutor executor, ObjectMapper objectMapper) {
        this.executor = executor;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "job-worker", topics = Topics.READY,
            concurrency = "${scheduler.worker.concurrency:3}",
            autoStartup = "${scheduler.worker.enabled:true}")
    public void onMessage(String value) throws Exception {
        executor.process(objectMapper.readValue(value, JobMessage.class));
    }
}
