package com.jobscheduler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.queue.JobMessage;
import com.jobscheduler.queue.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka entry points, one per priority. Each has its own consumer threads, so a backlog of
 * low-priority jobs never holds up high-priority ones. All instances share one consumer group,
 * so each message goes to exactly one worker.
 */
@Component
public class JobWorker {

    private final JobExecutor executor;
    private final ObjectMapper objectMapper;

    public JobWorker(JobExecutor executor, ObjectMapper objectMapper) {
        this.executor = executor;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "worker-high", topics = Topics.READY_HIGH,
            concurrency = "${scheduler.worker.concurrency.high:4}",
            autoStartup = "${scheduler.worker.enabled:true}")
    public void onHigh(String value) throws Exception {
        handle(value);
    }

    @KafkaListener(id = "worker-normal", topics = Topics.READY_NORMAL,
            concurrency = "${scheduler.worker.concurrency.normal:3}",
            autoStartup = "${scheduler.worker.enabled:true}")
    public void onNormal(String value) throws Exception {
        handle(value);
    }

    @KafkaListener(id = "worker-low", topics = Topics.READY_LOW,
            concurrency = "${scheduler.worker.concurrency.low:1}",
            autoStartup = "${scheduler.worker.enabled:true}")
    public void onLow(String value) throws Exception {
        handle(value);
    }

    private void handle(String value) throws Exception {
        executor.process(objectMapper.readValue(value, JobMessage.class));
    }
}
