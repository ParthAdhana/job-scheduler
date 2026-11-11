package com.jobscheduler.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.jobscheduler.SchedulerProperties;
import com.jobscheduler.observability.JobMetrics;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Publishes outbox rows to Kafka and marks them sent.
 *
 * <p>Rows are locked with {@code FOR UPDATE SKIP LOCKED}, so any number of instances can run the
 * relay at once without publishing the same row twice in the normal case. If Kafka accepts a
 * message but the transaction then fails, the row is published again on the next pass: delivery
 * is at-least-once, and consumers deduplicate.
 */
@Component
@ConditionalOnProperty(prefix = "scheduler.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final SchedulerProperties properties;
    private final Clock clock;
    private final JobMetrics metrics;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka,
                       SchedulerProperties properties, Clock clock, JobMetrics metrics) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.properties = properties;
        this.clock = clock;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox.poll-interval-ms:200}")
    @Transactional
    public void publishPending() {
        List<OutboxRow> rows = jdbc.sql("""
                        SELECT id, topic, message_key, payload FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", properties.outbox().batchSize())
                .query(OutboxRow.class)
                .list();
        if (rows.isEmpty()) {
            return;
        }

        // Send the whole batch, then wait once. Kafka batches these on the wire.
        CompletableFuture<?>[] sends = rows.stream()
                .map(row -> kafka.send(row.topic(), row.messageKey(), row.payload()))
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(sends).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Roll back so every row in the batch stays unpublished and is retried.
            throw new IllegalStateException("Kafka publish failed for outbox batch", e);
        }

        jdbc.sql("UPDATE outbox SET published_at = :now WHERE id IN (:ids)")
                .param("now", Timestamp.from(clock.instant()))
                .param("ids", rows.stream().map(OutboxRow::id).toList())
                .update();
        metrics.outboxPublished(rows.size());
        log.debug("Published {} outbox messages", rows.size());
    }

    record OutboxRow(long id, String topic, String messageKey, String payload) {
    }
}
