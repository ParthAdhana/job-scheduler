package com.jobscheduler.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/** Appends messages to the outbox table inside the caller's transaction. */
@Component
public class OutboxWriter {

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OutboxWriter(JdbcClient jdbc, ObjectMapper objectMapper, Clock clock) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /** Must join an existing transaction: an outbox row on its own would defeat the pattern. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, JobMessage message) {
        jdbc.sql("""
                        INSERT INTO outbox (message_id, topic, message_key, payload, created_at)
                        VALUES (:messageId, :topic, :key, :payload, :createdAt)
                        """)
                .param("messageId", message.messageId())
                .param("topic", topic)
                .param("key", message.jobId().toString())
                .param("payload", toJson(message))
                .param("createdAt", java.sql.Timestamp.from(Instant.now(clock)))
                .update();
    }

    private String toJson(JobMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize " + message, e);
        }
    }
}
