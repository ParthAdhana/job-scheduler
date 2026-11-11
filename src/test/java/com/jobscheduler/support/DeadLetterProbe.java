package com.jobscheduler.support;

import com.jobscheduler.queue.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

/** Reads the dead-letter topic from the beginning, looking for one record. */
public class DeadLetterProbe {

    private final ConsumerFactory<String, String> consumerFactory;

    public DeadLetterProbe(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public boolean contains(Predicate<ConsumerRecord<String, String>> match, Duration timeout) {
        // A fresh group with auto-offset-reset=earliest reads the topic from the start.
        try (Consumer<String, String> consumer =
                     consumerFactory.createConsumer("dlq-probe-" + UUID.randomUUID(), "probe")) {
            consumer.subscribe(List.of(Topics.DEAD_LETTER));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (match.test(record)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
