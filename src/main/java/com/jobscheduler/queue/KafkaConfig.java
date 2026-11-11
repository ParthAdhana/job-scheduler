package com.jobscheduler.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /** Partitions cap how many workers can consume in parallel within one consumer group. */
    @Bean
    NewTopic readyHighTopic() {
        return TopicBuilder.name(Topics.READY_HIGH).partitions(6).replicas(1).build();
    }

    @Bean
    NewTopic readyNormalTopic() {
        return TopicBuilder.name(Topics.READY_NORMAL).partitions(6).replicas(1).build();
    }

    @Bean
    NewTopic readyLowTopic() {
        return TopicBuilder.name(Topics.READY_LOW).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(Topics.DEAD_LETTER).partitions(3).replicas(1).build();
    }

    /**
     * Handles exceptions thrown out of a listener, which here means infrastructure trouble (the
     * database is briefly down) or a message that is not valid JSON. Job failures never get here:
     * the executor records those itself.
     *
     * <p>Retry a few times, then move the record to the dead-letter topic so one bad message cannot
     * block its partition forever. Unparseable messages skip the retries.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(Topics.DEAD_LETTER, -1));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 3));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}
