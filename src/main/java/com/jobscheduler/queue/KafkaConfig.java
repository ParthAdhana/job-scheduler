package com.jobscheduler.queue;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    /** Partitions cap how many workers can consume in parallel within one consumer group. */
    @Bean
    NewTopic readyTopic() {
        return TopicBuilder.name(Topics.READY).partitions(6).replicas(1).build();
    }
}
