package com.example.orderservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ORDERS_CREATED_TOPIC = "orders.created.v1";
    public static final String ORDERS_CREATED_DLT_TOPIC =
            ORDERS_CREATED_TOPIC + ".DLT";

    @Bean
    NewTopic ordersCreatedTopic() {
        return TopicBuilder.name(ORDERS_CREATED_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic ordersCreatedDltTopic() {
        return TopicBuilder.name(ORDERS_CREATED_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

