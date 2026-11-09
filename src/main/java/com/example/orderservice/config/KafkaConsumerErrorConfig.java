package com.example.orderservice.config;

import com.example.orderservice.order.InvalidAmountException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerErrorConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        KafkaTopicConfig.ORDERS_CREATED_DLT_TOPIC,
                        record.partition()
                )
        );


        var errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(500L, 2L)
        );

        // Add InvalidAmountException to the list of non-retryable exceptions
        errorHandler.addNotRetryableExceptions(InvalidAmountException.class);

        return errorHandler;
    }
}