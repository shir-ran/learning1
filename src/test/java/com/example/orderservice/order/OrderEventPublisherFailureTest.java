package com.example.orderservice.order;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderEventPublisherFailureTest {

    @Test
    void reportsFailureWhenKafkaIsUnavailable() {
        UUID orderId = UUID.randomUUID();
        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                "integration-test-customer",
                new BigDecimal("149.90"),
                Instant.now()
        );
        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:1"
        );
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000);//limits how long send() may wait for broker metadata or buffer space.
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1_500); //limits delivery time, including retries, after the producer can process the record.

        var producerFactory =
                new DefaultKafkaProducerFactory<String, OrderCreated>(properties);

        var kafkaTemplate =
                new KafkaTemplate<String, OrderCreated>(producerFactory);

        var publisher = new OrderEventPublisher(kafkaTemplate);


        try {
            assertThatThrownBy(() ->
                    publisher.publish(event)
                            .get(3, TimeUnit.SECONDS)
            ).hasRootCauseInstanceOf(
                    org.apache.kafka.common.errors.TimeoutException.class
            );
        } finally {
            producerFactory.destroy();
        }



    }

    @Test
    void rejectsAcksOneWhenIdempotenceIsEnabled() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:1"
        );
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );

        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        var producerFactory =
                new DefaultKafkaProducerFactory<String, OrderCreated>(
                        properties
                );

        try {
            assertThatThrownBy(producerFactory::createProducer)
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("acks");
        } finally {
            producerFactory.destroy();
        }
    }



}
