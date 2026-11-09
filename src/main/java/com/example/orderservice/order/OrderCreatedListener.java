package com.example.orderservice.order;

import com.example.orderservice.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final OrderProcessingService orderProcessingService;

    public OrderCreatedListener(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDERS_CREATED_TOPIC, groupId = "order-service")
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        var event = record.value();
        orderProcessingService.process(event);
        log.info("Consumed OrderCreated: orderId={}, customerId={}, totalAmount={}, record Key={}, record Partition={}, record Offset={}",
                event.orderId(), event.customerId(), event.totalAmount(), record.key(), record.partition(), record.offset());
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDERS_CREATED_DLT_TOPIC, groupId = "order-dlt-service")
    @Profile("dev")
    public void onOrderCorrupted(ConsumerRecord<String, OrderCreated> record) {
        var event = record.value();
        log.info("Consumed corrupt order: orderId={}, customerId={}, totalAmount={}, record Key={}, record Partition={}, record Offset={}",
                event.orderId(), event.customerId(), event.totalAmount(), record.key(), record.partition(), record.offset());
    }
}

