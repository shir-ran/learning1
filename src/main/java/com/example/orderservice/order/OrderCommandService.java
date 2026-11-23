package com.example.orderservice.order;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderCommandService {

    private final OrderCommandRepository orderCommandRepository;
    private final OutboxEventRepository outboxEventRepository;

    public OrderCommandService(OrderCommandRepository orderCommandRepository,
                               OutboxEventRepository outboxEventRepository) {
        this.orderCommandRepository = orderCommandRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional
    public CreatedOrder create(CreateOrderRequest request) {
        var eventId = java.util.UUID.randomUUID();
        var orderId = java.util.UUID.randomUUID();
        var occurredAt = java.time.Instant.now();
        var event = new OrderCreated(
                eventId,
                orderId,
                request.customerId(),
                request.totalAmount(),
                occurredAt
        );

        // Insert into the customer_order table
        orderCommandRepository.insert(event);

        // Insert into the outbox_event table
        outboxEventRepository.insert(event);

        return new CreatedOrder(orderId, eventId);
    }
}
