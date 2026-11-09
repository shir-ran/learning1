package com.example.orderservice.order;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderEventPublisher publisher;

    public OrderController(OrderEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    CompletableFuture<ResponseEntity<Map<String, UUID>>> create(@Valid @RequestBody CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                orderId,
                request.customerId(),
                request.totalAmount(),
                Instant.now()
        );

//        This is the contract 'Fire and forget'
//        publisher.publish(event);
//        return ResponseEntity.accepted().body(Map.of("orderId", orderId, "eventId", event.eventId()));

        // This is waiting for Kafka acknowledgement
        return publisher.publish(event)
                .thenApply(sendResult ->
                        ResponseEntity.accepted().body(
                                Map.of(
                                        "orderId", orderId,
                                        "eventId", event.eventId()
                                )
                        )
                )
                .exceptionally(error ->
                        ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .build()
                );
    }
}

