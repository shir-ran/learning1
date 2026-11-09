package com.example.orderservice.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreated(
        UUID eventId,
        UUID orderId,
        String customerId,
        BigDecimal totalAmount,
        Instant occurredAt
) {
}

