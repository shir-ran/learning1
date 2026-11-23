package com.example.orderservice.order;

import java.util.UUID;

public record CreatedOrder(UUID orderId,
                           UUID eventId
) {
}
