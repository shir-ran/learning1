package com.example.orderservice.order;

import java.util.UUID;

public interface ProcessedEventStore {
    boolean tryMarkAsProcessed(UUID eventId);
}
